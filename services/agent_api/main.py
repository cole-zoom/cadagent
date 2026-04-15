import logging
import os
from pathlib import Path

import anthropic
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from shared.clients.bigquery import BigQueryClient
from shared.config.logging import configure_logging
from shared.config.settings import settings

from .citation import format_citations, lookup_citations
from .config import AgentConfig
from .sql_validator import SQLValidationError, validate_sql
from .tools import TOOLS, dispatch_tool

configure_logging(settings.log_level, service="agent_api")
logger = logging.getLogger(__name__)

app = FastAPI(title="trace-ca Agent API", version="0.2.0")

# CORS — permissive for POC. Tighten allow_origins to your Vercel domain later
# via CORS_ALLOWED_ORIGINS="https://your-app.vercel.app,https://..." env var.
_allowed_origins = os.getenv("CORS_ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _allowed_origins],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = AgentConfig()
bq_client = BigQueryClient(project_id=settings.gcp_project_id)
llm_client = anthropic.Anthropic()

PROMPTS_DIR = Path(__file__).parent / "prompts"
SYSTEM_PROMPT = (PROMPTS_DIR / "system_prompt.txt").read_text()
CITATION_PROMPT = (PROMPTS_DIR / "citation_prompt.txt").read_text()

# Combined system prompt for the agent loop: schema knowledge + formatting rules.
AGENT_SYSTEM_PROMPT = (
    SYSTEM_PROMPT
    + "\n\n---\n\n"
    "You have access to tools for exploring the data warehouse. Use them "
    "iteratively to answer the user's question. When a query returns 0 rows, "
    "use `list_metrics`, `list_time_periods`, or `list_geographies` to find "
    "a close match and retry. For meta-questions like 'what data do you "
    "have?', start with `describe_coverage`.\n\n"
    "When you have enough information, respond with a final answer in "
    "GitHub-flavored Markdown:\n\n"
    + CITATION_PROMPT
)

MAX_TOOL_ITERATIONS = 8


class AskRequest(BaseModel):
    question: str
    department: str | None = None


class AskResponse(BaseModel):
    answer: str
    sql: str
    sources: list[dict]
    rows_returned: int
    tool_calls: list[dict]


class ExplainResponse(BaseModel):
    sql: str
    question: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "agent_api"}


@app.post("/ask", response_model=AskResponse)
def ask(request: AskRequest) -> AskResponse:
    """Answer a natural-language question using an agent loop with tools."""
    question = request.question
    if request.department and request.department != "all":
        question = (
            f"{question}\n\n(Filter results to department_id = "
            f"'{request.department}' where relevant.)"
        )

    messages: list[dict] = [{"role": "user", "content": question}]
    collected_doc_ids: set[str] = set()
    tool_calls_log: list[dict] = []
    last_sql = ""
    last_rows_returned = 0

    for iteration in range(MAX_TOOL_ITERATIONS):
        try:
            response = llm_client.messages.create(
                model=config.llm_model,
                max_tokens=2048,
                system=AGENT_SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )
        except anthropic.BadRequestError as e:
            logger.error("Anthropic bad request: %s", e)
            raise HTTPException(
                status_code=500, detail=f"LLM call failed: {e}"
            )

        # Append the assistant's turn to the conversation
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason != "tool_use":
            logger.warning(
                "Unexpected stop_reason=%s on iteration %d",
                response.stop_reason, iteration,
            )
            break

        # Execute each tool the model asked for
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            logger.info(
                "tool_use: name=%s input=%s", block.name, _truncate(block.input)
            )
            result = dispatch_tool(
                block.name,
                block.input,
                bq_client,
                settings.gcp_project_id,
                settings.bq_cur_dataset,
            )
            collected_doc_ids |= result.document_ids

            if block.name == "query_data" and isinstance(block.input, dict):
                last_sql = block.input.get("sql", last_sql)
                # Rough parse: find the row count from the JSON payload
                try:
                    import json as _json
                    parsed = _json.loads(result.content)
                    if isinstance(parsed, dict) and "rows_returned" in parsed:
                        last_rows_returned = int(parsed["rows_returned"])
                except Exception:
                    pass

            tool_calls_log.append({
                "name": block.name,
                "input": block.input,
                "is_error": result.is_error,
            })
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result.content,
                "is_error": result.is_error,
            })

        messages.append({"role": "user", "content": tool_results})
    else:
        logger.warning(
            "Agent loop hit MAX_TOOL_ITERATIONS (%d)", MAX_TOOL_ITERATIONS
        )

    # Extract final assistant text from the last response
    answer = _extract_final_text(response.content)
    if not answer.strip():
        answer = "The agent did not return a final answer. This usually means the loop hit the iteration limit — try a more specific question."

    # Look up citations for all doc_ids encountered
    citations = lookup_citations(
        bq_client,
        settings.gcp_project_id,
        settings.bq_cur_dataset,
        settings.bq_raw_dataset,
        list(collected_doc_ids),
    )

    return AskResponse(
        answer=answer,
        sql=last_sql,
        sources=[
            {
                "title": c.title,
                "url": c.source_url,
                "department": c.department,
            }
            for c in citations
        ],
        rows_returned=last_rows_returned,
        tool_calls=tool_calls_log,
    )


@app.post("/explain", response_model=ExplainResponse)
def explain(request: AskRequest) -> ExplainResponse:
    """Return generated SQL without executing it (single-shot, no tools)."""
    response = llm_client.messages.create(
        model=config.llm_model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": request.question}],
    )
    sql = response.content[0].text.strip()
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])
    sql = sql.strip()

    try:
        sql = validate_sql(sql, settings.gcp_project_id)
    except SQLValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid SQL: {e}")

    project = settings.gcp_project_id
    for dataset in ("cur", "quality"):
        sql = sql.replace(f"`{dataset}.", f"`{project}.{dataset}.")

    return ExplainResponse(sql=sql, question=request.question)


def _extract_final_text(content) -> str:
    """Join all text blocks from the final assistant message."""
    parts = []
    for block in content:
        if getattr(block, "type", None) == "text":
            parts.append(block.text)
    return "\n".join(parts).strip()


def _truncate(obj, limit: int = 200) -> str:
    s = str(obj)
    return s if len(s) <= limit else s[:limit] + "…"
