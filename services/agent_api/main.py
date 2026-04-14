import logging
import os
from pathlib import Path

import anthropic
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.clients.bigquery import BigQueryClient
from shared.config.logging import configure_logging
from shared.config.settings import settings

from .citation import format_citations, lookup_citations
from .config import AgentConfig
from .sql_validator import SQLValidationError, validate_sql

configure_logging(settings.log_level, service="agent_api")
logger = logging.getLogger(__name__)

app = FastAPI(title="trace-ca Agent API", version="0.1.0")

config = AgentConfig()
bq_client = BigQueryClient(project_id=settings.gcp_project_id)
llm_client = anthropic.Anthropic()

PROMPTS_DIR = Path(__file__).parent / "prompts"
SYSTEM_PROMPT = (PROMPTS_DIR / "system_prompt.txt").read_text()
CITATION_PROMPT = (PROMPTS_DIR / "citation_prompt.txt").read_text()


class AskRequest(BaseModel):
    question: str
    department: str | None = None


class AskResponse(BaseModel):
    answer: str
    sql: str
    sources: list[dict]
    rows_returned: int


class ExplainResponse(BaseModel):
    sql: str
    question: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "agent_api"}


@app.post("/ask", response_model=AskResponse)
def ask(request: AskRequest) -> AskResponse:
    """Answer a natural-language question with SQL + provenance."""
    question = request.question

    # Add department filter hint if specified
    dept_hint = ""
    if request.department and request.department != "all":
        dept_hint = f"\nFilter results to department_id = '{request.department}'."

    # Step 1: Generate SQL
    sql = _generate_sql(question + dept_hint)

    # Step 2: Validate SQL
    try:
        sql = validate_sql(sql, settings.gcp_project_id)
    except SQLValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid SQL generated: {e}")

    # Step 3: Replace table references with fully qualified names
    sql = _qualify_tables(sql)

    # Step 4: Execute query
    try:
        rows = bq_client.query(sql)
    except Exception as e:
        logger.error("Query execution failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")

    # Step 5: Look up citations
    doc_ids = list({r.get("document_id", "") for r in rows if r.get("document_id")})
    citations = lookup_citations(
        bq_client, settings.gcp_project_id, settings.bq_cur_dataset, settings.bq_raw_dataset, doc_ids
    )

    # Step 6: Format answer with LLM
    answer = _format_answer(question, rows, citations)

    return AskResponse(
        answer=answer,
        sql=sql,
        sources=[{"title": c.title, "url": c.source_url, "department": c.department} for c in citations],
        rows_returned=len(rows),
    )


@app.post("/explain", response_model=ExplainResponse)
def explain(request: AskRequest) -> ExplainResponse:
    """Return generated SQL without executing it."""
    sql = _generate_sql(request.question)
    try:
        sql = validate_sql(sql, settings.gcp_project_id)
    except SQLValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid SQL: {e}")

    return ExplainResponse(sql=_qualify_tables(sql), question=request.question)


def _generate_sql(question: str) -> str:
    """Use the LLM to generate SQL from a natural-language question."""
    response = llm_client.messages.create(
        model=config.llm_model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )

    sql = response.content[0].text.strip()

    # Strip markdown code fences if present
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])

    return sql.strip()


def _format_answer(question: str, rows: list[dict], citations: list) -> str:
    """Use the LLM to format a human-readable answer from query results."""
    results_str = str(rows[:20])  # Limit context size
    citations_str = format_citations(citations)

    response = llm_client.messages.create(
        model=config.llm_model,
        max_tokens=1024,
        system=CITATION_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Question: {question}\n\n"
                    f"Query returned {len(rows)} rows. First results:\n{results_str}\n\n"
                    f"Source documents:\n{citations_str}"
                ),
            }
        ],
    )

    return response.content[0].text.strip()


def _qualify_tables(sql: str) -> str:
    """Replace `cur.table` with `project.cur.table`."""
    project = settings.gcp_project_id
    for dataset in ("cur", "quality"):
        sql = sql.replace(f"`{dataset}.", f"`{project}.{dataset}.")
    return sql
