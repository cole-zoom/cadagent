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
from .tools import TOOLS, _qualify_tables, dispatch_tool

configure_logging(settings.log_level, service="agent_api")
logger = logging.getLogger(__name__)

app = FastAPI(title="trace-ca Agent API", version="0.3.0")

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

MAX_TOOL_ITERATIONS = 10

# ──────────────────────────────────────────────────────────────────────────
# Static data summary — pre-fetched once, baked into the agent's prompt so it
# doesn't waste tool calls on discovery.
# ──────────────────────────────────────────────────────────────────────────

_data_summary_cache: str | None = None


def _build_data_summary() -> str:
    """Fetch a concise warehouse summary from BigQuery. ~1k tokens."""
    project = settings.gcp_project_id
    cur = settings.bq_cur_dataset

    sections: list[str] = ["## Live data warehouse coverage (cached at server start)"]

    # Per-department counts
    rows = bq_client.query(f"""
        SELECT department_id, COUNT(*) AS obs, COUNT(DISTINCT document_id) AS docs
        FROM `{project}.{cur}.fact_observation`
        GROUP BY department_id ORDER BY obs DESC
    """)
    sections.append("\n### Departments")
    for r in rows:
        sections.append(
            f"- **{r['department_id']}**: {r['obs']:,} observations across {r['docs']} documents"
        )

    # Top 30 named metrics
    rows = bq_client.query(f"""
        SELECT dm.canonical_name, COUNT(f.observation_id) AS obs
        FROM `{project}.{cur}.fact_observation` f
        JOIN `{project}.{cur}.dim_metric` dm USING (metric_id)
        WHERE dm.canonical_name IS NOT NULL AND dm.canonical_name != ''
        GROUP BY dm.canonical_name
        HAVING COUNT(f.observation_id) > 0
        ORDER BY obs DESC LIMIT 30
    """)
    sections.append("\n### Top 30 metrics (canonical_name : obs count)")
    sections.extend(f"- {r['canonical_name']} ({r['obs']:,})" for r in rows)

    # All time labels
    rows = bq_client.query(f"""
        SELECT dt.label, COUNT(f.observation_id) AS obs
        FROM `{project}.{cur}.fact_observation` f
        JOIN `{project}.{cur}.dim_time` dt USING (time_id)
        GROUP BY dt.label
        HAVING COUNT(f.observation_id) > 0
        ORDER BY obs DESC
    """)
    sections.append("\n### Time periods (label : obs count)")
    sections.append(", ".join(f"{r['label']} ({r['obs']:,})" for r in rows))

    # Named geographies with data
    rows = bq_client.query(f"""
        SELECT dg.name_en, dg.geo_type, COUNT(f.observation_id) AS obs
        FROM `{project}.{cur}.fact_observation` f
        JOIN `{project}.{cur}.dim_geography` dg USING (geography_id)
        GROUP BY dg.name_en, dg.geo_type
        ORDER BY obs DESC
    """)
    sections.append(
        "\n### Named geographies with data "
        "(everything else has geography_id = NULL)"
    )
    sections.extend(
        f"- {r['name_en']} ({r['geo_type']}): {r['obs']} obs" for r in rows
    )

    return "\n".join(sections)


def get_data_summary() -> str:
    """Lazy-cached data summary. Built on first call, reused thereafter."""
    global _data_summary_cache
    if _data_summary_cache is None:
        try:
            _data_summary_cache = _build_data_summary()
            logger.info(
                "Data summary cached (%d chars)", len(_data_summary_cache)
            )
        except Exception as e:
            logger.warning("Failed to build data summary: %s", e)
            _data_summary_cache = ""
    return _data_summary_cache


def _agent_system_prompt() -> str:
    """System prompt for the agent loop: schema + data summary + citation rules."""
    return (
        SYSTEM_PROMPT
        + "\n\n---\n\n"
        + get_data_summary()
        + "\n\n---\n\n"
        + "When you have enough information, respond with a final answer in "
        "GitHub-flavored Markdown:\n\n"
        + CITATION_PROMPT
    )


def _format_answer_system_prompt() -> str:
    """System prompt for the fast-path answer formatter."""
    return get_data_summary() + "\n\n---\n\n" + CITATION_PROMPT


# ──────────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────────


class AskRequest(BaseModel):
    question: str
    department: str | None = None


class AskResponse(BaseModel):
    answer: str
    sql: str
    sources: list[dict]
    rows_returned: int
    tool_calls: list[dict]
    path: str  # "fast" or "agent" — for debugging


class ExplainResponse(BaseModel):
    sql: str
    question: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "agent_api"}


@app.post("/ask", response_model=AskResponse)
def ask(request: AskRequest) -> AskResponse:
    """Two-tier: try single-shot SQL first, fall back to agent loop on miss."""
    fast = _try_fast_path(request)
    if fast is not None:
        logger.info("Fast path succeeded (rows=%d)", fast.rows_returned)
        return fast

    logger.info("Fast path missed; escalating to agent loop")
    return _agent_loop(request)


@app.post("/explain", response_model=ExplainResponse)
def explain(request: AskRequest) -> ExplainResponse:
    """Return generated SQL without executing it (single-shot, no tools)."""
    sql = _generate_single_shot_sql(request.question)
    try:
        sql = validate_sql(sql, settings.gcp_project_id)
    except SQLValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid SQL: {e}")
    sql = _qualify_tables(sql, settings.gcp_project_id)
    return ExplainResponse(sql=sql, question=request.question)


# ──────────────────────────────────────────────────────────────────────────
# Fast path — single-shot SQL gen + format
# ──────────────────────────────────────────────────────────────────────────


def _try_fast_path(request: AskRequest) -> AskResponse | None:
    """Try to answer with one SQL query + one formatting pass.
    Returns None to escalate to the agent loop.
    """
    question = _decorate_question(request)

    # 1. Generate SQL
    try:
        sql = _generate_single_shot_sql(question)
        sql = validate_sql(sql, settings.gcp_project_id)
    except SQLValidationError as e:
        logger.info("Fast path SQL invalid: %s", e)
        return None
    except Exception as e:
        logger.info("Fast path SQL gen failed: %s", e)
        return None

    sql = _qualify_tables(sql, settings.gcp_project_id)

    # 2. Execute
    try:
        rows = bq_client.query(sql)
    except Exception as e:
        logger.info("Fast path SQL execution failed: %s", e)
        return None

    # 3. Empty result → escalate so the agent can retry / list_metrics
    if not rows:
        return None

    # 4. Format answer
    doc_ids = [r["document_id"] for r in rows if r.get("document_id")]
    citations = lookup_citations(
        bq_client,
        settings.gcp_project_id,
        settings.bq_cur_dataset,
        settings.bq_raw_dataset,
        list(set(doc_ids)),
    )
    answer = _format_answer(question, rows, citations)

    return AskResponse(
        answer=answer,
        sql=sql,
        sources=[
            {"title": c.title, "url": c.source_url, "department": c.department}
            for c in citations
        ],
        rows_returned=len(rows),
        tool_calls=[
            {"name": "fast_path_query", "input": {"sql": sql}, "is_error": False}
        ],
        path="fast",
    )


def _generate_single_shot_sql(question: str) -> str:
    """Single LLM call: NL → SQL string. No tools."""
    response = llm_client.messages.create(
        model=config.llm_model,
        max_tokens=2048,
        system=SYSTEM_PROMPT + "\n\n" + get_data_summary(),
        messages=[{"role": "user", "content": question}],
    )
    sql = response.content[0].text.strip()
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])
    return sql.strip()


def _format_answer(question: str, rows: list[dict], citations: list) -> str:
    """LLM call to format query results into a markdown answer."""
    results_str = str(rows[:20])  # cap context
    citations_str = format_citations(citations)
    response = llm_client.messages.create(
        model=config.llm_model,
        max_tokens=1500,
        system=_format_answer_system_prompt(),
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


# ──────────────────────────────────────────────────────────────────────────
# Agent loop — fallback for queries the fast path can't answer
# ──────────────────────────────────────────────────────────────────────────


def _agent_loop(request: AskRequest) -> AskResponse:
    """Tool-use loop with last-turn forcing + final-summary backstop."""
    question = _decorate_question(request)
    messages: list[dict] = [{"role": "user", "content": question}]
    collected_doc_ids: set[str] = set()
    tool_calls_log: list[dict] = []
    last_sql = ""
    last_rows_returned = 0
    response = None

    for iteration in range(MAX_TOOL_ITERATIONS):
        try:
            response = llm_client.messages.create(
                model=config.llm_model,
                max_tokens=2048,
                system=_agent_system_prompt(),
                tools=TOOLS,
                messages=messages,
            )
        except anthropic.BadRequestError as e:
            logger.error("Anthropic bad request: %s", e)
            raise HTTPException(status_code=500, detail=f"LLM call failed: {e}")

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason != "tool_use":
            logger.warning("Unexpected stop_reason=%s", response.stop_reason)
            break

        # Execute tool calls
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            logger.info("tool_use: name=%s input=%s", block.name, _truncate(block.input))
            result = dispatch_tool(
                block.name, block.input, bq_client,
                settings.gcp_project_id, settings.bq_cur_dataset,
            )
            collected_doc_ids |= result.document_ids

            if block.name == "query_data" and isinstance(block.input, dict):
                last_sql = block.input.get("sql", last_sql)
                try:
                    import json as _json
                    parsed = _json.loads(result.content)
                    if isinstance(parsed, dict) and "rows_returned" in parsed:
                        last_rows_returned = int(parsed["rows_returned"])
                except Exception:
                    pass

            tool_calls_log.append({
                "name": block.name, "input": block.input, "is_error": result.is_error,
            })
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result.content,
                "is_error": result.is_error,
            })

        # Last-turn forcing: if next iteration is the final one, append a hint
        # to the same user message that carries the tool_results.
        is_second_to_last = iteration == MAX_TOOL_ITERATIONS - 2
        user_content: list = list(tool_results)
        if is_second_to_last:
            user_content.append({
                "type": "text",
                "text": (
                    "FINAL TURN: do not call any more tools. Write your final "
                    "answer in markdown using what you've gathered so far."
                ),
            })
        messages.append({"role": "user", "content": user_content})

    # Backstop: if we exited without end_turn, force one no-tools call so the
    # user always gets an answer.
    if response is None or response.stop_reason != "end_turn":
        logger.info("Forcing final no-tools summary call")
        try:
            response = llm_client.messages.create(
                model=config.llm_model,
                max_tokens=2048,
                system=(
                    _agent_system_prompt()
                    + "\n\nIMPORTANT: Do not call tools. Write a final answer "
                    "in markdown using only what's already been gathered."
                ),
                messages=messages,
            )
            messages.append({"role": "assistant", "content": response.content})
        except Exception as e:
            logger.error("Final summary call failed: %s", e)

    answer = _extract_final_text(response.content) if response else ""
    if not answer.strip():
        answer = (
            "I couldn't compose a final answer from the data I gathered. "
            "Try a more specific question, or rephrase it with a metric name "
            "from the example questions list."
        )

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
            {"title": c.title, "url": c.source_url, "department": c.department}
            for c in citations
        ],
        rows_returned=last_rows_returned,
        tool_calls=tool_calls_log,
        path="agent",
    )


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


def _decorate_question(request: AskRequest) -> str:
    if request.department and request.department != "all":
        return (
            f"{request.question}\n\n(Filter results to department_id = "
            f"'{request.department}' where relevant.)"
        )
    return request.question


def _extract_final_text(content) -> str:
    parts = []
    for block in content:
        if getattr(block, "type", None) == "text":
            parts.append(block.text)
    return "\n".join(parts).strip()


def _truncate(obj, limit: int = 200) -> str:
    s = str(obj)
    return s if len(s) <= limit else s[:limit] + "…"
