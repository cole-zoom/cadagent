"use client";

import { useState } from "react";

type Department = "all" | "fin" | "statcan" | "tbs-sct";

type Source = {
  title: string;
  url: string;
  department: string;
};

type AskResponse = {
  answer: string;
  sql: string;
  sources: Source[];
  rows_returned: number;
};

const DEPARTMENTS: { id: Department; label: string }[] = [
  { id: "all", label: "All" },
  { id: "fin", label: "Finance" },
  { id: "statcan", label: "StatCan" },
  { id: "tbs-sct", label: "Treasury Board" },
];

const SAMPLE_PROMPTS = [
  "What was Canada's real GDP growth in 2023?",
  "Compare unemployment rates across provinces for 2023-24",
  "Federal tax revenues by category since 2020",
];

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export default function Page() {
  const [question, setQuestion] = useState("");
  const [department, setDepartment] = useState<Department>("all");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AskResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showSql, setShowSql] = useState(false);

  async function submit() {
    const q = question.trim();
    if (!q || loading) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setShowSql(false);

    try {
      const res = await fetch(`${API_URL}/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: q,
          department: department === "all" ? null : department,
        }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({ detail: res.statusText }));
        if (res.status === 400) {
          setError(
            `The generated query was rejected by the validator. ${body.detail || ""}`,
          );
        } else if (res.status >= 500) {
          setError(
            `The query could not be executed. ${body.detail || "Upstream error."}`,
          );
        } else {
          setError(body.detail || `Unexpected ${res.status}.`);
        }
        return;
      }

      const json = (await res.json()) as AskResponse;
      setResult(json);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Network error";
      setError(`Could not reach the data warehouse. ${msg}`);
    } finally {
      setLoading(false);
    }
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      submit();
    }
  }

  return (
    <main className="relative mx-auto max-w-[1100px] px-8 pb-32 pt-12 md:px-16 md:pt-20">
      {/* ─── Masthead ─────────────────────────────────────────── */}
      <header className="rise rise-1 flex items-baseline justify-between border-b border-rule/60 pb-6">
        <div className="flex items-baseline gap-3">
          <span className="font-display text-3xl italic leading-none text-ink">
            trace
          </span>
          <span className="text-xl text-accent">·</span>
          <span className="font-display text-3xl leading-none text-ink">ca</span>
        </div>
        <div className="hidden text-right font-mono text-[11px] uppercase tracking-widest text-muted md:block">
          a warehouse of
          <br />
          Canadian open data
        </div>
      </header>

      {/* ─── Hero / Query ──────────────────────────────────────── */}
      <section className="pt-16 md:pt-24">
        <div className="rise rise-2 mb-6 flex items-center gap-3 font-mono text-[11px] uppercase tracking-widest text-muted">
          <span>№ 01</span>
          <span className="h-px w-10 bg-rule" />
          <span>query</span>
        </div>

        <h1 className="rise rise-2 max-w-[18ch] font-display text-[clamp(2.6rem,6vw,4.8rem)] leading-[1.02] tracking-tight text-ink">
          Ask about Canadian
          <span className="italic text-accent"> fiscal </span>
          and statistical data.
        </h1>

        <p className="rise rise-3 mt-6 max-w-[52ch] text-[15px] leading-relaxed text-muted">
          Natural-language questions, translated into BigQuery against a curated
          star schema of observations sourced from open.canada.ca. Every answer
          cites its source documents.
        </p>

        {/* Textarea */}
        <div className="rise rise-3 mt-12">
          <div className="mb-3 flex items-center gap-3 font-mono text-[11px] uppercase tracking-widest text-muted">
            <span>your question</span>
            <span className="h-px flex-1 bg-rule/50" />
          </div>
          <label htmlFor="q" className="sr-only">
            Your question
          </label>
          <div className="group/input relative border-b border-rule/60 pb-3 transition-colors focus-within:border-ink/70">
            <textarea
              id="q"
              autoFocus
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={onKeyDown}
              placeholder="What was Canada's real GDP growth in 2023?"
              rows={2}
              className="block w-full resize-none bg-transparent pt-4 font-display text-[clamp(1.5rem,3vw,2.25rem)] leading-[1.3] text-ink placeholder:text-rule focus:outline-none"
            />
          </div>
        </div>

        {/* Sample prompts */}
        {!question && !result && (
          <div className="rise rise-4 mt-3 flex flex-wrap items-center gap-x-4 gap-y-2 text-[13px]">
            <span className="font-mono text-[11px] uppercase tracking-widest text-muted">
              try —
            </span>
            {SAMPLE_PROMPTS.map((p) => (
              <button
                key={p}
                type="button"
                onClick={() => setQuestion(p)}
                className="italic text-muted underline-offset-4 transition-colors hover:text-ink hover:underline"
              >
                {p}
              </button>
            ))}
          </div>
        )}

        {/* Department chips + submit */}
        <div className="rise rise-4 mt-14 flex flex-col items-start justify-between gap-8 border-t border-rule/50 pt-6 md:flex-row md:items-center">
          <div className="flex flex-wrap items-center gap-x-6 gap-y-3">
            <span className="font-mono text-[11px] uppercase tracking-widest text-muted">
              scope ↳
            </span>
            {DEPARTMENTS.map((d) => {
              const active = department === d.id;
              return (
                <button
                  key={d.id}
                  type="button"
                  onClick={() => setDepartment(d.id)}
                  className={`chip group relative pb-1 text-[13px] tracking-widish transition-colors ${
                    active ? "chip-active text-ink" : "text-muted hover:text-ink"
                  }`}
                >
                  {d.label}
                  <span
                    className={`chip-rule ${
                      active ? "bg-accent" : "bg-ink"
                    }`}
                    aria-hidden
                  />
                </button>
              );
            })}
          </div>

          <button
            type="button"
            onClick={submit}
            disabled={!question.trim() || loading}
            className={`group inline-flex items-center border px-5 py-3 font-mono text-[12px] uppercase tracking-widest transition-all disabled:cursor-not-allowed ${
              !question.trim() || loading
                ? "border-rule/60 text-muted"
                : "border-ink bg-ink text-paper hover:bg-accent hover:border-accent"
            }`}
          >
            <span className="arrow-line" aria-hidden />
            {loading ? (
              <span className="inline-flex items-baseline gap-2">
                querying
                <span className="inline-flex gap-[3px]">
                  <span className="dot">·</span>
                  <span className="dot">·</span>
                  <span className="dot">·</span>
                </span>
              </span>
            ) : (
              <span>run query</span>
            )}
          </button>
        </div>

        <p className="rise rise-5 mt-3 font-mono text-[10px] uppercase tracking-widest text-rule md:text-right">
          ⌘ + return
        </p>
      </section>

      {/* ─── Result ───────────────────────────────────────────── */}
      {(result || error) && (
        <section className="mt-24 border-t border-rule/60 pt-12">
          <div className="mb-6 flex items-center gap-3 font-mono text-[11px] uppercase tracking-widest text-muted">
            <span>№ 02</span>
            <span className="h-px w-10 bg-rule" />
            <span>{error ? "error" : "result"}</span>
            {result && (
              <span className="ml-auto italic tracking-normal normal-case text-rule">
                retrieved {result.rows_returned}
                {result.rows_returned === 1 ? " row" : " rows"}
              </span>
            )}
          </div>

          {error && (
            <div className="max-w-[70ch]">
              <p className="font-display text-2xl italic leading-snug text-ink">
                The query could not be completed.
              </p>
              <p className="mt-4 font-mono text-[12px] leading-relaxed text-muted">
                {error}
              </p>
            </div>
          )}

          {result && (
            <article className="max-w-[72ch]">
              {/* Answer */}
              <div className="font-display text-[22px] leading-[1.5] text-ink md:text-[24px]">
                {result.answer.split(/\n\n+/).map((para, i) => (
                  <p key={i} className={i > 0 ? "mt-5" : ""}>
                    {para}
                  </p>
                ))}
              </div>

              {/* SQL — expandable */}
              <div className="mt-14 border-t border-rule/50 pt-5">
                <button
                  type="button"
                  onClick={() => setShowSql((s) => !s)}
                  className="group flex w-full items-center justify-between font-mono text-[11px] uppercase tracking-widest text-muted transition-colors hover:text-ink"
                >
                  <span className="flex items-center gap-3">
                    <span>generated SQL</span>
                    <span className="h-px w-10 bg-rule" />
                  </span>
                  <span className="text-ink">
                    {showSql ? "collapse −" : "expand +"}
                  </span>
                </button>
                {showSql && (
                  <pre className="sql-scroll mt-5 overflow-x-auto whitespace-pre bg-paper-deep/60 p-5 font-mono text-[12.5px] leading-relaxed text-ink">
                    {result.sql}
                  </pre>
                )}
              </div>

              {/* Sources */}
              <div className="mt-10 border-t border-rule/50 pt-5">
                <div className="flex items-center gap-3 font-mono text-[11px] uppercase tracking-widest text-muted">
                  <span>sources</span>
                  <span className="h-px flex-1 bg-rule" />
                </div>
                {result.sources.length === 0 ? (
                  <p className="mt-4 text-[14px] italic text-muted">
                    No source documents returned.
                  </p>
                ) : (
                  <ol className="mt-5 space-y-4">
                    {result.sources.map((s, i) => (
                      <li key={`${s.url}-${i}`} className="flex gap-5">
                        <span className="w-6 shrink-0 pt-[3px] text-right font-mono text-[11px] text-rule">
                          {String(i + 1).padStart(2, "0")}
                        </span>
                        <div>
                          {s.url ? (
                            <a
                              href={s.url}
                              target="_blank"
                              rel="noreferrer"
                              className="font-display text-[18px] italic leading-snug text-ink underline decoration-rule decoration-1 underline-offset-4 transition-colors hover:decoration-accent"
                            >
                              {s.title || s.url}
                            </a>
                          ) : (
                            <span className="font-display text-[18px] italic leading-snug text-ink">
                              {s.title}
                            </span>
                          )}
                          {s.department && (
                            <div className="mt-1 font-mono text-[10px] uppercase tracking-widest text-muted">
                              {s.department}
                            </div>
                          )}
                        </div>
                      </li>
                    ))}
                  </ol>
                )}
              </div>
            </article>
          )}
        </section>
      )}

      {/* ─── Footer ───────────────────────────────────────────── */}
      <footer className="mt-32 flex flex-col items-start justify-between gap-3 border-t border-rule/60 pt-6 font-mono text-[10px] uppercase tracking-widest text-muted md:flex-row md:items-center">
        <span>trace·ca — proof of concept</span>
        <span>
          data ·{" "}
          <a
            href="https://open.canada.ca"
            target="_blank"
            rel="noreferrer"
            className="text-muted underline-offset-4 hover:text-ink hover:underline"
          >
            open.canada.ca
          </a>
        </span>
      </footer>
    </main>
  );
}
