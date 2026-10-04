"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import hashlib
import json
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def cache_key(text: str, model: str, prompt_version: str) -> str:
    """hash(input) + model + prompt version: any of the three changes -> a new call."""
    return hashlib.sha256(f"{model}\x1f{prompt_version}\x1f{text}".encode("utf-8")).hexdigest()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cached, validated LLM labelling. Re-runs with the same model + prompt make 0 calls."""
    # Read PROMPT_VERSION at call time (not import time) so a version bump re-labels.
    model, prompt_version = llm.model, PROMPT_VERSION
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        cache_key VARCHAR PRIMARY KEY, model VARCHAR, prompt_version VARCHAR,
        raw_answer VARCHAR, label VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_quarantine (
        ticket_id VARCHAR, cache_key VARCHAR, model VARCHAR, prompt_version VARCHAR,
        raw_answer VARCHAR, reason VARCHAR)""")

    tickets = live_tickets(con)
    keyed = [(tid, text, cache_key(text, model, prompt_version)) for tid, text in tickets]
    cached = {k for (k,) in con.execute("SELECT cache_key FROM llm_label_cache").fetchall()}
    todo = [(tid, text, k) for tid, text, k in keyed if k not in cached]

    # Rule 3: estimate cost BEFORE calling the model.
    est_tokens = estimate_tokens([text for _, text, _ in todo])
    est_cost = est_tokens / 1000 * PRICE_PER_1K_TOKENS_USD

    calls_before = llm.calls
    for _, text, k in todo:
        if k in cached:          # two tickets with identical text share one call
            continue
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        # The answer (valid or not) is cached: the same input never costs twice.
        con.execute("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?, ?)",
                    [k, model, prompt_version, raw, parse_label(raw)])
        cached.add(k)

    answers = dict(con.execute(
        "SELECT cache_key, struct_pack(raw := raw_answer, label := label) FROM llm_label_cache"
    ).fetchall())
    good, bad = [], []
    for tid, _, k in keyed:
        a = answers[k]
        if a["label"] is None:
            bad.append((tid, k, model, prompt_version, a["raw"], "off-schema answer"))
        else:
            good.append((tid, a["label"], model, prompt_version))

    # Gold and quarantine are rebuilt from the cache each run -> idempotent.
    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    if good:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", good)
    con.execute("DELETE FROM llm_label_quarantine")
    if bad:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?, ?)", bad)
    return {"labeled": len(good), "quarantined": len(bad),
            "calls": llm.calls - calls_before,
            "estimated_tokens": est_tokens, "estimated_cost_usd": round(est_cost, 6)}
