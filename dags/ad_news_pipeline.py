"""Thai marketing/advertising news pipeline.

create_tables -> [TaskGroup extract: 3 sources in parallel] -> merge_transform (dedup)
  -> quality_gate (branch) -> load_to_postgres (UPSERT)  |  quarantine
  -> done

- A failed source does NOT stop the DAG: merge_transform uses trigger_rule=ALL_DONE.
- Re-running never adds rows: UPSERT on url + cross-source title_key check.
- Demo failure:  SIMULATE_DOWN=positioning   |  Demo quarantine: QUALITY_MIN_ROWS=9999
"""
import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

import scrapers

CONN_ID = "adnews_pg"

DDL = """
CREATE TABLE IF NOT EXISTS ad_news (
    url            TEXT PRIMARY KEY,
    title          TEXT NOT NULL,
    title_key      TEXT NOT NULL,
    published_date DATE,
    category       TEXT,
    source         TEXT NOT NULL,
    brand          TEXT,
    first_seen_at  TIMESTAMPTZ DEFAULT now(),
    updated_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ad_news_title_key ON ad_news (title_key);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL PRIMARY KEY,
    run_id      TEXT,
    task        TEXT,
    source      TEXT,
    status      TEXT,
    rows_count  INT,
    detail      TEXT,
    finished_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ad_news_rejects (
    id         SERIAL PRIMARY KEY,
    run_id     TEXT,
    reason     TEXT,
    payload    JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""

UPSERT = """
INSERT INTO ad_news (url, title, title_key, published_date, category, source, brand)
SELECT %(url)s, %(title)s, %(title_key)s, %(published_date)s, %(category)s, %(source)s, %(brand)s
WHERE NOT EXISTS (SELECT 1 FROM ad_news WHERE title_key = %(title_key)s AND url <> %(url)s)
ON CONFLICT (url) DO UPDATE SET
    title          = EXCLUDED.title,
    published_date = COALESCE(EXCLUDED.published_date, ad_news.published_date),
    category       = EXCLUDED.category,
    brand          = EXCLUDED.brand,
    updated_at     = now()
"""


def _hook():
    return PostgresHook(postgres_conn_id=CONN_ID)


def _log(task_name, source, status, n=0, detail=""):
    run_id = get_current_context()["run_id"]
    _hook().run(
        "INSERT INTO pipeline_runs (run_id, task, source, status, rows_count, detail) VALUES (%s,%s,%s,%s,%s,%s)",
        parameters=(run_id, task_name, source, status, n, (detail or "")[:500]),
    )


def make_extract(source):
    @task(task_id=f"extract_{source}", retries=1, retry_delay=timedelta(seconds=15))
    def _extract():
        try:
            if source in os.getenv("SIMULATE_DOWN", ""):
                raise RuntimeError(f"simulated outage of {source}")
            rows = scrapers.EXTRACTORS[source]()
            if not rows:
                raise RuntimeError("0 rows extracted")
            _log(f"extract_{source}", source, "success", len(rows))
            return rows
        except Exception as e:
            _log(f"extract_{source}", source, "failed", 0, str(e))
            raise
    return _extract()


@dag(
    dag_id="ad_news_pipeline",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "student"},
    tags=["02739452", "ad-news"],
)
def ad_news_pipeline():

    @task
    def create_tables():
        _hook().run(DDL)

    with TaskGroup("extract", tooltip="3 sources in parallel") as extract_group:
        mo = make_extract("marketingoops")
        pos = make_extract("positioning")
        bi = make_extract("brandinside")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def merge_transform(a, b, c):
        batches = [x for x in (b, c, a) if x]          # a source that failed gives None -> skipped
        if not batches:
            raise AirflowFailException("all sources failed")
        raw = [r for batch in batches for r in batch]
        rows = scrapers.transform(raw)
        _log("merge_transform", "all", "success", len(rows),
             f"sources_ok={len(batches)}/3 raw={len(raw)} after_dedup={len(rows)}")
        return rows

    @task.branch
    def quality_gate(rows):
        res = scrapers.assess(rows, min_rows=int(os.getenv("QUALITY_MIN_ROWS", "30")))
        _log("quality_gate", "all", "pass" if res["ok"] else "fail", len(rows),
             json.dumps(res, ensure_ascii=False))
        return "load_to_postgres" if res["ok"] else "quarantine"

    @task
    def load_to_postgres(rows):
        today = datetime.utcnow().date().isoformat()
        good = [r for r in rows
                if r["url"] and len(r["title"]) >= 8 and not (r["published_date"] and r["published_date"] > today)]
        conn = _hook().get_conn()
        try:
            with conn.cursor() as cur:
                for r in good:
                    cur.execute(UPSERT, r)
            conn.commit()
        finally:
            conn.close()
        _log("load_to_postgres", "all", "success", len(good), f"upserted {len(good)} of {len(rows)}")

    @task
    def quarantine(rows):
        run_id = get_current_context()["run_id"]
        _hook().run(
            "INSERT INTO ad_news_rejects (run_id, reason, payload) VALUES (%s, %s, %s)",
            parameters=(run_id, "quality gate failed", json.dumps(rows, ensure_ascii=False)),
        )
        _log("quarantine", "all", "quarantined", len(rows), "quality gate failed - nothing loaded")

    done = EmptyOperator(task_id="done", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    created = create_tables()
    merged = merge_transform(mo, pos, bi)
    gate = quality_gate(merged)
    load = load_to_postgres(merged)
    quar = quarantine(merged)

    created >> extract_group
    gate >> [load, quar]
    [load, quar] >> done


ad_news_pipeline()
