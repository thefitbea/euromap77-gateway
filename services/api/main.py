import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://gateway:gateway_password@localhost:5432/euromap77",
)

app = FastAPI(title="EUROMAP 77 Gateway API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ControlUpdate(BaseModel):
    cycle_interval_s: float | None = Field(default=None, ge=0.2, le=60.0)
    force_alarm: bool | None = None
    inject_bad_payload: bool | None = None
    paused: bool | None = None


def get_conn() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


@app.get("/health")
def health() -> dict:
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
        return {"status": "ok", "database": "reachable"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


@app.get("/machines/{machine_id}/latest")
def latest_cycle(machine_id: str) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM injection_cycles
            WHERE machine_id = %s
            ORDER BY cycle_counter DESC
            LIMIT 1
            """,
            (machine_id,),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"No cycles found for {machine_id}")
    return row


@app.get("/machines/{machine_id}/cycles")
def list_cycles(
    machine_id: str,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM injection_cycles
            WHERE machine_id = %s
            ORDER BY cycle_counter DESC
            LIMIT %s
            """,
            (machine_id, limit),
        )
        return cur.fetchall()


@app.get("/machines/{machine_id}/summary")
def machine_summary(machine_id: str) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_cycles,
                COUNT(*) FILTER (WHERE NOT is_alarm) AS good_cycles,
                COUNT(*) FILTER (WHERE is_alarm) AS alarm_cycles,
                ROUND(AVG(shot_weight_g)::numeric, 2) AS avg_shot_weight_g,
                ROUND(AVG(melt_temp_c)::numeric, 1) AS avg_melt_temp_c,
                ROUND(AVG(mold_temp_c)::numeric, 1) AS avg_mold_temp_c,
                ROUND(AVG(injection_pressure_bar)::numeric, 1) AS avg_pressure_bar,
                ROUND(AVG(cycle_time_s)::numeric, 2) AS avg_cycle_time_s
            FROM injection_cycles
            WHERE machine_id = %s
            """,
            (machine_id,),
        )
        stats = cur.fetchone() or {}

        cur.execute(
            "SELECT COUNT(*) AS dlq_count FROM dead_letter_events WHERE machine_id = %s",
            (machine_id,),
        )
        dlq = cur.fetchone() or {"dlq_count": 0}

    total = stats.get("total_cycles") or 0
    good = stats.get("good_cycles") or 0
    quality_pct = round((good / total) * 100, 1) if total > 0 else 0.0

    return {
        "machine_id": machine_id,
        **stats,
        "quality_pct": quality_pct,
        "dlq_count": dlq["dlq_count"],
    }


@app.get("/dead-letter")
def dead_letter_events(
    machine_id: str | None = None,
    limit: int = Query(default=25, ge=1, le=200),
) -> list[dict]:
    with get_conn() as conn, conn.cursor() as cur:
        if machine_id:
            cur.execute(
                """
                SELECT *
                FROM dead_letter_events
                WHERE machine_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (machine_id, limit),
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM dead_letter_events
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
        return cur.fetchall()


@app.get("/machines/{machine_id}/controls")
def get_controls(machine_id: str) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM machine_controls WHERE machine_id = %s",
            (machine_id,),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Control row not found for {machine_id}")
    return row


@app.patch("/machines/{machine_id}/controls")
def update_controls(machine_id: str, patch: ControlUpdate) -> dict:
    updates = patch.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No control fields provided")

    set_clauses = [f"{col} = %s" for col in updates]
    set_clauses.append("updated_at = NOW()")
    params = list(updates.values()) + [machine_id]

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE machine_controls
            SET {", ".join(set_clauses)}
            WHERE machine_id = %s
            RETURNING *
            """,
            params,
        )
        row = cur.fetchone()
        conn.commit()

    if not row:
        raise HTTPException(status_code=404, detail=f"Control row not found for {machine_id}")
    return row
