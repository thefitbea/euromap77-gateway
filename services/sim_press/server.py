import asyncio
import json
import os
import psycopg
from asyncua import Client

OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/euromap77/imm01/")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://gateway:gateway_password@localhost:5432/euromap77")
NAMESPACE_URI = os.getenv("NAMESPACE_URI", "http://euromap.org/euromap77/imm01")
MACHINE_ID = os.getenv("MACHINE_ID", "IMM-01")
POLL_INTERVAL_S = float(os.getenv("POLL_INTERVAL_S", "2.0"))


def validate_cycle(payload: dict) -> tuple[bool, str]:
    """Validate physical ranges and required fields before writing to injection_cycles."""
    try:
        cycle_counter = int(payload["cycle_counter"])
        shot_weight_g = float(payload["shot_weight_g"])
        melt_temp_c = float(payload["melt_temp_c"])
        mold_temp_c = float(payload["mold_temp_c"])
        pressure_bar = float(payload["injection_pressure_bar"])
        cycle_time_s = float(payload["cycle_time_s"])
        status = str(payload["machine_status"]).strip()
    except (KeyError, TypeError, ValueError) as exc:
        return False, f"Type/schema error: {exc}"

    if cycle_counter <= 0:
        return False, f"Invalid cycle_counter: {cycle_counter}"
    if status == "DATA_FAULT":
        return False, "Machine reported DATA_FAULT status"
    if not (0.1 <= shot_weight_g <= 5000.0):
        return False, f"shot_weight_g out of bounds: {shot_weight_g}"
    if not (50.0 <= melt_temp_c <= 450.0):
        return False, f"melt_temp_c out of bounds: {melt_temp_c}"
    if not (5.0 <= mold_temp_c <= 250.0):
        return False, f"mold_temp_c out of bounds: {mold_temp_c}"
    if not (1.0 <= pressure_bar <= 3500.0):
        return False, f"injection_pressure_bar out of bounds: {pressure_bar}"
    if not (1.0 <= cycle_time_s <= 600.0):
        return False, f"cycle_time_s out of bounds: {cycle_time_s}"

    return True, ""


def write_dead_letter(conn: psycopg.Connection, machine_id: str, payload: dict, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dead_letter_events (machine_id, raw_payload, error_reason)
            VALUES (%s, %s, %s)
            """,
            (machine_id, json.dumps(payload, default=str), reason),
        )
    conn.commit()


def write_cycle(conn: psycopg.Connection, machine_id: str, payload: dict) -> None:
    status = str(payload["machine_status"]).strip()
    is_alarm = status == "ALARM"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO injection_cycles (
                machine_id,
                cycle_counter,
                shot_weight_g,
                melt_temp_c,
                mold_temp_c,
                injection_pressure_bar,
                cycle_time_s,
                machine_status,
                is_alarm
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (machine_id, cycle_counter) DO NOTHING
            """,
            (
                machine_id,
                int(payload["cycle_counter"]),
                float(payload["shot_weight_g"]),
                float(payload["melt_temp_c"]),
                float(payload["mold_temp_c"]),
                float(payload["injection_pressure_bar"]),
                float(payload["cycle_time_s"]),
                status,
                is_alarm,
            ),
        )
    conn.commit()


async def read_snapshot(client: Client, idx: int) -> dict:
    root = client.nodes.objects
    imm = await root.get_child([f"{idx}:{MACHINE_ID}"])
    status_obj = await imm.get_child([f"{idx}:MachineStatus"])
    job_obj = await imm.get_child([f"{idx}:ActiveJob"])

    nodes = {
        "cycle_counter": await job_obj.get_child([f"{idx}:CycleCounter"]),
        "shot_weight_g": await job_obj.get_child([f"{idx}:ShotWeightG"]),
        "cycle_time_s": await job_obj.get_child([f"{idx}:CycleTimeS"]),
        "melt_temp_c": await status_obj.get_child([f"{idx}:MeltTempC"]),
        "mold_temp_c": await status_obj.get_child([f"{idx}:MoldTempC"]),
        "injection_pressure_bar": await status_obj.get_child([f"{idx}:InjectionPressureBar"]),
        "machine_status": await status_obj.get_child([f"{idx}:Status"]),
    }

    snapshot = {}
    for key, node in nodes.items():
        snapshot[key] = await node.read_value()
    return snapshot


async def run_ingestor() -> None:
    last_seen_cycle = -1

    while True:
        try:
            async with Client(url=OPCUA_URL) as client, psycopg.connect(DATABASE_URL) as db_conn:
                idx = await client.get_namespace_index(NAMESPACE_URI)
                print(f"[ingestor] Connected to {OPCUA_URL} (ns={idx}) and PostgreSQL")

                while True:
                    snapshot = await read_snapshot(client, idx)
                    cycle_num = snapshot.get("cycle_counter")

                    if cycle_num is not None and cycle_num != last_seen_cycle:
                        is_valid, reason = validate_cycle(snapshot)
                        if is_valid:
                            write_cycle(db_conn, MACHINE_ID, snapshot)
                            print(
                                f"[ingestor] Saved cycle {cycle_num} "
                                f"(status={snapshot['machine_status']}, weight={snapshot['shot_weight_g']}g)"
                            )
                        else:
                            write_dead_letter(db_conn, MACHINE_ID, snapshot, reason)
                            print(f"[ingestor] DLQ cycle {cycle_num}: {reason}")
                        last_seen_cycle = cycle_num

                    await asyncio.sleep(POLL_INTERVAL_S)

        except Exception as exc:
            print(f"[ingestor] Connection/loop error: {exc}. Retrying in 3s...")
            await asyncio.sleep(3.0)


if __name__ == "__main__":
    asyncio.run(run_ingestor())
