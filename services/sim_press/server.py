import asyncio
import os
import random
from datetime import datetime, timezone
from asyncua import Server, ua

ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://0.0.0.0:4840/euromap77/imm01/")
NAMESPACE_URI = "http://euromap.org/euromap77/imm01"
MACHINE_ID = os.getenv("MACHINE_ID", "IMM-01")
CYCLE_INTERVAL_S = float(os.getenv("CYCLE_INTERVAL_S", "2.0"))


async def main() -> None:
    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)
    server.set_server_name(f"EUROMAP 77 Simulator - {MACHINE_ID}")

    idx = await server.register_namespace(NAMESPACE_URI)
    objects = server.nodes.objects

    imm = await objects.add_object(idx, MACHINE_ID)
    status_obj = await imm.add_object(idx, "MachineStatus")
    mes_obj = await imm.add_object(idx, "MachineMESStatus")
    job_obj = await imm.add_object(idx, "ActiveJob")
    ctrl_obj = await imm.add_object(idx, "SimulationControl")

    # Core EUROMAP 77-style telemetry variables
    cycle_counter = await job_obj.add_variable(idx, "CycleCounter", 0, ua.VariantType.Int64)
    shot_weight_g = await job_obj.add_variable(idx, "ShotWeightG", 42.5, ua.VariantType.Double)
    melt_temp_c = await status_obj.add_variable(idx, "MeltTempC", 225.0, ua.VariantType.Double)
    mold_temp_c = await status_obj.add_variable(idx, "MoldTempC", 45.0, ua.VariantType.Double)
    injection_pressure_bar = await status_obj.add_variable(idx, "InjectionPressureBar", 850.0, ua.VariantType.Double)
    cycle_time_s = await job_obj.add_variable(idx, "CycleTimeS", 14.2, ua.VariantType.Double)
    machine_status = await status_obj.add_variable(idx, "Status", "RUNNING", ua.VariantType.String)
    mes_state = await mes_obj.add_variable(idx, "MESState", "PRODUCTION", ua.VariantType.String)
    last_update_utc = await status_obj.add_variable(
        idx, "LastUpdateUtc", datetime.now(timezone.utc).isoformat(), ua.VariantType.String
    )

    # Control variables (writable by ingestor/dashboard)
    force_alarm = await ctrl_obj.add_variable(idx, "ForceAlarm", False, ua.VariantType.Boolean)
    inject_bad_payload = await ctrl_obj.add_variable(idx, "InjectBadPayload", False, ua.VariantType.Boolean)

    writable_vars = [
        cycle_counter,
        shot_weight_g,
        melt_temp_c,
        mold_temp_c,
        injection_pressure_bar,
        cycle_time_s,
        machine_status,
        mes_state,
        last_update_utc,
        force_alarm,
        inject_bad_payload,
    ]
    for var in writable_vars:
        await var.set_writable()

    count = 0
    print(f"[sim_press] Starting {MACHINE_ID} OPC UA server at {ENDPOINT}")

    async with server:
        while True:
            await asyncio.sleep(CYCLE_INTERVAL_S)
            count += 1

            alarm_flag = bool(await force_alarm.read_value()) or (random.random() < 0.08)
            bad_payload_flag = bool(await inject_bad_payload.read_value()) or (random.random() < 0.10)

            now_iso = datetime.now(timezone.utc).isoformat()
            await cycle_counter.write_value(ua.Variant(count, ua.VariantType.Int64))
            await last_update_utc.write_value(ua.Variant(now_iso, ua.VariantType.String))

            if bad_payload_flag:
                # Deliberately publish an invalid/corrupt reading (NaN/negative weight) to test DLQ
                await shot_weight_g.write_value(ua.Variant(-999.0, ua.VariantType.Double))
                await machine_status.write_value(ua.Variant("DATA_FAULT", ua.VariantType.String))
                await mes_state.write_value(ua.Variant("ERROR", ua.VariantType.String))
                await inject_bad_payload.write_value(ua.Variant(False, ua.VariantType.Boolean))
                print(f"[sim_press] Cycle {count}: injected invalid payload (shot_weight_g=-999.0)")
                continue

            weight = round(random.gauss(42.5, 0.35), 2)
            melt_t = round(random.gauss(225.0, 1.8), 1)
            mold_t = round(random.gauss(45.0, 0.7), 1)
            pressure = round(random.gauss(850.0, 14.0), 1)
            c_time = round(random.gauss(14.2, 0.25), 2)

            if alarm_flag:
                pressure = round(random.uniform(1120.0, 1240.0), 1)
                status_str = "ALARM"
                mes_str = "DOWNTIME"
            else:
                status_str = "RUNNING"
                mes_str = "PRODUCTION"

            await shot_weight_g.write_value(ua.Variant(weight, ua.VariantType.Double))
            await melt_temp_c.write_value(ua.Variant(melt_t, ua.VariantType.Double))
            await mold_temp_c.write_value(ua.Variant(mold_t, ua.VariantType.Double))
            await injection_pressure_bar.write_value(ua.Variant(pressure, ua.VariantType.Double))
            await cycle_time_s.write_value(ua.Variant(c_time, ua.VariantType.Double))
            await machine_status.write_value(ua.Variant(status_str, ua.VariantType.String))
            await mes_state.write_value(ua.Variant(mes_str, ua.VariantType.String))

            print(
                f"[sim_press] Cycle {count}: status={status_str} weight={weight}g "
                f"melt={melt_t}C mold={mold_t}C press={pressure}bar time={c_time}s"
            )


if __name__ == "__main__":
    asyncio.run(main())
