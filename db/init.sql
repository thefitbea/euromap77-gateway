CREATE TABLE IF NOT EXISTS injection_cycles (
    id BIGSERIAL PRIMARY KEY,
    machine_id VARCHAR(64) NOT NULL,
    cycle_counter BIGINT NOT NULL,
    shot_weight_g DOUBLE PRECISION NOT NULL,
    melt_temp_c DOUBLE PRECISION NOT NULL,
    mold_temp_c DOUBLE PRECISION NOT NULL,
    injection_pressure_bar DOUBLE PRECISION NOT NULL,
    cycle_time_s DOUBLE PRECISION NOT NULL,
    machine_status VARCHAR(32) NOT NULL,
    mes_state VARCHAR(32) NOT NULL DEFAULT 'PRODUCTION',
    is_alarm BOOLEAN NOT NULL DEFAULT FALSE,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (machine_id, cycle_counter)
);

CREATE INDEX IF NOT EXISTS idx_cycles_machine_time
    ON injection_cycles (machine_id, recorded_at DESC);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    machine_id VARCHAR(64) NOT NULL,
    raw_payload TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS machine_controls (
    machine_id VARCHAR(64) PRIMARY KEY,
    cycle_interval_s DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    force_alarm BOOLEAN NOT NULL DEFAULT FALSE,
    inject_bad_payload BOOLEAN NOT NULL DEFAULT FALSE,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO machine_controls (machine_id)
VALUES ('IMM-01')
ON CONFLICT (machine_id) DO NOTHING;
