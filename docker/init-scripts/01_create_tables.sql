-- Source Database 초기화 스크립트
-- 샘플 테이블 생성 (APS/제조 데이터)

-- Lot History 테이블
CREATE TABLE IF NOT EXISTS lot_history (
    lot_id VARCHAR(50) PRIMARY KEY,
    product_code VARCHAR(50) NOT NULL,
    process_step VARCHAR(100) NOT NULL,
    equipment_id VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    quantity INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Equipment Event 테이블
CREATE TABLE IF NOT EXISTS equipment_event (
    event_id SERIAL PRIMARY KEY,
    equipment_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    duration_minutes INTEGER DEFAULT 0,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Process Result 테이블
CREATE TABLE IF NOT EXISTS process_result (
    result_id SERIAL PRIMARY KEY,
    lot_id VARCHAR(50) NOT NULL,
    process_step VARCHAR(100) NOT NULL,
    measurement_value DECIMAL(10,4),
    spec_lower DECIMAL(10,4),
    spec_upper DECIMAL(10,4),
    pass_fail VARCHAR(10) DEFAULT 'PASS',
    measured_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_lot_history_created_at ON lot_history(created_at);
CREATE INDEX IF NOT EXISTS idx_lot_history_status ON lot_history(status);
CREATE INDEX IF NOT EXISTS idx_equipment_event_time ON equipment_event(event_time);
CREATE INDEX IF NOT EXISTS idx_process_result_measured_at ON process_result(measured_at);

-- 샘플 데이터 삽입
INSERT INTO lot_history (lot_id, product_code, process_step, equipment_id, start_time, end_time, quantity, status, created_at)
SELECT
    'LOT-' || LPAD(i::text, 6, '0'),
    'PROD-' || (1 + (i % 10))::text,
    'STEP-' || (1 + (i % 5))::text,
    'EQ-' || (1 + (i % 20))::text,
    CURRENT_TIMESTAMP - ((i % 30) || ' days')::interval - ((i % 24) || ' hours')::interval,
    CURRENT_TIMESTAMP - ((i % 30) || ' days')::interval - ((i % 24) || ' hours')::interval + ((30 + (i % 60)) || ' minutes')::interval,
    100 + (i % 500),
    CASE WHEN i % 10 = 0 THEN 'HOLD' WHEN i % 7 = 0 THEN 'COMPLETED' ELSE 'IN_PROGRESS' END,
    CURRENT_TIMESTAMP - ((i % 30) || ' days')::interval
FROM generate_series(1, 1000) AS i
ON CONFLICT (lot_id) DO NOTHING;

INSERT INTO equipment_event (equipment_id, event_type, event_time, duration_minutes, description)
SELECT
    'EQ-' || (1 + (i % 20))::text,
    CASE
        WHEN i % 5 = 0 THEN 'DOWN'
        WHEN i % 3 = 0 THEN 'IDLE'
        ELSE 'RUN'
    END,
    CURRENT_TIMESTAMP - ((i % 30) || ' days')::interval - ((i % 24) || ' hours')::interval,
    CASE
        WHEN i % 5 = 0 THEN 10 + (i % 50)
        WHEN i % 3 = 0 THEN 5 + (i % 30)
        ELSE 60 + (i % 180)
    END,
    'Auto-generated event #' || i
FROM generate_series(1, 2000) AS i;

INSERT INTO process_result (lot_id, process_step, measurement_value, spec_lower, spec_upper, pass_fail, measured_at)
SELECT
    'LOT-' || LPAD((1 + (i % 1000))::text, 6, '0'),
    'STEP-' || (1 + (i % 5))::text,
    10.0 + (random() * 5)::decimal(10,4),
    8.0,
    18.0,
    CASE WHEN random() > 0.05 THEN 'PASS' ELSE 'FAIL' END,
    CURRENT_TIMESTAMP - ((i % 30) || ' days')::interval
FROM generate_series(1, 5000) AS i;

-- 결과 확인
SELECT 'lot_history' as table_name, COUNT(*) as row_count FROM lot_history
UNION ALL
SELECT 'equipment_event', COUNT(*) FROM equipment_event
UNION ALL
SELECT 'process_result', COUNT(*) FROM process_result;
