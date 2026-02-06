-- =============================================================================
-- Trino DDL: Run Event Log 외부 테이블
-- S3에 저장된 Dagster event log Parquet 데이터를 Trino에서 조회
--
-- S3 경로: s3://data-lake/project_id={tenant_id}/logs/run_events/date={YYYYMMDD}/{run_id}.parquet
-- =============================================================================

-- Iceberg 카탈로그 사용 시
CREATE TABLE IF NOT EXISTS iceberg.observability.run_events (
    run_id           VARCHAR,
    step_key         VARCHAR,
    "timestamp"      VARCHAR,
    category         VARCHAR,    -- failure / materialization / duration
    event_type       VARCHAR,    -- STEP_FAILURE, ASSET_MATERIALIZATION 등
    asset_key        VARCHAR,    -- Asset key (materialization 이벤트)
    error_type       VARCHAR,    -- Exception 클래스명 (failure 이벤트)
    error_message    VARCHAR,    -- 에러 메시지 (failure 이벤트)
    start_timestamp  VARCHAR,    -- Step 시작 시각 (duration 이벤트)
    end_timestamp    VARCHAR,    -- Step 종료 시각 (duration 이벤트)
    duration_sec     DOUBLE,     -- 실행 시간 초 (duration 이벤트)
    status           VARCHAR,    -- success / failure (duration 이벤트)
    row_count        VARCHAR,    -- materialization metadata
    s3_path          VARCHAR,    -- materialization metadata
    tenant_id        VARCHAR,    -- 테넌트 ID (run tags)
    job_name         VARCHAR,    -- Job 이름
    run_status       VARCHAR,    -- SUCCESS / FAILURE
    partition_key    VARCHAR     -- 파티션 키
)
WITH (
    external_location = 's3a://data-lake/',
    format = 'PARQUET'
);

-- =============================================================================
-- 유용한 쿼리 예시
-- =============================================================================

-- 1. 장애 분석: 최근 실패한 step 목록
-- SELECT run_id, step_key, error_type, error_message, "timestamp"
-- FROM iceberg.observability.run_events
-- WHERE category = 'failure'
-- ORDER BY "timestamp" DESC
-- LIMIT 20;

-- 2. 성능 병목 분석: step별 평균 실행 시간
-- SELECT step_key,
--        AVG(duration_sec) AS avg_duration,
--        MAX(duration_sec) AS max_duration,
--        COUNT(*) AS run_count
-- FROM iceberg.observability.run_events
-- WHERE category = 'duration' AND status = 'success'
-- GROUP BY step_key
-- ORDER BY avg_duration DESC;

-- 3. 데이터 결과 추적: 최근 materialization 내역
-- SELECT run_id, asset_key, row_count, s3_path, "timestamp"
-- FROM iceberg.observability.run_events
-- WHERE category = 'materialization'
-- ORDER BY "timestamp" DESC
-- LIMIT 50;

-- 4. 테넌트별 실행 현황
-- SELECT tenant_id, run_status, COUNT(DISTINCT run_id) AS run_count
-- FROM iceberg.observability.run_events
-- GROUP BY tenant_id, run_status;

-- 5. 특정 run의 전체 이벤트 타임라인
-- SELECT step_key, category, "timestamp", duration_sec, status, error_message
-- FROM iceberg.observability.run_events
-- WHERE run_id = '<RUN_ID>'
-- ORDER BY "timestamp";
