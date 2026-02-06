# Dagster ETL Project Dockerfile
FROM python:3.11-slim

# 환경 변수 설정
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# Python 의존성 설치
COPY pyproject.toml .
RUN pip install -e .

# 애플리케이션 코드 복사
COPY . .

# Dagster 홈 디렉토리 설정
ENV DAGSTER_HOME=/app/.dagster_home
RUN mkdir -p $DAGSTER_HOME

# dagster.yaml 복사
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml

# 포트 노출 (Dagster Webserver)
EXPOSE 3000

# 기본 명령어 (dagster-webserver 실행)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
