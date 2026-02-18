FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"


RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY ingest_data.py .


ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]
