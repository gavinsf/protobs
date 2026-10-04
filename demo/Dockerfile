FROM python:3.12-slim


ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN for dir in backend/schemas/*/; do \
      v=$(basename "$dir"); \
      mkdir -p "gen/$v"; \
      python -m grpc_tools.protoc -I "./backend/schemas/$v" --python_out="gen/$v" "./backend/schemas/$v"/*.proto; \
    done

RUN for dir in gen/*/; do \
      PYTHONPATH="$dir" python -c "import orders_pb2"; \
    done

RUN useradd --create-home appuser && chown -R appuser /app
USER appuser

ENV ROLE=producer \
    SCHEMA_VERSION=v1

CMD ["sh", "-c", "PYTHONPATH=/app/gen/$SCHEMA_VERSION exec python backend/$ROLE/main.py"]