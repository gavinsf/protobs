FROM python:3.12-slim AS protos
WORKDIR /build
RUN pip install --no-cache-dir grpcio-tools
COPY schemas/ schemas/
RUN for dir in schemas/*/; do \
      v=$(basename "$dir"); \
      mkdir -p "gen/$v"; \
      python -m grpc_tools.protoc -I "$dir" --python_out="gen/$v" "$dir"*.proto; \
    done \
 && for dir in gen/*/; do PYTHONPATH="$dir" python -c "import orders_pb2"; done

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY demo/requirements.txt .
RUN pip install -r requirements.txt
RUN useradd --create-home appuser
COPY --chown=appuser demo/consumer/consumer.py .
COPY --from=protos --chown=appuser /build/gen/ gen/
USER appuser
ENV SCHEMA_VERSION=v1
CMD ["sh", "-c", "PYTHONPATH=/app/gen/$SCHEMA_VERSION exec python consumer.py"]