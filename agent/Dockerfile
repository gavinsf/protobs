FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY agent/requirements.txt .
RUN pip install -r requirements.txt

RUN useradd --create-home appuser && mkdir /data && chown appuser /data
COPY --chown=appuser agent/src/ ./src/

USER appuser
CMD ["python", "-m", "src"]