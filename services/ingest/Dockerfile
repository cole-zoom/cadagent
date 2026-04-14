FROM python:3.12-slim

WORKDIR /app

COPY services/ingest/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/ingest/ services/ingest/

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.ingest.main"]
