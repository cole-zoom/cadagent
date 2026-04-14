FROM python:3.12-slim

WORKDIR /app

COPY services/extract/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/extract/ services/extract/

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.extract.main"]
