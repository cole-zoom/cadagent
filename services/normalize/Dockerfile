FROM python:3.12-slim

WORKDIR /app

COPY services/normalize/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/normalize/ services/normalize/
COPY mappings/ mappings/

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.normalize.main"]
