FROM python:3.12-slim

WORKDIR /app

COPY services/agent_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/agent_api/ services/agent_api/

ENV PYTHONPATH=/app
ENV PORT=8080

CMD ["uvicorn", "services.agent_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
