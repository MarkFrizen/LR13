FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY cmd/dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py .
COPY .streamlit/ .streamlit/

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
