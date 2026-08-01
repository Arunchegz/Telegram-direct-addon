FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# HuggingFace Spaces Docker SDK runs containers as uid 1000
# Files are written to /data/tgstream (persistent volume).
RUN useradd -m -u 1000 -s /bin/bash appuser && \
    mkdir -p /data/tgstream && \
    chown -R appuser:appuser /app /data

USER appuser
EXPOSE 7860
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-7860}"]
