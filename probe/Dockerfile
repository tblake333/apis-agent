# Production Dockerfile for the probe application
# Build: docker build -t probe .
# Run: docker run --env-file .env.prod probe

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Firebird client
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfbclient2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py", "--env"]
