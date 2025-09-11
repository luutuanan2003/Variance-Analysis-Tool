# Use small Python base
FROM python:3.11-slim

# System prep: faster, smaller installs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
# We copy only what's needed for runtime (thanks to .dockerignore)
COPY app ./app
COPY frontend ./frontend
COPY README.md ./README.md

# Expose FastAPI/uvicorn port
EXPOSE 8000

# Run the API
# - app.main:app  (module:variable)
# - host 0.0.0.0 so it’s reachable from your LAN
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
# Use PORT env var if set (e.g. by cloud providers), default to 8000