# Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# --- Simplified dependencies (removed pkg-config, kept build tools for other libs maybe) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    # python3-dev # Often helpful for C extensions
    # Add pyodbc dependencies if needed: unixodbc-dev
    && rm -rf /var/lib/apt/lists/*
# --- END Simplified ---

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# Set work directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install project dependencies
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY ./app /app/app
COPY ./alembic.ini /app/alembic.ini
COPY ./alembic /app/alembic

# Command to run the application using Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80", "--proxy-headers", "--forwarded-allow-ips='*'"]
