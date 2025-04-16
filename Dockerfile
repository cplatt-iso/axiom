# Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# ENV POETRY_NO_INTERACTION=1 \
#     POETRY_VIRTUALENVS_IN_PROJECT=false \
#     POETRY_VIRTUALENVS_CREATE=false
ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# Install poetry - Commented out as we use requirements.txt
# RUN pip install poetry

# Set work directory
WORKDIR /app

# Copy only dependency definitions
# Commented out poetry related lines:
# COPY poetry.lock pyproject.toml ./
# Using requirements.txt instead:
COPY requirements.txt .

# Install project dependencies
# Commented out poetry install command:
# RUN poetry install --no-dev --no-root
# Using pip with requirements.txt:
# Use cache mount for better layer caching during pip install
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY ./app /app/app

# Command to run the application using Uvicorn
# Use 0.0.0.0 to allow connections from outside the container
# --reload is useful for development, remove for production
# Increase workers for production based on CPU cores
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload", "--proxy-headers", "--forwarded-allow-ips='*'"]
# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5173", "--reload"]
# Production CMD example:
# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80", "--workers", "4"]
