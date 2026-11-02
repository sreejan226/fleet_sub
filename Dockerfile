FROM python:3.12.8-slim-bookworm

WORKDIR /app
ENV PYTHONPATH=/app/src
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY fleet.yaml /app/fleet.yaml
COPY tests /app/tests

EXPOSE 8000
CMD ["python", "-m", "generation_fleet"]
