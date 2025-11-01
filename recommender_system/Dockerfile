FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el repo para permitir imports relativos como `database.*` o `API.*`
COPY . /app

ENV PYTHONPATH=/app

# Exponer el puerto del maestro de jobs
EXPOSE 8100

CMD ["python", "-m", "uvicorn", "recommender_system.recommender_master:app", "--host", "0.0.0.0", "--port", "8100"]
