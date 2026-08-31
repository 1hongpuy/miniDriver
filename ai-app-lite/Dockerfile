FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Keep semantic models optional: the default hash provider gives a small,
# repeatable operations image. OpenCLIP/GPU belongs in a later dedicated image.
COPY requirements.txt requirements-postgres.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-postgres.txt

COPY cinedata_ai ./cinedata_ai

ENTRYPOINT ["python", "-m", "cinedata_ai.main"]
CMD ["api", "--host", "0.0.0.0", "--port", "18290"]
