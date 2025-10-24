FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingesta_tmdb.py .
ENV TZ=Europe/Madrid
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*
CMD ["python", "ingesta_tmdb.py"]