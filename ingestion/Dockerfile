FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingesta_tmdb.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "ingesta_tmdb.py"]