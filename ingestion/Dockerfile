FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
COPY ingesta_tmdb.py .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "ingesta_tmdb.py"]