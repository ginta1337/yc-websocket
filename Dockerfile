FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN apk add --no-cache ca-certificates tzdata \
    && pip install --no-cache-dir -r requirements.txt

COPY bridge.py .

EXPOSE 8080

CMD ["python3", "bridge.py"]
