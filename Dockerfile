FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv/lake

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts
COPY samples ./samples

ENV DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8000

# 2 个 worker 进程验证并发发布的安全性（目录库写事务串行化）
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", "--timeout", "120", "app.web:app"]
