FROM python:3.13-slim AS builder

WORKDIR /app


COPY app.py app.py
COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install -r requirements.txt

RUN rm -rf /root/.cache/pip

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]