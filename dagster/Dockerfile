FROM python:3.12-slim

RUN pip install --no-cache-dir --upgrade pip
WORKDIR /opt/dagster/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p "$DAGSTER_HOME"
COPY dagster.yaml "$DAGSTER_HOME/dagster.yaml"
COPY workspace.yaml /opt/dagster/app/workspace.yaml
COPY sdwan_orchestration /opt/dagster/app/sdwan_orchestration

WORKDIR /opt/dagster/app
