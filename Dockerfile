FROM python:3.14

WORKDIR /opt/app

COPY pyproject.toml ./
RUN pip install --no-cache-dir ".[dashboard]"

COPY dagster_project ./dagster_project
COPY panel_app ./panel_app
COPY shared ./shared

ENV PYTHONPATH=/opt/app

EXPOSE 3000 5006
