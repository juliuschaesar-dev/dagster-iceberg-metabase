FROM python:3.14

WORKDIR /opt/dagster/app

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY dagster_project ./dagster_project

ENV PYTHONPATH=/opt/dagster/app

EXPOSE 3000
