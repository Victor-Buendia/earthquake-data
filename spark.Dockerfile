FROM docker.io/bitnami/spark:3.5

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

USER 1001