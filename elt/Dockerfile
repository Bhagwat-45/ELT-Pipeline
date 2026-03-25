FROM python:3.8-slim

RUN apt-get update && apt-get install -y curl ca-certificates gnupg lsb-release \
    # Add the official PostgreSQL apt repository
    && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    # Install pg client version 18 to match your server
    && apt-get update && apt-get install -y postgresql-client-18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY elt_script.py .

CMD ["python", "elt_script.py"]