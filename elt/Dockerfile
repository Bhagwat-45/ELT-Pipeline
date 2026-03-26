FROM python:3.8-slim

RUN apt-get update && apt-get install -y \
    curl ca-certificates gnupg lsb-release cron \
    && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y postgresql-client-18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY elt_script.py .
COPY start.sh .

RUN echo "0 3 * * * python /app/elt_script.py >> /var/log/cron.log 2>&1" | crontab - \
    && chmod +x start.sh

CMD ["./start.sh"]