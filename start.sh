#!/bin/bash

cron &

python /app/elt_script.py

# Keep container alive for cron
tail -f /var/log/cron.log