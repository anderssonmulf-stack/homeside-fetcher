#!/bin/bash
# Energy Importer Cron Script
# Runs the energy importer with environment from .env

cd /opt/dev/homeside-fetcher

# Load environment variables from .env
set -a
source .env
set +a

# Run importer
python3 energy_importer.py >> /var/log/energy_importer.log 2>&1
