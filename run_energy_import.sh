#!/bin/bash
# Energy data import from Dropbox
cd /opt/dev/homeside-fetcher

# Load environment
set -a
source .env
set +a

# Run importer
python3 energy_importer.py
