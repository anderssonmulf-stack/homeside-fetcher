FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py .
COPY variables_config.json .
COPY settings.json .
COPY profiles/ ./profiles/
COPY buildings/ ./buildings/
COPY energy_models/ ./energy_models/

# Run the fetcher
CMD ["python", "-u", "HSF_Fetcher.py"]
