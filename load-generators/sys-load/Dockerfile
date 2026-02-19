
FROM python:3.9-slim

WORKDIR /workdir

# Install dependencies with no cache to keep image small
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "generate_load.py"]