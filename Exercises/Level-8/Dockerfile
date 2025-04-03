# Use Python 3.12 base image
FROM python:3.12

# Set working directory
WORKDIR /app
COPY . /app

# Upgrade pip and install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
