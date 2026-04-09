FROM python:3.11-slim

WORKDIR /app

# 1. Install your apps requirements PLUS gunicorn
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# 2. Copy the rest of your files
COPY . .

# 3. Inform Docker we are using port 5000
EXPOSE 5000

# 4. START with Gunicorn instead of python app.py
# This starts 4 "workers" (mini-processes) to handle your UI traffic
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app", "--workers", "4", "--timeout", "120"]