# Web Crawler with Google Cloud Storage Integration

A FastAPI-based web crawler that can crawl websites and automatically upload the results to Google Cloud Storage.

## Features
- Web crawling using Scrapy
- FastAPI REST API interface
- Google Cloud Storage integration
- Asynchronous task processing
- Configurable crawling parameters
- Real-time task status monitoring

## Prerequisites
- Python 3.8+
- Google Cloud SDK
- Google Cloud Project with Storage enabled

## Installation

1. Clone the repository
```bash
git clone https://github.com/NguyenBaPhat/web-crawler.git
cd web-crawler
```

2. Create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Set up Google Cloud credentials
```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

## Usage

1. Start the API server
```bash
python api.py
```

2. Send requests using curl or Postman:
```bash
curl -X POST http://localhost:8000/crawl \
-H "Content-Type: application/json" \
-d '{
    "url": "https://example.com",
    "page_limit": 5,
    "directory": "crawled_data"
}'
```

3. Check task status:
```bash
curl http://localhost:8000/task/{task_id}
```

## API Endpoints

- POST /crawl - Start a new crawl task
- GET /task/{task_id} - Get task status

## Configuration

Default values:
- page_limit: 1
- obey_robots: True
- directory: "crawled_data"

## Author
NguyenBaPhat - phtnguyen14112002@gmail.com