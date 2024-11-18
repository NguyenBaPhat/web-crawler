from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl, validator
from typing import Optional
import asyncio
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
import logging
from datetime import datetime
import os
import shutil
from crochet import setup
from twisted.internet import asyncioreactor
from google.cloud import storage

# Cấu hình reactor
import sys
if 'twisted.internet.reactor' not in sys.modules:
    asyncioreactor.install()
from twisted.internet import reactor

# Khởi tạo crochet
setup()

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Khởi tạo Google Cloud Storage client
storage_client = storage.Client()
BUCKET_NAME = "ai-demo-421403-knowledge-base-agents"
BUCKET_FOLDER = "test"

app = FastAPI(
    title="Web Crawler API",
    description="API for crawling websites with configurable parameters",
    version="1.0.0"
)

class CrawlRequest(BaseModel):
    url: HttpUrl
    page_limit: int = 1
    obey_robots: bool = True
    directory: str = "crawled_data"  # Thêm tham số directory với giá trị mặc định

    @validator('page_limit')
    def validate_page_limit(cls, v):
        if v <= 0:
            raise ValueError('page_limit must be greater than 0')
        return v

    @validator('directory')
    def validate_directory(cls, v):
        if not v.strip():
            raise ValueError('directory cannot be empty')
        return v

    class Config:
        schema_extra = {
            "example": {
                "url": "https://example.com",
                "page_limit": 1,
                "obey_robots": True,
                "directory": "crawled_data"
            }
        }

class CrawlResponse(BaseModel):
    task_id: str
    status: str
    output_directory: Optional[str] = None
    gcs_path: Optional[str] = None
    message: str

# Lưu trữ trạng thái task
crawl_tasks = {}

# Cấu hình Scrapy
configure_logging({'LOG_LEVEL': 'INFO'})
runner = CrawlerRunner(get_project_settings())

async def upload_to_gcs(local_directory: str, task_id: str):
    """Upload directory to Google Cloud Storage"""
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        base_path = f"{BUCKET_FOLDER}/{task_id}"
        uploaded_files = []
        
        logger.info(f"Uploading files from {local_directory} to gs://{BUCKET_NAME}/{base_path}")
        
        # Upload all files in the directory
        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_directory)
                blob_path = f"{base_path}/{relative_path}"
                
                blob = bucket.blob(blob_path)
                try:
                    blob.upload_from_filename(local_path)
                    uploaded_files.append(blob_path)
                    logger.info(f"Uploaded {local_path} to gs://{BUCKET_NAME}/{blob_path}")
                except Exception as e:
                    logger.error(f"Failed to upload {local_path}: {str(e)}")
                    continue
        
        if not uploaded_files:
            raise Exception("No files were uploaded successfully")
        
        return f"gs://{BUCKET_NAME}/{base_path}"
    
    except Exception as e:
        error_msg = f"Error uploading to GCS: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

async def run_spider(url: str, page_limit: int, obey_robots: bool, task_id: str):
    """Chạy spider"""
    try:
        settings = get_project_settings()
        settings.set('ROBOTSTXT_OBEY', obey_robots)
        
        deferred = runner.crawl(
            'fast_spider',
            url=url,
            page_limit=page_limit,
            task_id=task_id
        )
        
        future = asyncio.Future()
        
        def on_complete(result):
            if not future.done():
                future.set_result(result)
        
        def on_error(failure):
            if not future.done():
                future.set_exception(failure.value)
        
        deferred.addCallbacks(on_complete, on_error)
        
        await future
        return future.result()
        
    except Exception as e:
        logger.error(f"Error in run_spider: {str(e)}")
        raise

async def handle_crawl(url: str, page_limit: int, obey_robots: bool, task_id: str, directory: str):
    """Xử lý task crawl"""
    try:
        crawl_tasks[task_id]["status"] = "running"
        await run_spider(url, page_limit, obey_robots, task_id)
        
        # Create specified directory if it doesn't exist
        output_dir = os.path.join(directory, task_id)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Move crawled files to specified directory
        source_dir = f'crawled_pages_{task_id}'
        if os.path.exists(source_dir):
            for item in os.listdir(source_dir):
                s = os.path.join(source_dir, item)
                d = os.path.join(output_dir, item)
                shutil.copy2(s, d)
            shutil.rmtree(source_dir)
        
        # Upload to GCS
        gcs_path = await upload_to_gcs(output_dir, task_id)
        
        crawl_tasks[task_id]["status"] = "completed"
        crawl_tasks[task_id]["end_time"] = datetime.now().isoformat()
        crawl_tasks[task_id]["gcs_path"] = gcs_path
        logger.info(f"Task {task_id} completed successfully")
        
    except Exception as e:
        error_msg = f"Task {task_id} failed: {str(e)}"
        logger.error(error_msg)
        crawl_tasks[task_id]["status"] = "failed"
        crawl_tasks[task_id]["error"] = error_msg

@app.post("/crawl", response_model=CrawlResponse)
async def start_crawl(crawl_request: CrawlRequest):
    """
    Start a new crawl task
    """
    try:
        task_id = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Starting new crawl task with configuration:")
        logger.info(f"URL: {crawl_request.url}")
        logger.info(f"Page limit: {crawl_request.page_limit}")
        logger.info(f"Obey robots.txt: {crawl_request.obey_robots}")
        logger.info(f"Directory: {crawl_request.directory}")
        
        crawl_tasks[task_id] = {
            "status": "starting",
            "start_time": datetime.now().isoformat(),
            "url": str(crawl_request.url),
            "page_limit": crawl_request.page_limit,
            "obey_robots": crawl_request.obey_robots,
            "directory": crawl_request.directory,
            "configuration": {
                "url": str(crawl_request.url),
                "page_limit": crawl_request.page_limit,
                "obey_robots": crawl_request.obey_robots,
                "directory": crawl_request.directory
            }
        }
        
        asyncio.create_task(
            handle_crawl(
                str(crawl_request.url),
                crawl_request.page_limit,
                crawl_request.obey_robots,
                task_id,
                crawl_request.directory
            )
        )
        
        return CrawlResponse(
            task_id=task_id,
            status="started",
            output_directory=os.path.join(crawl_request.directory, task_id),
            message=f"Crawl task has been started with configuration: "
                   f"page_limit={crawl_request.page_limit}, "
                   f"obey_robots={crawl_request.obey_robots}, "
                   f"directory={crawl_request.directory}"
        )

    except Exception as e:
        logger.error(f"Error starting crawl: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/task/{task_id}", response_model=CrawlResponse)
async def get_task_status(task_id: str):
    """
    Get the status of a crawl task
    """
    if task_id not in crawl_tasks:
        raise HTTPException(status_code=404, detail="Task not found")

    task = crawl_tasks[task_id]
    output_dir = os.path.join(task["directory"], task_id)

    return CrawlResponse(
        task_id=task_id,
        status=task["status"],
        output_directory=output_dir if os.path.exists(output_dir) else None,
        gcs_path=task.get("gcs_path"),
        message=f"Task is {task['status']}" + (f": {task.get('error', '')}" if task.get('error') else "")
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)