# api_preview.py
import time
import logging
import asyncio
from typing import Optional, Dict, List
from pydantic import BaseModel, HttpUrl

from fastapi import FastAPI, HTTPException
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from crochet import setup
from twisted.internet import asyncioreactor


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

app = FastAPI(
    title="URL Counter API",
    description="API for counting URLs in a website",
    version="1.0.0"
)

class URLCountRequest(BaseModel):
    url: HttpUrl
    robots_txt: bool = True

    class Config:
        schema_extra = {
            "example": {
                "url": "https://example.com",
                "robots_txt": True
            }
        }

class LevelStatistics(BaseModel):
    count: int
    percentage: float

class URLInfo(BaseModel):
    index: int
    url: str
    level: int

class URLCountResponse(BaseModel):
    url: str
    total_urls: int
    top_paths: Dict[str, int]
    ordered_urls: List[URLInfo]
    max_level: int
    level_statistics: Dict[str, LevelStatistics]
    execution_time: float
    status: str
    message: str
    reason: Optional[str] = None

# Cấu hình Scrapy và tạo runner
configure_logging({'LOG_LEVEL': 'INFO'})
settings = get_project_settings()
settings.setdict({
    'CONCURRENT_REQUESTS': 100,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
    'DOWNLOAD_DELAY': 0.01,
    'COOKIES_ENABLED': False,
    'DOWNLOAD_TIMEOUT': 30,
    'LOG_LEVEL': 'INFO',
    'REACTOR_THREADPOOL_MAXSIZE': 30,
    'DEPTH_LIMIT': 0,
    'DEPTH_STATS_VERBOSE': True
})
crawler_runner = CrawlerRunner(settings)

# Biến global để lưu kết quả
spider_results = {}

def spider_closed(spider):
    """Callback khi spider kết thúc"""
    if hasattr(spider.crawler, 'stats'):
        stats = spider.crawler.stats.get_stats()
        spider_results[spider.name] = stats.get('url_stats', {})

@app.post("/count-urls", response_model=URLCountResponse)
async def count_urls(request: URLCountRequest):
    """
    Count all URLs in a website with depth information
    """
    try:
        start_time = time.time()
        logger.info(f"Starting URL count for: {request.url}")
        
        # Xóa kết quả cũ
        if 'count_spider' in spider_results:
            del spider_results['count_spider']
        
        # Chạy crawler
        settings = get_project_settings()
        settings.set('ROBOTSTXT_OBEY', request.robots_txt)
        
        deferred = crawler_runner.crawl(
            'count_spider', 
            url=str(request.url),
            callback=spider_closed
        )
        
        # Đợi kết quả (không có timeout)
        while 'count_spider' not in spider_results:
            await asyncio.sleep(1)
            
        result = spider_results['count_spider']
        execution_time = time.time() - start_time
            
        return URLCountResponse(
            url=str(request.url),
            total_urls=result['total_unique_urls'],
            top_paths=result['top_paths'],
            ordered_urls=result['ordered_urls'],
            max_level=result['max_level'],
            level_statistics=result['level_statistics'],
            execution_time=round(execution_time, 2),
            status="completed",
            message=(
                f"Found {result['total_unique_urls']} unique URLs across "
                f"{result['max_level'] + 1} levels in {round(execution_time, 2)} seconds"
            ),
            reason=result['reason']
        )

    except Exception as e:
        logger.error(f"Error counting URLs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)