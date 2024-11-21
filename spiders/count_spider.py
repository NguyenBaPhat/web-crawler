# spiders/count_spider.py
import scrapy
from urllib.parse import urlparse, urljoin
from collections import Counter, defaultdict
from scrapy import signals
from scrapy.exceptions import CloseSpider

class CountSpider(scrapy.Spider):
    name = 'count_spider'
    custom_settings = {
        'CONCURRENT_REQUESTS': 100,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
        'DOWNLOAD_DELAY': 0.01,
        'COOKIES_ENABLED': False,
        'DOWNLOAD_TIMEOUT': 30,
        'LOG_LEVEL': 'INFO',
        'REACTOR_THREADPOOL_MAXSIZE': 30,
        'DEPTH_LIMIT': 0,
        'DEPTH_STATS_VERBOSE': True
    }
    
    # Giới hạn cố định 1000 trang
    MAX_PAGES = 1000
    
    def __init__(self, url=None, callback=None, *args, **kwargs):
        super(CountSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url]
        self.allowed_domain = urlparse(url).netloc
        self.url_counter = Counter()
        self.found_urls = set()
        self.url_index = {}
        self.url_levels = defaultdict(int)
        self.level_counts = defaultdict(int)
        self.current_index = 1
        self.start_time = time.time()
        self.max_level_found = 0
        self.finished = False
        self.close_callback = callback
        self.stopped_reason = None

        # Initialize first URL
        self.url_levels[url] = 0
        self.level_counts[0] = 1
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider
    
    def parse(self, response, current_level=0):
        try:
            # Kiểm tra giới hạn số trang
            if len(self.found_urls) >= self.MAX_PAGES:
                if not self.stopped_reason:
                    self.stopped_reason = f'Reached maximum limit of {self.MAX_PAGES} pages'
                    self.logger.info(self.stopped_reason)
                    raise CloseSpider(reason=self.stopped_reason)
                return

            current_url = response.url

            if current_url not in self.found_urls:
                self.found_urls.add(current_url)
                self.url_index[current_url] = self.current_index
                self.current_index += 1
                
                parsed_url = urlparse(current_url)
                self.url_counter[parsed_url.path] += 1
                
                self.url_levels[current_url] = current_level
                self.level_counts[current_level] += 1
                self.max_level_found = max(self.max_level_found, current_level)
                
                self.logger.info(
                    f"[{self.url_index[current_url]}/{self.MAX_PAGES}] Found: {current_url} "
                    f"(Level: {current_level})"
                )

            # Chỉ tiếp tục tìm URLs mới nếu chưa đạt giới hạn
            if len(self.found_urls) < self.MAX_PAGES:
                next_level = current_level + 1
                for href in response.css('a::attr(href)').getall():
                    url = urljoin(response.url, href)
                    parsed = urlparse(url)
                    
                    if (parsed.netloc == self.allowed_domain and 
                        url not in self.found_urls and 
                        len(self.found_urls) < self.MAX_PAGES):
                        self.found_urls.add(url)
                        self.url_index[url] = self.current_index
                        self.current_index += 1
                        
                        parsed_url = urlparse(url)
                        self.url_counter[parsed_url.path] += 1
                        
                        self.url_levels[url] = next_level
                        self.level_counts[next_level] += 1
                        self.max_level_found = max(self.max_level_found, next_level)
                        
                        self.logger.info(
                            f"[{self.url_index[url]}/{self.MAX_PAGES}] Found: {url} "
                            f"(Level: {next_level})"
                        )
                        
                        if len(self.found_urls) < self.MAX_PAGES:
                            yield scrapy.Request(
                                url, 
                                callback=lambda r, level=next_level: self.parse(r, level),
                                errback=self.errback,
                                dont_filter=True
                            )

        except CloseSpider:
            raise
        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {str(e)}")

    def errback(self, failure):
        self.logger.error(f"Request failed: {str(failure.value)}")

    def spider_closed(self, spider, reason='finished'):
        """Handler khi spider đóng"""
        if not self.finished:
            self.finished = True
            duration = time.time() - self.start_time
            
            # Tạo thống kê chi tiết theo level
            level_stats = {
                str(level): {
                    'count': count,
                    'percentage': (count / len(self.found_urls)) * 100
                }
                for level, count in self.level_counts.items()
            }
            
            # Sắp xếp URLs theo thứ tự
            ordered_urls = sorted(
                [(idx, url, self.url_levels[url]) 
                 for url, idx in self.url_index.items()],
                key=lambda x: x[0]
            )

            total_urls = len(self.found_urls)
            reached_limit = total_urls >= self.MAX_PAGES

            # Chuẩn bị thống kê
            spider_stats = {
                'total_unique_urls': total_urls,
                'url_counter': dict(self.url_counter),
                'top_paths': dict(self.url_counter.most_common(10)),
                'execution_time': duration,
                'max_level': self.max_level_found,
                'level_statistics': level_stats,
                'ordered_urls': [
                    {
                        'index': idx,
                        'url': url,
                        'level': level
                    }
                    for idx, url, level in ordered_urls
                ],
                'reached_limit': reached_limit,
                'reason': self.stopped_reason if self.stopped_reason else reason
            }

            if hasattr(self, 'crawler') and self.crawler:
                self.crawler.stats.set_value('url_stats', spider_stats)
            
            if self.close_callback:
                self.close_callback(self)

            # Log kết quả cuối cùng
            status_msg = (
                f"Reached limit of {self.MAX_PAGES} pages. " if reached_limit 
                else "Completed scanning all available pages. "
            )
            
            self.logger.info(
                f"Spider closed. {status_msg}"
                f"Found {total_urls} URLs across {self.max_level_found + 1} levels "
                f"in {duration:.2f} seconds"
            )
            
            for level, stats in level_stats.items():
                self.logger.info(
                    f"Level {level}: {stats['count']} URLs "
                    f"({stats['percentage']:.1f}%)"
                )