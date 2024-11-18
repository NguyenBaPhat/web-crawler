# web_crawler/spiders/fast_spider.py

import scrapy
from urllib.parse import urlparse
import os
from datetime import datetime
import logging


class FastSpider(scrapy.Spider):
    name = 'fast_spider'
    custom_settings = {
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
        'DOWNLOAD_DELAY': 0.1,
        'COOKIES_ENABLED': False,
        'DOWNLOAD_TIMEOUT': 15,
        'LOG_LEVEL': 'INFO',
        'DNSCACHE_ENABLED': True,
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter'
    }

    def __init__(self, url=None, page_limit=None, task_id=None, *args, **kwargs):
        super(FastSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url]
        self.allowed_domain = urlparse(self.start_urls[0]).netloc
        self.page_limit = int(page_limit)
        self.pages_crawled = 0
        self.visited_urls = set()
        self.task_id = task_id

        # Create output directory with task_id
        self.output_dir = f'crawled_pages_{task_id}'
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # Create log files
        self.index_file = os.path.join(self.output_dir, 'index.txt')
        self.error_file = os.path.join(self.output_dir, 'errors.log')
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        # Setup error logging
        self.error_logger = logging.getLogger(f'error_logger_{self.task_id}')
        error_handler = logging.FileHandler(self.error_file)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.error_logger.addHandler(error_handler)
        self.error_logger.setLevel(logging.ERROR)
        
        # Initialize index file
        with open(self.index_file, 'w', encoding='utf-8') as f:
            f.write(f"Crawl started at: {datetime.now()}\n")
            f.write(f"Start URL: {self.start_urls[0]}\n")
            f.write(f"Page limit: {self.page_limit}\n\n")
            f.write("Index of crawled pages:\n")
            f.write("=" * 80 + "\n\n")

    def extract_text(self, element):
        """Extract text from element, including text in child elements"""
        return ' '.join([text.strip() for text in element.css('*::text').getall() if text.strip()])

    def extract_content(self, response):
        """Extract all content from page"""
        try:
            content = {
                'url': response.url,
                'crawl_time': str(datetime.now()),
                'title': response.css('title::text').get('').strip(),
                'meta_description': response.css('meta[name="description"]::attr(content)').get('').strip(),
                'headings': {},
                'paragraphs': [],
                'links': [],
                'text_content': []
            }

            # Extract headings
            for heading in response.css('h1, h2, h3, h4, h5, h6'):
                level = heading.xpath('name()').get()
                text = self.extract_text(heading)
                if level not in content['headings']:
                    content['headings'][level] = []
                content['headings'][level].append(text)

            # Extract paragraphs
            content['paragraphs'] = [self.extract_text(p) for p in response.css('p') if self.extract_text(p)]

            # Extract links
            for link in response.css('a'):
                href = link.css('::attr(href)').get()
                text = self.extract_text(link)
                if href and text:
                    content['links'].append({
                        'text': text,
                        'href': response.urljoin(href)
                    })

            return content
            
        except Exception as e:
            self.error_logger.error(f"Content extraction error on {response.url}: {str(e)}")
            return None

    def save_content(self, content, filename):
        """Save content to file"""
        try:
            with open(os.path.join(self.output_dir, filename), 'w', encoding='utf-8') as f:
                f.write(f"URL: {content['url']}\n")
                f.write(f"Crawled at: {content['crawl_time']}\n")
                f.write("=" * 80 + "\n\n")

                sections = [
                    ('TITLE', content['title']),
                    ('META DESCRIPTION', content['meta_description']),
                    ('HEADINGS', content['headings']),
                    ('CONTENT', content['paragraphs']),
                    ('LINKS', content['links'])
                ]

                for section_name, section_content in sections:
                    f.write(f"{section_name}:\n")
                    if isinstance(section_content, dict):
                        for key, value in section_content.items():
                            f.write(f"\n{key}:\n")
                            for item in value:
                                f.write(f"  - {item}\n")
                    elif isinstance(section_content, list):
                        for item in section_content:
                            if isinstance(item, dict):
                                f.write(f"  - {item.get('text', '')}: {item.get('href', '')}\n")
                            else:
                                f.write(f"  - {item}\n")
                    else:
                        f.write(f"{section_content}\n")
                    f.write("\n" + "-" * 80 + "\n\n")

        except Exception as e:
            self.error_logger.error(f"Error saving content to {filename}: {str(e)}")

    def parse(self, response):
        """Process each crawled page"""
        # Check if we've reached the page limit
        if self.pages_crawled >= self.page_limit:
            # Log completion and close spider
            self.crawler.engine.close_spider(self, 'Reached page limit')
            return

        if response.url in self.visited_urls:
            return

        try:
            # Extract and save content
            content = self.extract_content(response)
            if content:
                filename = f"page_{self.pages_crawled + 1}.txt"
                self.save_content(content, filename)

                # Update index
                with open(self.index_file, 'a', encoding='utf-8') as f:
                    f.write(f"{self.pages_crawled + 1}. {content['title']}\n")
                    f.write(f"   URL: {response.url}\n")
                    f.write(f"   File: {filename}\n\n")

                # Update status
                self.visited_urls.add(response.url)
                self.pages_crawled += 1
                
                print(f'Successfully crawled: {response.url} ({self.pages_crawled}/{self.page_limit} pages)')

                # Find and crawl new links if we haven't reached the limit
                if self.pages_crawled < self.page_limit:
                    for link in content['links']:
                        url = link['href']
                        if (url not in self.visited_urls and 
                            self.allowed_domain in urlparse(url).netloc):
                            yield scrapy.Request(
                                url,
                                callback=self.parse,
                                errback=self.errback,
                                dont_filter=True
                            )

        except Exception as e:
            self.error_logger.error(f'Error processing {response.url}: {str(e)}')

    def errback(self, failure):
        """Handle request failures"""
        self.error_logger.error(f'Request failed: {str(failure.value)}')

    def closed(self, reason):
        """Called when crawler finishes"""
        try:
            # Write closing information to index file
            with open(self.index_file, 'a', encoding='utf-8') as f:
                f.write(f"\nCrawl finished at: {datetime.now()}\n")
                f.write(f"Total pages crawled: {self.pages_crawled}\n")
                f.write(f"Reason: {reason}\n")
            
            logging.info(f"Crawler completed. Task ID: {self.task_id}")
            logging.info(f"Pages crawled: {self.pages_crawled}")
            logging.info(f"Output directory: {self.output_dir}")
            
        except Exception as e:
            logging.error(f"Error in closed method: {str(e)}")