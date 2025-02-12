import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from typing import Dict, Optional, List
import logging
from urllib.parse import urlparse
import time
from selenium.common.exceptions import NoSuchElementException
from requests.exceptions import RequestException
import re
import pandas as pd
from datetime import datetime
import html









class WebScraper:
    def __init__(self, timeout: int = 20, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self._setup_logging()
        self._setup_selenium_options()
        self.results = []

    def _setup_logging(self) -> None:
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _setup_selenium_options(self) -> None:
        self.options = Options()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--window-size=1920,1080')
        self.options.add_argument('--disable-notifications')
        self.service = Service(GeckoDriverManager().install())

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = html.unescape(text)  # Decode HTML entities
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text

    def _get_content_requests(self, url: str) -> Optional[Dict[str, str]]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, "html.parser")
                
                title = self._clean_text(soup.title.text) if soup.title else ""
                
                description = soup.find("meta", {"name": ["description", "Description"]})
                if not description:
                    description = soup.find("meta", {"property": "og:description"})
                description = self._clean_text(description["content"]) if description else ""
                
                body = self._clean_text(soup.body.get_text(" ")) if soup.body else ""
                
                return {
                    "url": url,
                    "title": title or "No title found",
                    "description": description or "No description found",
                    "body": body[:1000] if body else "No body content found",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "method": "requests"
                }

            except RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed with requests: {str(e)}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(2)

    def _get_content_selenium(self, url: str) -> Optional[Dict[str, str]]:
        driver = None
        try:
            driver = webdriver.Firefox(service=self.service, options=self.options)
            driver.set_page_load_timeout(self.timeout)
            
            driver.get(url)
            wait = WebDriverWait(driver, self.timeout)
            
            title = driver.title
            
            description = ""
            try:
                description = driver.find_element(By.CSS_SELECTOR, 
                    'meta[name="description"], meta[property="og:description"]').get_attribute("content")
            except NoSuchElementException:
                pass
            
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            body = driver.find_element(By.TAG_NAME, "body").text
            
            return {
                "url": url,
                "title": self._clean_text(title) or "No title found",
                "description": self._clean_text(description) or "No description found",
                "body": self._clean_text(body)[:1000] if body else "No body content found",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "method": "selenium"
            }
            
        except Exception as e:
            self.logger.error(f"Selenium error: {str(e)}")
            return None
            
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    self.logger.error(f"Error closing driver: {str(e)}")

    def scrape(self, urls: List[str]) -> pd.DataFrame:
        """Scrape multiple URLs and return results as DataFrame"""
        results = []
        
        for url in urls:
            self.logger.info(f"Scraping: {url}")
            
            try:
                result = urlparse(url)
                if not all([result.scheme, result.netloc]):
                    raise ValueError("Invalid URL format")
                
                content = self._get_content_requests(url)
                if not content or all(val in ["", "No title found", "No description found", "No body content found"] 
                                   for val in [content['title'], content['description'], content['body']]):
                    self.logger.info("Falling back to Selenium")
                    content = self._get_content_selenium(url)
                
                if content:
                    results.append(content)
                    self.logger.info(f"Successfully scraped using {content['method']}")
                else:
                    self.logger.error("Failed to scrape with both methods")
                    results.append({
                        "url": url,
                        "title": "Failed to scrape",
                        "description": "Failed to scrape",
                        "body": "Failed to scrape",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "method": "failed"
                    })
                
            except Exception as e:
                self.logger.error(f"Error processing {url}: {str(e)}")
                results.append({
                    "url": url,
                    "title": f"Error: {str(e)}",
                    "description": "Error occurred",
                    "body": "Error occurred",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "method": "error"
                })
            
            time.sleep(1)
        
        return pd.DataFrame(results)

def scrape_url_list(urls):
    "List of urls to scrape"    
    scraper = WebScraper(timeout=20, max_retries=3)
    df = scraper.scrape(urls)
    print("\nScraping Results:")
    print(df[['url', 'title', 'method']])


