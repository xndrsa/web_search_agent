import scrapy
from scrapy.http import HtmlResponse
from bs4 import BeautifulSoup

class JinaSpider(scrapy.Spider):
    name = "jina_spider"
    all_results = []
    
    def __init__(self, urls_list=None, *args, **kwargs):
        super(JinaSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls_list or []
        JinaSpider.all_results = []

    def start_requests(self):
        seen_urls = set()
        for url in self.start_urls:
            if url not in seen_urls:
                seen_urls.add(url)
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response: HtmlResponse):
        try:
            print(f"\nProcessing URL: {response.url}")
            
            if any(x in response.text.lower() for x in ['request rejected', 'access denied', 'captcha']):
                print(f"Access blocked for URL: {response.url}")
                print("Response status:", response.status)
                print("Response headers:", response.headers)
                return
                
            soup = BeautifulSoup(response.body, 'html.parser')
            
            title = soup.title.string if soup.title else "No title"
            print(f"Page title: {title}")
            
            for element in soup(['script', 'style', 'meta', 'link', 'footer', 'nav', 'aside']):
                element.decompose()
            
            # Focus on main content areas
            main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
            if main_content:
                text_content = ' '.join(main_content.stripped_strings)
            else:
                # Fallback to body content
                text_content = ' '.join(soup.body.stripped_strings if soup.body else soup.stripped_strings)
            
            # Clean up the text
            text_content = ' '.join(
                text for text in text_content.split()
                if len(text) > 1
            )
            
            if len(text_content) > 2000:
                text_content = text_content[:2000]
                last_period = text_content.rfind('.')
                if last_period > 0:
                    text_content = text_content[:last_period + 1]
            
            if len(text_content.strip()) > 100:
                JinaSpider.all_results.append({
                    '####url': response.url,
                    '####content': text_content.strip()
                })
                print(f"Successfully extracted {len(text_content)} characters")
            else:
                print(f"Extracted content too short: {len(text_content)} characters")

        except Exception as e:
            print(f"Error parsing {response.url}: {str(e)}")
            print("Response status:", response.status)
            print("Response headers:", response.headers)

from scrapy.crawler import CrawlerProcess

def run_spider(list_of_results):
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "ERROR"
    })
    
    process.crawl(JinaSpider, urls_list=list_of_results)
    process.start()
    
    return JinaSpider.all_results

def get_default_response(search_type, error_msg="Information not available"):
    """Return properly formatted default response based on search type"""
    if search_type.lower() == 'product':
        return f"Not available<||>Not available<||>Not available<||>{error_msg}"
    elif search_type.lower() == 'location':
        return f"Not available<||>Not available<||>Not available<||>Not available<||>{error_msg}"
    elif search_type.lower() == 'company':
        return f"Not available<||>Not available<||>Not available<||>{error_msg}"
    else:
        return f"Not available<||>{error_msg}"

def format_response(search_type, found_info, query):
    """Format unstructured response into required format"""
    try:
        if search_type.lower() == 'product':
            return "<||>".join([
                next((info for info in found_info if info.strip()), "Not available"),
                next((info for info in found_info if any(x in info.lower() for x in ['category', 'type', 'product'])), "Category not available"),
                next((info for info in found_info if '$' in info), "Price not available"),
                next((info for info in found_info if 'http' in info.lower()), "URL not available")
            ])
        elif search_type.lower() == 'location':
            return "<||>".join([
                query,
                next((info for info in found_info if any(x in info.lower() for x in ['city', 'state', 'country', 'capital'])), "Type not available"),
                next((info for info in found_info if any(x in info.lower() for x in ['located in', 'country:', 'nation:'])), "Country not available"),
                next((info for info in found_info if any(x in info.lower() for x in ['population', 'inhabitants', 'people'])), "Population not available"),
                next((info for info in found_info if any(x in info.lower() for x in ['km²', 'km2', 'square', 'area'])), "Area not available")
            ])
        elif search_type.lower() == 'company':
            return "<||>".join([
                query,
                next((info for info in found_info if any(x in info.lower() for x in ['industry', 'sector', 'business'])), "Industry not available"),
                next((info for info in found_info if any(x in info.lower() for x in ['revenue', 'sales', '$'])), "Revenue not available"),
                next((info for info in found_info if any(x in info.lower() for x in ['headquarters', 'based in', 'located'])), "Location not available")
            ])
    except Exception:
        return None