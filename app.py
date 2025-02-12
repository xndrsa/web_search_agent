import random
import asyncio
from duckduckgo_search import DDGS
from tools.new_tools import get_proxy_list
import pandas as pd
from tools.scrape import scrape_url_list
from typing import List, Dict, Optional
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_groq import ChatGroq
from datetime import datetime
import os



class SearchScraper:
    def __init__(self):
        self.sleep_times = [2, 3, 4, 5, 6]
        self.max_search_results = 3
        self.proxies_list = get_proxy_list()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/90.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59"
        ]
        self.ddgs = DDGS(timeout=20)
        self.llm = ChatGroq(
            model="gemma2-9b-it",
            temperature=0.0,
            max_retries=2,
            callbacks=[],
            verbose=True,
            api_key=os.getenv('GROQ_API_KEY')
        )
        print("LLM initialized successfully")
        self.prompt_templates = {
            "product": """You are a product search specialist. Based on the search results, provide details about: {query}

            TASK:
            1. Extract key product information from search results
            2. Identify: Product name, category/type, price, and source URL
            3. Format response according to specification

            Available information:
            {search_results}

            RULES:
            - Only use prices directly found in search results
            - Use "Price not found" if no clear price is available
            - Prefer official retailer URLs (Amazon, Home Depot, etc.)
            - If information is uncertain, mark as "Not found"
            - NO GUESSING OR HALLUCINATION

            OUTPUT FORMAT:
            [Product Full Name]<||>[Category/Type]<||>[Price]<||>[Source URL]""",
            
            "location": """You are a location specialist. Based on the search results, provide details about: {query}

            TASK:
            1. Extract location information from search results
            2. Identify: Location name, type, country, population, area
            3. Format response according to specification

            Available information:
            {search_results}

            OUTPUT FORMAT:
            [Location Name]<||>[Type]<||>[Country]<||>[Population]<||>[Area]""",
            
            "company": """You are a company specialist. Based on the search results, provide details about: {query}

            TASK:
            1. Extract company information from search results
            2. Identify: Company name, industry, revenue, headquarters
            3. Format response according to specification

            Available information:
            {search_results}

            OUTPUT FORMAT:
            [Company Name]<||>[Industry]<||>[Revenue]<||>[Headquarters]"""
        }
        print(f"Found {len(self.proxies_list)} proxies")

    async def search_with_proxy(self, query: str) -> List[Dict]:
        proxy = random.choice(self.proxies_list)
        proxy_url = f"socks5://{proxy['ip']}:{proxy['port']}"
        user_agent = random.choice(self.user_agents)
        print(f"\nUsing proxy {proxy_url}")
        print(f"Searching for: {query}")

        self.ddgs.proxy = proxy_url
        self.ddgs.headers = {"User-Agent": user_agent}

        try:
            results = list(self.ddgs.text(query, max_results=self.max_search_results))
            print(f"Found {len(results)} results")
            return results
        except Exception as e:
            print(f"Search error: {e}")
            return []
        finally:
            await asyncio.sleep(random.choice(self.sleep_times))

    async def process_llm(self, query: str, search_type: str, search_results: pd.DataFrame) -> Optional[str]:
        """Process search results with LLM"""
        try:
            print("\n=== LLM Processing Start ===")
            print(f"Processing query: {query}")
            print(f"Search type: {search_type}")
            
            if not isinstance(search_results, pd.DataFrame):
                print(f"Error: search_results is not a DataFrame, got {type(search_results)}")
                return None
                
            if search_results.empty:
                print("Error: Empty search results DataFrame")
                return None

            #Format search results for LLM
            formatted_results = ""
            for _, row in search_results.iterrows():
                formatted_results += f"SOURCE: {row['url']}\n"
                formatted_results += f"TITLE: {row['title']}\n"
                formatted_results += f"DESCRIPTION: {row['description']}\n"
                formatted_results += f"CONTENT: {row['body'][:500]}...\n"
                formatted_results += "-" * 80 + "\n\n"

            print(f"\nFormatted {len(search_results)} results for LLM")
            print("Sample of formatted content:")
            print(formatted_results[:500] + "..." if len(formatted_results) > 500 else formatted_results)

            template = self.prompt_templates.get(search_type, self.prompt_templates["product"])
            
            search_prompt = ChatPromptTemplate.from_messages([
                SystemMessage(content=template),
                MessagesPlaceholder(variable_name="scratchpad"),
                ("human", "{input}\n\nSearch Results:\n{search_results}")
            ])
            
            messages = search_prompt.format_messages(
                query=query,
                input=query,
                search_results=formatted_results,
                scratchpad=[]
            )
            
            print("\nSending to LLM with formatted content...")
            
            try:
                response = self.llm.invoke(messages)
                print("\nRaw LLM Response:", response)
                
                if hasattr(response, 'content'):
                    content = response.content.strip()
                    if content:
                        return content
                    else:
                        print("Warning: Empty content from LLM")
                        return None
                else:
                    print(f"Error: Unexpected response format: {type(response)}")
                    return None
                    
            except Exception as llm_error:
                print(f"LLM invocation error: {str(llm_error)}")
                import traceback
                print("LLM Traceback:", traceback.format_exc())
                return None
                
        except Exception as e:
            print(f"LLM processing error: {str(e)}")
            import traceback
            print("Process LLM Traceback:", traceback.format_exc())
            return None
        finally:
            print("=== LLM Processing End ===\n")

    def parse_llm_response(self, response: str, search_type: str) -> Dict:
        if not response:
            return {}
            
        parts = response.split("<||>")
        
        if search_type == "product":
            return {
                "product_name": parts[0] if len(parts) > 0 else "Not found",
                "category": parts[1] if len(parts) > 1 else "Not found",
                "price": parts[2] if len(parts) > 2 else "Not found",
                "source_url": parts[3] if len(parts) > 3 else "Not found"
            }
        elif search_type == "location":
            return {
                "location_name": parts[0] if len(parts) > 0 else "Not found",
                "type": parts[1] if len(parts) > 1 else "Not found",
                "country": parts[2] if len(parts) > 2 else "Not found",
                "population": parts[3] if len(parts) > 3 else "Not found",
                "area": parts[4] if len(parts) > 4 else "Not found"
            }
        elif search_type == "company":
            return {
                "company_name": parts[0] if len(parts) > 0 else "Not found",
                "industry": parts[1] if len(parts) > 1 else "Not found",
                "revenue": parts[2] if len(parts) > 2 else "Not found",
                "headquarters": parts[3] if len(parts) > 3 else "Not found"
            }
        return {}

    async def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        all_results = []
        
        for index, row in df.iterrows():
            print(f"\n{'='*50}")
            print(f"Processing row {index + 1}/{len(df)}")
            print(f"Query: {row['query']}")
            print(f"Search type: {row['search_type']}")
            
            try:
                search_results = await self.search_with_proxy(row['query'])
                print(f"\nSearch results type: {type(search_results)}")
                print(f"Search results count: {len(search_results) if search_results else 0}")
                
                if search_results:
                    ddg_results = []
                    for result in search_results:
                        ddg_data = {
                            'url': result.get('link') or result.get('href', ''),
                            'title': result.get('title', 'No title'),
                            'description': result.get('snippet', 'No description'),
                            'body': f"{result.get('title', '')} - {result.get('snippet', '')}",
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'method': 'duckduckgo'
                        }
                        ddg_results.append(ddg_data)
                    
                    ddg_df = pd.DataFrame(ddg_results)
                    print("\nDuckDuckGo results created")
                    
                    urls = [result.get('link') or result.get('href') for result in search_results 
                           if result.get('link') or result.get('href')]
                    print(f"Found {len(urls)} URLs to scrape: {urls}")
                    
                    combined_results = ddg_df.copy()
                    
                    try:
                        if urls:
                            print("\nAttempting scraping...")
                            scraped_df = scrape_url_list(urls)
                            if isinstance(scraped_df, pd.DataFrame) and not scraped_df.empty:
                                print("Adding scraped data to results")
                                combined_results = pd.concat([combined_results, scraped_df], ignore_index=True)
                                combined_results = combined_results.drop_duplicates(subset=['url'], keep='last')
                    except Exception as scrape_error:
                        print(f"Scraping failed: {str(scrape_error)}")
                    
                    if not combined_results.empty:
                        print(f"\nCombined data shape: {combined_results.shape}")
                        print("Sources:", combined_results['method'].value_counts().to_dict())
                        

                        print("\nSending combined results to LLM...")
                        llm_response = await self.process_llm(row['query'], row['search_type'], combined_results)
                        print("\nLLM Response received:", llm_response)
                        
                        if llm_response:
                            parsed_response = self.parse_llm_response(llm_response, row['search_type'])
                            print("\nParsed Response:", parsed_response)
                            
                            # Combine original query with LLM results
                            result = {
                                'original_query': row['query'],
                                'search_type': row['search_type'],
                                **parsed_response
                            }
                            all_results.append(result)
                            print(f"\nAdded result for query: {row['query']}")
                        else:
                            result = self._create_default_response(row, 'llm_failed', combined_results)
                            all_results.append(result)
                    else:
                        result = self._create_default_response(row, 'no_data')
                        all_results.append(result)
                else:
                    result = self._create_default_response(row, 'no_results')
                    all_results.append(result)
                
            except Exception as e:
                print(f"Error processing row {index}: {str(e)}")
                result = self._create_default_response(row, 'error')
                all_results.append(result)
                continue
        
        if all_results:
            final_df = pd.DataFrame(all_results)
            print("\nFinal DataFrame columns:", final_df.columns.tolist())
            print("\nNumber of results:", len(final_df))
            return final_df
        
        print("No results to create DataFrame")
        return pd.DataFrame()

    def _create_default_response(self, row: pd.Series, status: str, data: pd.DataFrame = None) -> Dict:
        """Helper method to create default responses"""
        response = {
            'original_query': row['query'],
            'search_type': row['search_type'],
            'product_name': 'Not found',
            'category': 'Not found',
            'price': 'Not found',
            'source_url': 'Not found'
        }
        
        if status == 'error':
            response.update({
                'product_name': 'Error',
                'category': 'Error',
                'price': 'Error',
                'source_url': 'Error'
            })
        
        return response

async def main():
    # Test DataFrame
    df = pd.read_csv("search_data.csv")
    
    scraper = SearchScraper()
    result_df = await scraper.process_dataframe(df)
    
    if not result_df.empty:
        print("\nFinal Results:")
        print(result_df)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_df.to_csv(f'search_results_{timestamp}.csv', index=False)
        print(f"\nResults saved to search_results_{timestamp}.csv")
    else:
        print("\nNo results found")

if __name__ == "__main__":
    asyncio.run(main())