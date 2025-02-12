import streamlit as st
import asyncio
import pandas as pd
from tools.scrape import scrape_url_list
from datetime import datetime
from app import SearchScraper  # Reuse the main class
import time

# Configure Streamlit page
st.set_page_config(
    page_title="Search Agent",
    page_icon="🔍",
    layout="wide"
)

# Initialize session state
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'results' not in st.session_state:
    st.session_state.results = None
if 'progress' not in st.session_state:
    st.session_state.progress = 0

async def process_with_progress(df: pd.DataFrame, progress_bar) -> pd.DataFrame:
    """Process DataFrame with progress updates"""
    scraper = SearchScraper()
    all_results = []
    total_rows = len(df)
    
    for index, row in df.iterrows():
        try:
            # Update progress
            progress = (index + 1) / total_rows
            progress_bar.progress(progress)
            st.write(f"Processing {row['query']} ({index + 1}/{total_rows})")
            
            search_results = await scraper.search_with_proxy(row['query'])
            
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
                
                urls = [result.get('link') or result.get('href') for result in search_results 
                       if result.get('link') or result.get('href')]
                
                combined_results = ddg_df.copy()
                
                try:
                    if urls:
                        scraped_df = scrape_url_list(urls)
                        if isinstance(scraped_df, pd.DataFrame) and not scraped_df.empty:
                            combined_results = pd.concat([combined_results, scraped_df], ignore_index=True)
                            combined_results = combined_results.drop_duplicates(subset=['url'], keep='last')
                except Exception as scrape_error:
                    st.error(f"Scraping error for {row['query']}: {str(scrape_error)}")
                
                if not combined_results.empty:
                    llm_response = await scraper.process_llm(row['query'], row['search_type'], combined_results)
                    
                    if llm_response:
                        parsed_response = scraper.parse_llm_response(llm_response, row['search_type'])
                        result = {
                            'original_query': row['query'],
                            'search_type': row['search_type'],
                            **parsed_response
                        }
                        all_results.append(result)
                    else:
                        all_results.append(scraper._create_default_response(row, 'llm_failed'))
                else:
                    all_results.append(scraper._create_default_response(row, 'no_data'))
            else:
                all_results.append(scraper._create_default_response(row, 'no_results'))
            
        except Exception as e:
            st.error(f"Error processing {row['query']}: {str(e)}")
            all_results.append(scraper._create_default_response(row, 'error'))
    
    progress_bar.progress(1.0)
    return pd.DataFrame(all_results)

def save_dataframe(df: pd.DataFrame) -> str:
    """Save DataFrame to CSV and return filename"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'search_results_{timestamp}.csv'
    df.to_csv(filename, index=False)
    return filename

def main():
    st.title("🔍 Search Agent")
    st.write("Upload a CSV file with 'query' and 'search_type' columns to process")
    
    # File uploader
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            
            if 'query' not in df.columns or 'search_type' not in df.columns:
                st.error("CSV must contain 'query' and 'search_type' columns")
                return
                
            st.write("Preview of input data:")
            st.dataframe(df.head())
            
            if st.button("Process Queries"):
                st.session_state.processing = True
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Process data
                start_time = time.time()
                results_df = asyncio.run(process_with_progress(df, progress_bar))
                end_time = time.time()
                
                # Save results
                if not results_df.empty:
                    filename = save_dataframe(results_df)
                    
                    st.success(f"Processing completed in {end_time - start_time:.2f} seconds!")
                    st.write("Results preview:")
                    st.dataframe(results_df)
                    
                    # Provide download link
                    with open(filename, 'rb') as f:
                        st.download_button(
                            label="Download Results",
                            data=f,
                            file_name=filename,
                            mime='text/csv'
                        )
                else:
                    st.error("No results were generated")
                
                st.session_state.processing = False
                
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    main()
