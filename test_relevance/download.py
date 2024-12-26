import sqlite3
import requests
from pathlib import Path
import time

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "database" / "arxiv_relevance.db"
PDF_DIR = SCRIPT_DIR / "pdfs"

def download_pdf(url, file_path):
    """Download PDF from URL with retries"""
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            return True
            
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Retry {attempt + 1}/{max_retries} for {url}: {e}")
                time.sleep(retry_delay)
            else:
                print(f"Failed to download {url}: {e}")
                return False

def get_papers():
    """Get papers from database that need PDFs"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, pdf_url 
            FROM full_documents
        """)
        
        return cursor.fetchall()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
        
    finally:
        conn.close()

def main():
    """Download PDFs for papers in database"""
    # Create PDF directory if it doesn't exist
    PDF_DIR.mkdir(exist_ok=True)
    
    papers = get_papers()
    if not papers:
        print("No papers found in database")
        return
    
    print(f"\nFound {len(papers)} papers to process")
    
    # Download PDFs
    for paper_id, pdf_url in papers:
        pdf_path = PDF_DIR / f"{paper_id}.pdf"
        
        if pdf_path.exists():
            print(f"PDF already exists for {paper_id}")
            continue
            
        print(f"\nDownloading PDF for {paper_id}...")
        if download_pdf(pdf_url, pdf_path):
            print(f"✓ Downloaded {paper_id}")
        
        # Be nice to the server
        time.sleep(1)
    
    print("\nFinished downloading PDFs")

if __name__ == "__main__":
    main() 