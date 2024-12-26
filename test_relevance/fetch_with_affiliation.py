import sqlite3
import arxiv
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json
import time

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent  # test_relevance directory
PROJECT_ROOT = SCRIPT_DIR.parent  # go up one level to reach project root
DB_PATH = SCRIPT_DIR / "database" / "arxiv_relevance.db"
CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"

def load_config():
    """Load configuration from config/config.json"""
    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
        return config["queries"]["JEPA"]  # Using JEPA query as base
    except FileNotFoundError:
        print(f"Config file not found at {CONFIG_PATH}")
        raise
    except KeyError as e:
        print(f"Missing key in config file: {e}")
        raise

def get_paper_metadata(paper_id):
    """Fetch paper metadata from arXiv API"""
    # Use the metadata endpoint
    metadata_url = f"https://export.arxiv.org/api/query?id_list={paper_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(metadata_url, headers=headers)
        response.raise_for_status()
        
        # Parse XML response
        soup = BeautifulSoup(response.content, 'xml')
        
        # Find all affiliation tags
        affiliations = []
        
        # Print raw XML for debugging
        print(f"\nRaw XML for {paper_id}:")
        print(response.text)
        
        # Try to find affiliations in different ways
        for tag in soup.find_all(['arxiv:affiliation', 'affiliation']):
            if tag.string:
                aff = tag.string.strip()
                print(f"Found affiliation: {aff}")
                affiliations.append(aff)
        
        # Also check the comment field as it sometimes contains affiliation info
        comment = soup.find('arxiv:comment')
        if comment and comment.string:
            comment_text = comment.string.strip()
            if any(keyword in comment_text.lower() for keyword in ['university', 'institute', 'lab', 'corporation', 'inc.', 'company']):
                print(f"Found affiliation in comment: {comment_text}")
                affiliations.append(comment_text)
        
        if not affiliations:
            print(f"No affiliations found for {paper_id}")
            
        return '; '.join(set(affiliations)) if affiliations else None
        
    except Exception as e:
        print(f"Error fetching metadata for {paper_id}: {e}")
        return None

def fetch_papers():
    """Fetch papers from arXiv with increased max_results"""
    config = load_config()
    
    # Build search query
    search_query = " OR ".join(config["keywords"])
    
    # Search arXiv with increased max_results
    search = arxiv.Search(
        query=search_query,
        max_results=5,  # Increased to 40 papers
        sort_by=arxiv.SortCriterion.SubmittedDate
    )

    papers = []
    for result in search.results():
        # Get paper ID without version number
        paper_id = result.get_short_id().split('v')[0]
        
        # Get metadata including affiliations
        affiliations = get_paper_metadata(paper_id)
        print(f"Found affiliations for {paper_id}: {affiliations}")
        
        paper = {
            'id': paper_id,
            'title': result.title,
            'authors': ', '.join(author.name for author in result.authors),
            'affiliation': affiliations,
            'pdf_url': result.pdf_url,
            'abstract': result.summary,
            'conclusion': None  # Will be populated by extract_regex later
        }
        papers.append(paper)
        
        # Be nice to the API
        time.sleep(1)
    
    return papers

def save_to_db(papers):
    """Save papers to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Save to full_documents table
        cursor.executemany("""
            INSERT OR REPLACE INTO full_documents 
            (id, title, authors, affiliation, pdf_url, abstract, conclusion, selected)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [(p['id'], p['title'], p['authors'], p['affiliation'], 
               p['pdf_url'], p['abstract'], p['conclusion'], None) for p in papers])
        
        # Initialize entries in summaries table
        cursor.executemany("""
            INSERT OR IGNORE INTO summaries (id)
            VALUES (?)
        """, [(p['id'],) for p in papers])
        
        conn.commit()
        print(f"Saved {len(papers)} papers to database")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        
    finally:
        conn.close()

def main():
    """Fetch papers and save to database"""
    print("\nFetching papers from arXiv...")
    papers = fetch_papers()
    if papers:
        save_to_db(papers)
        print("✓ Finished fetching papers with affiliations")
    else:
        print("No papers found")

if __name__ == "__main__":
    main() 