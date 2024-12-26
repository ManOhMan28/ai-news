import sqlite3
import arxiv
from pathlib import Path
import json

# Hardcoded query name
QUERY_NAME = "JEPA"

def load_config():
    """Load configuration from config/config.json"""
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    with open(config_path) as f:
        config = json.load(f)
    
    if QUERY_NAME not in config["queries"]:
        raise ValueError(f"Query '{QUERY_NAME}' not found in config file")
    
    return config["queries"][QUERY_NAME]

def fetch_papers():
    """Fetch papers from arXiv based on config"""
    config = load_config()
    
    # Build search query
    search_query = " OR ".join(config["keywords"])
    max_results = config["max_results"]
    
    # Search arXiv
    search = arxiv.Search(
        query=search_query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate
    )

    # Convert iterator to list and slice to get exact number of results
    results = list(search.results())[:max_results]
    
    papers = []
    for result in results:
        paper = {
            'id': result.get_short_id(),
            'title': result.title,
            'authors': ', '.join(author.name for author in result.authors),
            'pdf_url': result.pdf_url
        }
        papers.append(paper)
    
    print(f"Fetched {len(papers)} papers (max_results={max_results})")
    return papers  # Already limited by slicing results

def save_to_db(papers):
    """Save papers to database"""
    db_path = Path(__file__).parent.parent / "database" / "arxiv_docs.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Insert into full_documents table
        cursor.executemany("""
            INSERT OR REPLACE INTO full_documents 
            (id, title, authors, pdf_url)
            VALUES (?, ?, ?, ?)
        """, [(p['id'], p['title'], p['authors'], p['pdf_url']) for p in papers])
        
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
    papers = fetch_papers()
    save_to_db(papers)
    print("\nFinished fetching papers")

if __name__ == "__main__":
    main()
