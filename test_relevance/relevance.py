import json
import sqlite3
from pathlib import Path
from ollama import Client
from pydantic import BaseModel

# Define the response schema
class PaperEvaluation(BaseModel):
    is_prestigious: bool
    reason: str  # Added to capture the reasoning

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
        return config["models"]["relevance"]
    except FileNotFoundError:
        print(f"Config file not found at {CONFIG_PATH}")
        raise
    except KeyError as e:
        print(f"Missing key in config file: {e}")
        raise

def load_papers():
    """Load papers from database that haven't been evaluated yet"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, title, authors, affiliation, abstract 
            FROM full_documents 
            WHERE selected IS NULL
        """)
        
        papers = {}
        for row in cursor.fetchall():
            doc_id, title, authors, affiliation, abstract = row
            papers[doc_id] = {
                'title': title,
                'authors': authors,
                'affiliation': affiliation if affiliation else "Not provided",
                'abstract': abstract
            }
        
        return papers
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return {}
    finally:
        if 'conn' in locals():
            conn.close()

def evaluate_paper(paper):
    """Evaluate paper using LLM based on affiliation prestige"""
    config = load_config()
    
    # Initialize Ollama client
    client = Client(host='http://localhost:11434')
    
    prompt = f"""Paper Information:
Title: {paper['title']}
Authors: {paper['authors']}
Affiliation: {paper['affiliation']}
Abstract: {paper['abstract']}

Analyze the affiliation information and determine if this is from a prestigious institution."""

    try:
        response = client.generate(
            model=config["model"],
            prompt=prompt,
            format=PaperEvaluation.model_json_schema(),
            stream=False
        )
        
        print("\nModel response:")
        print(response['response'])
        
        try:
            result = PaperEvaluation.model_validate_json(response['response'])
            print(f"Decision: {'Prestigious' if result.is_prestigious else 'Not prestigious'}")
            print(f"Reason: {result.reason}")
            return 'yes' if result.is_prestigious else 'no'
        except Exception as e:
            print(f"Error parsing structured response: {e}")
            return 'no'  # Default to 'no' if there's an error
            
    except Exception as e:
        print(f"Error evaluating paper: {str(e)}")
        return 'no'  # Default to 'no' if there's an error

def save_evaluation(paper_id, selection):
    """Save evaluation result to database"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE full_documents 
            SET selected = ?
            WHERE id = ?
        """, (selection, paper_id))
        
        conn.commit()
        if selection == 'yes':
            print(f"Selected paper {paper_id}")
        else:
            print(f"Rejected paper {paper_id}")
        
    except sqlite3.Error as e:
        print(f"Database error for {paper_id}: {e}")
        
    finally:
        if conn:
            conn.close()

def cleanup_unselected():
    """Update statistics about selected/rejected papers"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Count papers by selection status
        cursor.execute("""
            SELECT selected, COUNT(*) 
            FROM full_documents 
            GROUP BY selected
        """)
        stats = cursor.fetchall()
        
        print("\nPaper Selection Statistics:")
        for status, count in stats:
            if status == 'yes':
                print(f"Selected (prestigious): {count} papers")
            elif status == 'no':
                print(f"Rejected (not prestigious): {count} papers")
            else:
                print(f"Pending evaluation: {count} papers")
        
    except sqlite3.Error as e:
        print(f"Database error during statistics gathering: {e}")
        
    finally:
        if conn:
            conn.close()

def main():
    """Process papers and evaluate their relevance"""
    config = load_config()
    print(f"\nUsing model: {config['model']}")
    
    # Load papers
    papers = load_papers()
    
    if not papers:
        print("No papers found to evaluate")
        return
    
    print(f"Found {len(papers)} papers to evaluate")
    
    # Evaluate papers
    for paper_id, paper in papers.items():
        print(f"\nEvaluating {paper_id}...")
        selection = evaluate_paper(paper)
        save_evaluation(paper_id, selection)
    
    # Clean up unselected papers
    cleanup_unselected()
    print("\nFinished processing papers")

if __name__ == "__main__":
    main() 