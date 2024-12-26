import json
import sqlite3
from pathlib import Path
from ollama import Client

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_DIR = PROJECT_ROOT / "database"
DB_PATH = DB_DIR / "arxiv_docs.db"

def load_config():
    """Load configuration from config/config.json"""
    config_path = PROJECT_ROOT / "config" / "config.json"
    with open(config_path) as f:
        config = json.load(f)
    return config["models"]["summarise"]

def load_documents_from_db():
    """Load documents from the SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all documents, even those with missing sections
        cursor.execute("""
            SELECT id, abstract, conclusion 
            FROM full_documents
        """)
        
        documents = {}
        for row in cursor.fetchall():
            doc_id, abstract, conclusion = row
            documents[doc_id] = {
                'abstract': abstract if abstract else '',
                'conclusion': conclusion if conclusion else ''
            }
        
        print(f"\nLoaded {len(documents)} documents from database")
        return documents
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return {}
    finally:
        if 'conn' in locals():
            conn.close()

def summarise_paper(abstract, conclusion):
    """Generate a summary using the configured model"""
    config = load_config()
    
    # Initialize Ollama client
    client = Client(host='http://localhost:11434')
    
    # Determine which sections are missing
    missing_sections = []
    if not abstract or abstract.strip() == '':
        missing_sections.append('abstract')
    if not conclusion or conclusion.strip() == '':
        missing_sections.append('conclusion')
    
    # If both sections are missing, we can't generate a summary
    if len(missing_sections) == 2:
        print("Both abstract and conclusion are missing")
        return None
        
    # Build the prompt based on available sections
    prompt = "Here is "
    if abstract and abstract.strip():
        prompt += "the abstract"
        if conclusion and conclusion.strip():
            prompt += " and conclusion"
    else:
        prompt += "the conclusion"
    prompt += " from a research paper:\n\n"
    
    if abstract and abstract.strip():
        prompt += f"Abstract:\n{abstract}\n\n"
    if conclusion and conclusion.strip():
        prompt += f"Conclusion:\n{conclusion}\n\n"
    
    prompt += "Please provide a clear and concise summary of the key findings and implications."

    try:
        response = client.generate(
            model=config["model"],
            prompt=prompt,
            system=config["system_prompt"],
            stream=False
        )
        
        if not response or 'response' not in response:
            print("Error: No response from model")
            return None
            
        summary = response['response'].strip()
        if not summary:
            print("Error: Empty summary")
            return None
            
        # Add missing section flags at the start if needed
        if missing_sections:
            flags = [f"(missing {section})" for section in missing_sections]
            summary = f"{' '.join(flags)} {summary}"
            
        return summary
        
    except Exception as e:
        print(f"Error generating summary: {str(e)}")
        return None

def save_summary_to_db(paper_id, summary):
    """Save summary to the database"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO summaries (id, summary)
            VALUES (?, ?)
        """, (paper_id, summary))
        
        conn.commit()
        print(f"Saved summary for paper {paper_id}")
        
    except sqlite3.Error as e:
        print(f"Database error for {paper_id}: {e}")
        
    finally:
        if conn:
            conn.close()

def main():
    """Process papers and generate summaries"""
    config = load_config()
    print(f"\nUsing model: {config['model']}")
    
    # Load documents from database
    documents = load_documents_from_db()
    
    if not documents:
        print("No documents found to summarize")
        return
    
    print(f"Found {len(documents)} papers to process")
    
    # Generate and save summaries
    for paper_id, paper in documents.items():
        print(f"\nProcessing {paper_id}...")
        
        abstract = paper.get('abstract', '').strip()
        conclusion = paper.get('conclusion', '').strip()
        
        # Try to generate summary even if one section is missing
        if abstract or conclusion:  # At least one section must be present
            summary = summarise_paper(abstract, conclusion)
            if summary:
                save_summary_to_db(paper_id, summary)
        else:
            print(f"Skipping {paper_id} - no content available")
    
    print("\nFinished processing all papers")

if __name__ == "__main__":
    main()
