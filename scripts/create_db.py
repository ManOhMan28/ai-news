import sqlite3
from pathlib import Path

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent  # testing directory
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # go up two levels
DB_PATH = PROJECT_ROOT / "database" / "arxiv_docs.db"

def create_database():
    """Create the database and its tables"""
    DB_PATH.parent.mkdir(exist_ok=True)  # Ensure database directory exists
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Create table for full documents
    c.execute('''CREATE TABLE IF NOT EXISTS full_documents (
                 id TEXT PRIMARY KEY,
                 title TEXT,
                 authors TEXT,
                 pdf_url TEXT,
                 abstract TEXT,
                 conclusion TEXT)''')

    # Create table for summaries
    c.execute('''CREATE TABLE IF NOT EXISTS summaries (
                 id TEXT PRIMARY KEY,
                 summary TEXT)''')

    conn.commit()
    conn.close()
    print(f"Database created at {DB_PATH}")

if __name__ == "__main__":
    create_database()
