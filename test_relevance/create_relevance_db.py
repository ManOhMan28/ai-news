import sqlite3
from pathlib import Path

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent
DB_DIR = SCRIPT_DIR / "database"  # database directory inside test_relevance
DB_PATH = DB_DIR / "arxiv_relevance.db"

def create_database():
    """Create the arxiv_relevance database with necessary tables"""
    try:
        # Create database directory if it doesn't exist
        DB_DIR.mkdir(exist_ok=True)
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create full_documents table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS full_documents (
                id TEXT PRIMARY KEY,
                title TEXT,
                authors TEXT,
                affiliation TEXT,
                pdf_url TEXT,
                abstract TEXT,
                conclusion TEXT,
                selected TEXT
            )
        """)

        # Create summaries table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS summaries (
                id TEXT PRIMARY KEY,
                summary TEXT,
                FOREIGN KEY (id) REFERENCES full_documents (id)
            )
        """)

        conn.commit()
        print(f"✓ Created database at {DB_PATH}")
        print("✓ Created tables: full_documents, summaries")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_database() 