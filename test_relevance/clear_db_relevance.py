import sqlite3
from pathlib import Path

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent  # test_relevance directory
DB_PATH = SCRIPT_DIR / "database" / "arxiv_relevance.db"

def clear_database():
    """Clear all data from the database tables while preserving the schema"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Clear the summaries table
        cursor.execute("DELETE FROM summaries")
        
        # Clear the full_documents table
        cursor.execute("DELETE FROM full_documents")
        
        # Commit the changes
        conn.commit()
        print("Successfully cleared all data from the database.")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    clear_database() 