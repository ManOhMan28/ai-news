import sqlite3
from pathlib import Path

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH = PROJECT_ROOT / "database" / "arxiv_docs.db"

def clear_database():
    """Clear all data from database tables"""
    try:
        # First verify the database exists
        if not DB_PATH.exists():
            print(f"Database not found at: {DB_PATH}")
            return
            
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get table names to verify they exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Found tables: {tables}")
        
        # Clear the tables
        if 'full_documents' in tables:
            cursor.execute("DELETE FROM full_documents")
            print(f"Cleared full_documents table, rows affected: {cursor.rowcount}")
            
        if 'summaries' in tables:
            cursor.execute("DELETE FROM summaries")
            print(f"Cleared summaries table, rows affected: {cursor.rowcount}")
        
        # Commit the changes
        conn.commit()
        
        # Verify tables are empty
        for table in ['full_documents', 'summaries']:
            if table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"Rows remaining in {table}: {count}")
        
        print("Database tables cleared successfully")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    clear_database()