import sqlite3
import json
from pathlib import Path
from datetime import datetime

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent  # testing directory
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # go up two levels to reach project root
DB_PATH = PROJECT_ROOT / "database" / "arxiv_docs.db"
DB_DIR = DB_PATH.parent  # Get database directory

def dump_table_to_json(table_name, output_file, quiet=False):
    """
    Dump a database table to a JSON file
    Args:
        table_name (str): Name of the table to dump
        output_file (Path): Path to output JSON file
        quiet (bool): If True, suppress print statements
    """
    try:
        # Connect to database
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Get all records from table
        c.execute(f'SELECT * FROM {table_name}')
        rows = c.fetchall()
        
        if not rows:
            if not quiet:
                print(f"No records found in {table_name}")
            return
        
        # Convert rows to dictionary
        documents = {}
        for row in rows:
            doc_dict = dict(row)
            doc_id = doc_dict.pop('id')
            documents[doc_id] = doc_dict
        
        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
            
        if not quiet:
            print(f"\n{table_name} dump summary:")
            print(f"- Total records: {len(documents)}")
            print(f"- Output file: {output_file}")
            
            # Print sample of first document
            if documents:
                first_id = next(iter(documents))
                print(f"\nSample record ({first_id}):")
                first_doc = documents[first_id]
                for key, value in first_doc.items():
                    if value:
                        preview = str(value)[:100] + "..." if len(str(value)) > 100 else value
                        print(f"  {key}: {preview}")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def dump_full_documents(quiet=False):
    """Dump the full_documents table to JSON"""
    dump_file = DB_DIR / "full_documents.json"
    dump_table_to_json('full_documents', dump_file, quiet)

def dump_summaries(quiet=False):
    """Dump the summaries table to JSON"""
    dump_file = DB_DIR / "summaries.json"
    dump_table_to_json('summaries', dump_file, quiet)

def refresh_dump():
    """Hook function to be called after fetch and extract_regex"""
    try:
        dump_full_documents(quiet=True)
        dump_summaries(quiet=True)
        print("✓ Database JSON dumps refreshed")
    except Exception as e:
        print(f"⚠ Error refreshing database dumps: {e}")

def main():
    """Main function when run directly"""
    try:
        print("Starting database dumps...")
        dump_full_documents()
        dump_summaries()
        print("\n✓ Database dumps completed successfully")
    except Exception as e:
        print(f"\n⚠ Error during database dumps: {e}")
        raise

if __name__ == "__main__":
    main() 