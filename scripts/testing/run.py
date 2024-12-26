import subprocess
import shutil
import time
from pathlib import Path
from datetime import datetime

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_DIR = PROJECT_ROOT / "database"
CONV_DIR = PROJECT_ROOT / "conversions"
PDF_DIR = PROJECT_ROOT / "pdfs"

def clean_directory(directory: Path):
    """Remove all files in directory but keep the directory"""
    if directory.exists():
        for item in directory.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        print(f"✓ Cleaned {directory}")

def clean_all():
    """Clean all generated content"""
    try:
        # Clean directories
        clean_directory(DB_DIR)
        clean_directory(CONV_DIR)
        clean_directory(PDF_DIR)
        print("✓ All directories cleaned")
        
        # Create database
        run_script("testing/create_db.py")
        
    except Exception as e:
        print(f"⚠ Error during cleanup: {e}")
        return False
    return True

def run_script(script_name):
    """Run a script and return True if successful"""
    script_path = SCRIPT_DIR / script_name
    try:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running {script_name}...")
        result = subprocess.run(["python", str(script_path)], check=True)
        print(f"✓ {script_name} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠ Error running {script_name}: {e}")
        return False

def run_pipeline():
    """Run the full paper processing pipeline"""
    pipeline = [
        ("fetch.py", True),                  # 1. Fetch papers metadata
        ("download.py", True),               # 2. Download PDFs
        ("parse.py", True),                  # 3. Parse PDFs to JSON
        ("extract_regex.py", True),          # 4. Extract sections
        ("summarise.py", True),              # 5. Generate summaries
    ]
    
    for script, required in pipeline:
        success = run_script(script)
        if not success and required:
            print(f"\n⚠ Pipeline stopped due to failure in {script}")
            return False
        
        # Add delay after extract_regex
        if script == "extract_regex.py" and success:
            print("Waiting 10 seconds before summarization...")
            time.sleep(10)
        
        # Generate summaries.json right after summarise.py
        if script == "summarise.py" and success:
            from testing.fulldocs_dump import dump_summaries
            try:
                dump_summaries(quiet=True)
                print("✓ Generated summaries.json")
            except Exception as e:
                print(f"⚠ Error generating summaries.json: {e}")
                if required:
                    return False
    
    # Generate full documents dump at the end
    try:
        from testing.fulldocs_dump import dump_full_documents
        dump_full_documents(quiet=True)
        print("✓ Generated full_documents.json")
    except Exception as e:
        print(f"⚠ Error generating full_documents.json: {e}")
            
    print("\n✓ Pipeline completed successfully")
    return True

def main():
    """Main function with error handling"""
    try:
        print("\nStarting fresh run...")
        if clean_all():
            run_pipeline()
    except Exception as e:
        print(f"\n⚠ Pipeline error: {e}")
    
if __name__ == "__main__":
    main() 