# options:
#   -h, --help            show this help message and exit
#   --start-from {clear,fetch,download,parse,extract,summarise}
#                         Start workflow from a specific step
#   --show-summaries      Run workflow and display summaries at the end

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('workflow.log')
    ]
)

class WorkflowManager:
    """Manages the AI News workflow pipeline"""
    
    REQUIRED_FILES = {
        'clear': 'scripts/clear_db.py',
        'fetch': 'scripts/fetch.py',
        'download': 'scripts/download.py',
        'parse': 'scripts/parse.py',
        'extract': 'scripts/extract_regex.py',
        'summarise': 'scripts/summarise.py'
    }
    
    def __init__(self):
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parent
        self.state: Dict[str, bool] = {}
        
        # Verify required files exist and are in scripts directory
        missing_files = self._verify_required_files()
        if missing_files:
            logging.error(f"Missing required files: {', '.join(missing_files)}")
            raise FileNotFoundError(f"Missing required files: {', '.join(missing_files)}")
    
    def _verify_required_files(self) -> List[str]:
        """Verify all required script files exist in scripts directory"""
        missing = []
        for script in self.REQUIRED_FILES.values():
            if not (self.project_root / script).exists():
                missing.append(script.split('/')[-1])  # Only show filename in error
            elif 'scripts' not in Path(script).parts:
                missing.append(f"{script.split('/')[-1]} (not in scripts directory)")
        return missing
    
    def _clear_directory(self, dir_path: Path, pattern: str = "*.json") -> None:
        """Clear all files matching pattern from directory"""
        if not dir_path.exists():
            logging.warning(f"Directory not found: {dir_path}")
            return
            
        count = 0
        for file in dir_path.glob(pattern):
            try:
                file.unlink()
                count += 1
            except Exception as e:
                logging.error(f"Error deleting {file}: {e}")
        
        logging.info(f"Cleared {count} files from {dir_path}")
    
    def clear_workspace(self) -> bool:
        """Clear database and workspace files"""
        try:
            # Clear database using venv python if available
            logging.info("Clearing database...")
            db_script = self.project_root / self.REQUIRED_FILES['clear']
            
            # Use the virtual environment's Python if available
            python_cmd = str(self.project_root / "venv" / "bin" / "python")
            if not Path(python_cmd).exists():
                python_cmd = "python"
                
            result = os.system(f"{python_cmd} {db_script}")
            if result != 0:
                logging.error("Failed to clear database")
                return False
            
            # Clear JSON files from conversions directory
            conversions_dir = self.project_root / "conversions"
            self._clear_directory(conversions_dir)
            
            # Clear PDF directory
            pdfs_dir = self.project_root / "pdfs"
            self._clear_directory(pdfs_dir, "*.pdf")
            
            # Clear JSON files from database directory
            database_dir = self.project_root / "database"
            self._clear_directory(database_dir, "*.json")
            
            logging.info("Workspace cleared successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error clearing workspace: {e}")
            return False
    
    def fetch(self) -> bool:
        """Fetch new papers from arXiv"""
        return self._run_script(self.REQUIRED_FILES['fetch'], "Fetching papers from arXiv")
    
    def download(self) -> bool:
        """Download PDF files"""
        return self._run_script(self.REQUIRED_FILES['download'], "Downloading PDFs")
    
    def parse(self) -> bool:
        """Parse PDF files"""
        return self._run_script(self.REQUIRED_FILES['parse'], "Parsing PDFs")
    
    def extract(self) -> bool:
        """Extract sections from parsed papers"""
        return self._run_script(self.REQUIRED_FILES['extract'], "Extracting sections")
    
    def summarise(self) -> bool:
        """Generate summaries"""
        return self._run_script(self.REQUIRED_FILES['summarise'], "Generating summaries")
    
    def show_summaries(self) -> bool:
        """Display all summaries with their paper titles"""
        try:
            logging.info("\nRetrieving summaries...")
            
            # Connect to database
            db_path = self.project_root / "database" / "arxiv_docs.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get summaries with titles
            cursor.execute("""
                SELECT f.id, f.title, s.summary
                FROM full_documents f
                JOIN summaries s ON f.id = s.id
                WHERE s.summary IS NOT NULL
                ORDER BY f.id
            """)
            
            results = cursor.fetchall()
            if not results:
                logging.info("No summaries found in database")
                return True
                
            # Display summaries
            logging.info("\n" + "="*100)
            logging.info(f"Found {len(results)} summaries:")
            logging.info("="*100 + "\n")
            
            for paper_id, title, summary in results:
                logging.info(f"Paper ID: {paper_id}")
                logging.info(f"Title: {title}")
                logging.info("-"*50)
                logging.info(f"Summary: {summary}")
                logging.info("="*100 + "\n")
                
            return True
            
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            return False
        except Exception as e:
            logging.error(f"Error showing summaries: {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()
    
    def run_full_workflow(self, start_from: Optional[str] = None) -> bool:
        """
        Run the complete workflow
        
        Args:
            start_from: Optional step to start from ('clear', 'fetch', 'download', 'parse', 'extract', 'summarise')
        """
        steps = [
            ('clear', self.clear_workspace),
            ('fetch', self.fetch),
            ('download', self.download),
            ('parse', self.parse),
            ('extract', self.extract),
            ('summarise', self.summarise)
        ]
        
        # Track start time
        start_time = datetime.now()
        logging.info(f"Starting workflow at {start_time}")
        
        # Find starting point
        if start_from:
            try:
                start_idx = next(i for i, (name, _) in enumerate(steps) if name == start_from)
                steps = steps[start_idx:]
            except StopIteration:
                logging.error(f"Invalid starting point: {start_from}")
                return False
        
        # Run each step
        step_times = {}
        for step_name, step_func in steps:
            step_start = datetime.now()
            self.state[step_name] = False
            logging.info(f"\n{'='*50}\nStarting step: {step_name}\n{'='*50}")
            if not step_func():
                logging.error(f"Workflow failed at step: {step_name}")
                return False
            self.state[step_name] = True
            step_times[step_name] = datetime.now() - step_start
            
        # Calculate duration and show summary
        total_duration = datetime.now() - start_time
        
        logging.info("\n" + "="*50)
        logging.info("Workflow Summary:")
        logging.info("="*50)
        for step_name, duration in step_times.items():
            logging.info(f"{step_name:10} : {duration}")
        logging.info("-"*50)
        logging.info(f"Total Time : {total_duration}")
        logging.info("="*50 + "\n")
        
        return True
    
    def get_status(self) -> Dict[str, bool]:
        """Get the status of each workflow step"""
        return self.state
    
    def _run_script(self, script_name: str, description: str) -> bool:
        """Run a Python script and handle its execution"""
        script_path = self.project_root / script_name
        if not script_path.exists():
            logging.error(f"Script not found: {script_name}")
            return False
            
        logging.info(f"Starting: {description}")
        try:
            # Use the virtual environment's Python if available
            python_cmd = str(self.project_root / "venv" / "bin" / "python")
            if not Path(python_cmd).exists():
                python_cmd = "python"
                
            result = os.system(f"{python_cmd} {script_path}")
            success = result == 0
            if success:
                logging.info(f"Completed: {description}")
            else:
                logging.error(f"Failed: {description} (exit code: {result})")
            return success
        except Exception as e:
            logging.error(f"Error in {description}: {e}")
            return False

def main():
    """Run the workflow"""
    import argparse
    
    parser = argparse.ArgumentParser(description='AI News Workflow Manager')
    parser.add_argument('--start-from', type=str, 
                      choices=['clear', 'fetch', 'download', 'parse', 'extract', 'summarise'],
                      help='Start workflow from a specific step')
    parser.add_argument('--show-summaries', action='store_true',
                      help='Run workflow and display summaries at the end')
    args = parser.parse_args()
    
    try:
        workflow = WorkflowManager()
        
        # Run the workflow
        success = workflow.run_full_workflow(start_from=args.start_from)
        if not success:
            logging.error("Workflow failed")
            exit(1)
        
        # Show summaries if requested
        if args.show_summaries:
            logging.info("\nDisplaying summaries:")
            if not workflow.show_summaries():
                logging.error("Failed to display summaries")
                exit(1)
        
        logging.info("Operation completed successfully")
            
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        exit(1)

if __name__ == "__main__":
    main() 