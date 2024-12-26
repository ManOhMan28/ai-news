import sqlite3
import json
import re
from pathlib import Path

# Define paths relative to script location
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_PATH = PROJECT_ROOT / "database" / "arxiv_docs.db"
CONV_DIR = PROJECT_ROOT / "conversions"
SECTIONS_JSON_PATH = CONV_DIR / "sections_regex.json"

def extract_sections(json_path):
    """Extract sections using regex patterns from JSON content"""
    try:
        with open(json_path) as f:
            doc = json.load(f)
            
        # Reconstruct text content from the JSON structure
        content = ""
        if 'texts' in doc:
            for text_element in doc['texts']:
                if 'text' in text_element:
                    content += text_element['text'].strip() + "\n\n"
        elif isinstance(doc, str):
            # Handle case where the JSON contains direct text
            content = doc
        elif isinstance(doc, dict) and 'text' in doc:
            # Handle case where text is in root level
            content = doc['text']
            
        if not content.strip():
            print(f"Warning: No content found in {json_path}")
            return {'abstract': None, 'conclusion': None}
            
        print(f"\nDocument content preview ({len(content)} chars):")
        preview = "\n".join(content.split("\n")[:5])
        print(preview + "...")
        
        sections = {
            'abstract': None,
            'conclusion': None
        }
        
        # Extract Abstract patterns (expanded)
        abstract_patterns = [
            r'Abstract\s*\n(.*?)(?=\n\s*\d|\n\s*[A-Z][a-z]|\Z)',
            r'ABSTRACT\s*\n(.*?)(?=\n\s*\d|\n\s*[A-Z][a-z]|\Z)',
            r'Abstract\s*[-–—]\s*(.*?)(?=\n\s*\d|\n\s*[A-Z][a-z]|\Z)',
            r'ABSTRACT\s*[-–—]\s*(.*?)(?=\n\s*\d|\n\s*[A-Z][a-z]|\Z)',
            r'Abstract[:\.]?\s*(.*?)(?=\n\s*(?:\d+\.?\s*[A-Z]|Introduction|INTRODUCTION)|\Z)',
            r'ABSTRACT[:\.]?\s*(.*?)(?=\n\s*(?:\d+\.?\s*[A-Z]|Introduction|INTRODUCTION)|\Z)'
        ]
        
        # Extract Conclusion patterns (expanded)
        conclusion_patterns = [
            r'(?:\d+\.?\s*)?Conclusion[s]?\s*\n(.*?)(?=\n\s*(?:Acknowledgement|ACKNOWLEDGEMENT|Reference|REFERENCE|\d+\.?\s*[A-Z])|\Z)',
            r'(?:\d+\.?\s*)?CONCLUSION[S]?\s*\n(.*?)(?=\n\s*(?:Acknowledgement|ACKNOWLEDGEMENT|Reference|REFERENCE|\d+\.?\s*[A-Z])|\Z)',
            r'Conclusion and Limitations\s*\n(.*?)(?=\n\s*(?:Acknowledgement|ACKNOWLEDGEMENT|Reference|REFERENCE|\d+\.?\s*[A-Z])|\Z)',
            r'(?:\d+\.?\s*)?Conclusion[s]?[:\.]?\s*(.*?)(?=\n\s*(?:Acknowledgement|Reference|\d+\.?\s*[A-Z])|\Z)',
            r'(?:\d+\.?\s*)?CONCLUSION[S]?[:\.]?\s*(.*?)(?=\n\s*(?:Acknowledgement|Reference|\d+\.?\s*[A-Z])|\Z)',
            r'Concluding Remarks\s*\n(.*?)(?=\n\s*(?:Acknowledgement|Reference|\d+\.?\s*[A-Z])|\Z)'
        ]

        # Extract Discussion patterns (expanded)
        discussion_patterns = [
            r'(?:\d+\.?\s*)?Discussion\s*\n(.*?)(?=\n\s*(?:Conclusion|CONCLUSION|Acknowledgement|ACKNOWLEDGEMENT|Reference|REFERENCE|\d+\.?\s*[A-Z])|\Z)',
            r'(?:\d+\.?\s*)?DISCUSSION\s*\n(.*?)(?=\n\s*(?:Conclusion|CONCLUSION|Acknowledgement|ACKNOWLEDGEMENT|Reference|REFERENCE|\d+\.?\s*[A-Z])|\Z)',
            r'Discussion and Future Work\s*\n(.*?)(?=\n\s*(?:Conclusion|CONCLUSION|Acknowledgement|ACKNOWLEDGEMENT|Reference|REFERENCE|\d+\.?\s*[A-Z])|\Z)',
            r'(?:\d+\.?\s*)?Discussion[:\.]?\s*(.*?)(?=\n\s*(?:Conclusion|Acknowledgement|Reference|\d+\.?\s*[A-Z])|\Z)',
            r'Discussion and Analysis\s*\n(.*?)(?=\n\s*(?:Conclusion|Acknowledgement|Reference|\d+\.?\s*[A-Z])|\Z)'
        ]

        # Find abstract
        for pattern in abstract_patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                abstract_text = match.group(1).strip()
                abstract_text = re.sub(r'\s+', ' ', abstract_text)
                if len(abstract_text) > 50:  # Minimum length check
                    sections['abstract'] = abstract_text
                    print(f"Found abstract ({len(abstract_text)} chars)")
                    break

        # Find conclusion or discussion
        conclusion_found = False
        
        # First try to find conclusion
        for pattern in conclusion_patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                conclusion_text = match.group(1).strip()
                conclusion_text = re.sub(r'\s+', ' ', conclusion_text)
                conclusion_text = re.split(r'\n\s*References?|\n\s*REFERENCES?', conclusion_text)[0]
                if len(conclusion_text) > 50:  # Minimum length check
                    sections['conclusion'] = conclusion_text
                    print(f"Found conclusion ({len(conclusion_text)} chars)")
                    conclusion_found = True
                    break
        
        # If no conclusion found, look for discussion
        if not conclusion_found:
            for pattern in discussion_patterns:
                match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
                if match:
                    discussion_text = match.group(1).strip()
                    discussion_text = re.sub(r'\s+', ' ', discussion_text)
                    discussion_text = re.split(r'\n\s*References?|\n\s*REFERENCES?', discussion_text)[0]
                    if len(discussion_text) > 50:  # Minimum length check
                        sections['conclusion'] = discussion_text
                        print(f"Found discussion as conclusion ({len(discussion_text)} chars)")
                        break
        
        # Final validation
        if not sections['abstract'] and not sections['conclusion']:
            print("Warning: No sections found in document")
        elif not sections['abstract']:
            print("Warning: No abstract found")
        elif not sections['conclusion']:
            print("Warning: No conclusion found")
            
        return sections
    except Exception as e:
        print(f"Error processing {json_path}: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(traceback.format_exc())
        return {'abstract': None, 'conclusion': None}

def update_database_with_sections(id, sections):
    """Update the database with extracted sections and verify the update"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # First check current values
        c.execute("SELECT abstract, conclusion FROM full_documents WHERE id = ?", (id,))
        current = c.fetchone()
        if current:
            print(f"\nCurrent values for {id}:")
            print(f"Abstract: {current[0][:100] + '...' if current[0] else 'None'}")
            print(f"Conclusion: {current[1][:100] + '...' if current[1] else 'None'}")
        
            # Only update if we have new content
            update_abstract = sections['abstract'] is not None
            update_conclusion = sections['conclusion'] is not None
            
            if update_abstract or update_conclusion:
                # Construct dynamic UPDATE query
                update_fields = []
                params = []
                if update_abstract:
                    update_fields.append("abstract = ?")
                    params.append(sections['abstract'])
                if update_conclusion:
                    update_fields.append("conclusion = ?")
                    params.append(sections['conclusion'])
                
                query = f"""
                    UPDATE full_documents 
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                """
                params.append(id)
                
                c.execute(query, params)
                conn.commit()
                
                # Verify the update
                c.execute("SELECT abstract, conclusion FROM full_documents WHERE id = ?", (id,))
                updated = c.fetchone()
                print(f"\nUpdated values for {id}:")
                print(f"Abstract: {updated[0][:100] + '...' if updated[0] else 'None'}")
                print(f"Conclusion: {updated[1][:100] + '...' if updated[1] else 'None'}")
                
                if ((not update_abstract or updated[0] == sections['abstract']) and 
                    (not update_conclusion or updated[1] == sections['conclusion'])):
                    print(f"✓ Successfully updated database for ID: {id}")
                    return True
                else:
                    print(f"⚠ Warning: Database update for {id} may not have been successful")
                    return False
            else:
                print(f"No new content to update for {id}")
                return False
        else:
            print(f"Error: Document {id} not found in database")
            return False
            
    except sqlite3.Error as e:
        print(f"Database error for {id}: {e}")
        return False
    finally:
        conn.close()

def main():
    """Main function to process documents and update database"""
    try:
        # First, check what documents we have JSON files for
        json_files = list(CONV_DIR.glob("*.json"))
        print(f"Found {len(json_files)} JSON files in {CONV_DIR}")
        
        # Load existing sections if any
        existing_sections = {}
        if SECTIONS_JSON_PATH.exists():
            with open(SECTIONS_JSON_PATH) as f:
                existing_sections = json.load(f)
            print(f"Loaded {len(existing_sections)} existing sections from {SECTIONS_JSON_PATH}")
        
        # Connect to database and get all document IDs
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT id FROM full_documents')
        documents = c.fetchall()
        conn.close()

        if not documents:
            print("No documents found in database")
            return

        print(f"Found {len(documents)} documents in database")
        
        extracted_sections = existing_sections.copy()
        successful_updates = 0
        skipped = 0
        failed = 0
        
        for id, in documents:
            json_path = CONV_DIR / f"{id}.json"
            if json_path.exists():
                if id in existing_sections:
                    print(f"\nSkipping {id} - already processed")
                    skipped += 1
                    continue
                    
                print(f"\nProcessing: {json_path}")
                try:
                    sections = extract_sections(json_path)
                    if sections['abstract'] or sections['conclusion']:
                        if update_database_with_sections(id, sections):
                            extracted_sections[id] = sections
                            successful_updates += 1
                        else:
                            failed += 1
                    else:
                        print(f"No sections found in {id}")
                        failed += 1
                except Exception as e:
                    print(f"Error processing {id}: {e}")
                    failed += 1
            else:
                print(f"JSON file not found: {id}.json")
                failed += 1

        print(f"\nOperation Summary:")
        print(f"- Total documents in database: {len(documents)}")
        print(f"- JSON files found: {len(json_files)}")
        print(f"- Previously processed: {len(existing_sections)}")
        print(f"- Successfully processed: {successful_updates}")
        print(f"- Skipped (already processed): {skipped}")
        print(f"- Failed: {failed}")
        
        # Save all sections, including previously existing ones
        with open(SECTIONS_JSON_PATH, 'w') as f:
            json.dump(extracted_sections, f, indent=2)
        print(f"✓ Saved {len(extracted_sections)} sections to {SECTIONS_JSON_PATH}")
        
    except Exception as e:
        print(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main() 