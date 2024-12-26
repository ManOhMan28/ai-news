import logging
import json
import asyncio
import time
from pathlib import Path
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor
from docling.document_converter import (
    DocumentConverter,
    PdfFormatOption,
)
from docling.datamodel.base_models import InputFormat
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend

_log = logging.getLogger(__name__)

def create_converter():
    """Create a configured DocumentConverter instance with minimal processing"""
    # Get default options and modify them
    options = StandardPdfPipeline.get_default_options()
    
    # Create pipeline instance with minimal processing
    pdf_pipeline = StandardPdfPipeline(pipeline_options=options)
    # Disable all models to minimize processing
    pdf_pipeline.build_pipe = []
    pdf_pipeline.enrichment_pipe = []
    
    return DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_cls=StandardPdfPipeline,  # Pass the class directly
                backend=PyPdfiumDocumentBackend,
                pipeline_options=options
            ),
        },
    )

async def convert_pdf(converter, pdf_path, out_path, instance_id):
    """Convert a single PDF using the specified converter instance"""
    try:
        print(f"Instance {instance_id} starting: {pdf_path.name}")
        
        # Check if output already exists
        json_path = out_path / f"{pdf_path.stem}.json"
        if json_path.exists():
            print(f"Instance {instance_id} skipping {pdf_path.name} - output already exists")
            return True
        
        # Run the conversion in a thread pool since docling's convert is blocking
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = await loop.run_in_executor(
                pool,
                lambda: converter.convert(pdf_path)
            )
        
        # Export to JSON
        with json_path.open("w") as fp:
            json.dump(result.document.export_to_dict(), fp, indent=2)
            
        print(f"✓ Instance {instance_id} completed: {pdf_path.name}")
        return True
    except Exception as e:
        print(f"Error in instance {instance_id} processing {pdf_path.name}: {str(e)}")
        _log.error(f"Detailed error for {pdf_path.name}: {e}", exc_info=True)
        return False

async def convert_pdfs_parallel():
    """Convert PDF files to JSON format using parallel instances"""
    start_time = time.time()
    
    # Setup paths relative to script location
    project_root = Path(__file__).parent.parent
    pdfs_dir = project_root / "pdfs"
    out_path = project_root / "conversions"
    out_path.mkdir(exist_ok=True)

    # Get all PDF files from the pdfs directory
    input_paths = list(pdfs_dir.glob("*.pdf"))
    
    if not input_paths:
        print("No PDF files found in pdfs directory")
        return

    print(f"\nFound {len(input_paths)} PDF files to process")
    
    # Create two converter instances with unique IDs
    converters = [
        (f"instance{i+1}", create_converter())
        for i in range(2)
    ]
    
    print(f"Created {len(converters)} parallel converter instances")

    # Create tasks for parallel processing
    tasks = []
    for pdf_path, (instance_id, converter) in zip(input_paths, cycle(converters)):
        task = convert_pdf(converter, pdf_path, out_path, instance_id)
        tasks.append(task)
    
    try:
        # Run conversions in parallel with timeout
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successes and failures
        successful = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is False or isinstance(r, Exception))
        skipped = len(results) - successful - failed
        
        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        
        print(f"\nConversion Summary:")
        print(f"- Total files: {len(input_paths)}")
        print(f"- Successfully converted: {successful}")
        print(f"- Failed: {failed}")
        print(f"- Skipped (already exists): {skipped}")
        print(f"- Total time: {elapsed_time:.2f} seconds")
        print(f"- Average time per file: {elapsed_time/len(input_paths):.2f} seconds")
        print(f"- Output directory: {out_path}")
        
    except Exception as e:
        print(f"Error during parallel processing: {str(e)}")
        _log.error("Detailed error during parallel processing", exc_info=True)
        raise

async def main():
    try:
        await convert_pdfs_parallel()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Set up asyncio event loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        raise 