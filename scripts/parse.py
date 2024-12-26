import logging
import json
from pathlib import Path
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.document_converter import (
    DocumentConverter,
    PdfFormatOption,
)
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline

_log = logging.getLogger(__name__)

def convert_pdfs():
    """Convert PDF files to JSON format"""
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

    # Configure converter with PDF-specific options
    doc_converter = DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_cls=StandardPdfPipeline,
                backend=PyPdfiumDocumentBackend
            ),
        },
    )

    conv_results = doc_converter.convert_all(input_paths)

    for res in conv_results:
        print(
            f"Document {res.input.file.name} converted.\n"
            f"Saved JSON output to: {str(out_path)}"
        )
        
        # Export to JSON using export_to_dict() and json.dump()
        with (out_path / f"{res.input.file.stem}.json").open("w") as fp:
            json.dump(res.document.export_to_dict(), fp, indent=2)

def main():
    convert_pdfs()

if __name__ == "__main__":
    main()
    