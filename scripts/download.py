import requests
import sqlite3
import os

# Define the database path
DB_PATH = os.path.join(os.path.dirname(__file__), '../database/arxiv_docs.db')

# Define the path to store downloaded PDFs
PDF_DIR = os.path.join(os.path.dirname(__file__), '../pdfs/')

def download_pdfs():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, pdf_url FROM full_documents WHERE pdf_url IS NOT NULL')
    documents = c.fetchall()
    conn.close()

    for id, pdf_path in documents:
        response = requests.get(pdf_path)
        if response.status_code == 200:
            pdf_path = os.path.join(PDF_DIR, f'{id}.pdf')
            with open(pdf_path, 'wb') as file:
                file.write(response.content)

def download_job():
    download_pdfs()

if __name__ == "__main__":
    download_job()
