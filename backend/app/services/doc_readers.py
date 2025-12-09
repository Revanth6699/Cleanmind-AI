# app/services/doc_readers.py
import pandas as pd
import pdfplumber
from docx import Document
from bs4 import BeautifulSoup
from PIL import Image
import pytesseract

from .ingestion_base import register_reader



@register_reader(["pdf"])
def read_pdf(path: str) -> pd.DataFrame:
    # Very simple: extract text per page as rows
    rows = []
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            rows.append({"page": i + 1, "text": text})
    return pd.DataFrame(rows)


@register_reader(["docx"])
def read_docx(path: str) -> pd.DataFrame:
    doc = Document(path)
    paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
    return pd.DataFrame({"paragraph": paragraphs})


@register_reader(["png", "jpg", "jpeg"])
def read_image_ocr(path: str) -> pd.DataFrame:
    # OCR the entire image; later you can do table detection etc.
    img = Image.open(path)
    text = pytesseract.image_to_string(img)
    return pd.DataFrame({"text": [text]})


