"""Configuration for PDF to DB application."""
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
PAGES_FOLDER = os.path.join(BASE_DIR, "uploads", "pages")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")
ALLOWED_EXTENSIONS = {"pdf"}
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500 MB max file size

# Ensure directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PAGES_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
