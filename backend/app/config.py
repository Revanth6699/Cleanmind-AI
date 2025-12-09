import os

# Base directory → backend/app/
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Directory to store uploaded datasets
BASE_UPLOAD_DIR = os.path.join(BASE_DIR, "..", "uploads")

# Create upload directory if not exists
os.makedirs(BASE_UPLOAD_DIR, exist_ok=True)

from datetime import timedelta

# Existing settings...
BASE_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "uploaded_datasets")
os.makedirs(BASE_UPLOAD_DIR, exist_ok=True)

# ===== Auth / JWT Settings =====
SECRET_KEY = "super-secret-key-change-this"  # change for production
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60  # 1 hour
