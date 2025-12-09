# app/utils/id_gen.py

import uuid

def generate_dataset_id() -> str:
    return str(uuid.uuid4())
