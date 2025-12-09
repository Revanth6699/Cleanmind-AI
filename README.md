CleanMind-AI
AI-Powered Automated Data Cleaning & Preprocessing Tool

CleanMind-AI is an intelligent data-cleaning assistant that automatically handles missing values, duplicates, outliers, encoding, normalization, and dataset validation.
It is built for data scientists, ML engineers, and analysts who want clean, ready-to-use datasets within seconds.

Features

Automatic Missing Value Handling

Duplicate Row Removal

Outlier Detection & Treatment

Categorical Encoding (Label, One-Hot)

Numeric Scaling & Normalization

File Support: CSV, Excel, JSON, Parquet

Downloadable Cleaned Dataset

Explainability Log (what was cleaned & why)

Tech Stack

Python

Pandas, NumPy

Scikit-learn

Matplotlib (optional for EDA)

FastAPI / Streamlit UI (optional)

How It Works

Upload any dataset

CleanMind-AI analyzes missingness, duplicates, and patterns

AI suggests the best cleaning strategy

Preprocessing pipeline runs automatically

Download cleaned dataset + log report

Example Usage
from cleaner import CleanMindAI

cleaner = CleanMindAI("data/input.csv")
df_cleaned, log = cleaner.clean()

print(df_cleaned.head())
print(log)

🛠 Installation
git clone https://github.com/<your-username>/CleanMind-AI.git
cd CleanMind-AI
pip install -r requirements.txt

Run Application
Streamlit UI
streamlit run app.py

FastAPI
uvicorn app:app --reload

Supported File Types

.csv

.xlsx

.xls

.json

.parquet

Roadmap

 Add advanced outlier detection (Isolation Forest)

 Add AutoML preprocessing mode

 Add integration with cloud storage

 Build a drag-and-drop UI

 Add correlation heatmaps & missingness visualization

Contributing

Pull requests are welcome! For major changes, open an issue to discuss your ideas first.

License

MIT License

Support the Project

If this project helped you, give it a star on GitHub!
