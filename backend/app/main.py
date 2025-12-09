# backend/app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import datasets

app = FastAPI(
    title="CleanMind AI Backend",
    description="Simple data upload / profiling / cleaning backend (no auth).",
    version="0.2.0",
)

# CORS – allow your local frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # or restrict later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "backend is running 🚀", "service": "CleanMind AI"}


# Only datasets routes (no auth router)
app.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
