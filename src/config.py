import os
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DATA_DIR = os.getenv("DATA_DIR", "data/")
MODEL = "claude-sonnet-4-6"
DB_PATH = os.getenv("DB_PATH", "data/warehouse.duckdb")
MAX_ROWS = int(os.getenv("MAX_ROWS", "10000"))
