import os
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DATA_DIR = os.getenv("DATA_DIR", "data/")
MODEL = "claude-sonnet-4-6"
