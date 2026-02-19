import os
from dotenv import load_dotenv

# Load .env from project root
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

DB_CONFIG = {
    "url": os.getenv("SUPABASE_URL"),
    "key": os.getenv("SUPABASE_KEY"),         # <-- fixed
    "database_url": os.getenv("DATABASE_URL") # <-- fixed
}

# Validation
if not DB_CONFIG["url"]:
    raise ValueError("SUPABASE_URL not set in .env")
if not DB_CONFIG["key"]:
    raise ValueError("SUPABASE_KEY not set in .env")
if not DB_CONFIG["database_url"]:
    raise ValueError("DATABASE_URL not set in .env")
