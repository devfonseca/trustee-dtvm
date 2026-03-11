from pathlib import Path
from datetime import date

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "fiduciario.db"

BASE_URL = "https://fiduciario.com.br/wp-admin/admin-ajax.php"

TIMEOUT = 30

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://fiduciario.com.br/diarios/",
}

DEFAULT_BACKFILL_START = "1999-01-01"
TODAY = date.today().isoformat()
RECENT_DAYS_WINDOW = 7