
import os
from pathlib import Path
from dotenv import load_dotenv


def _load_env_file() -> None:
    """Load .env from the project directory when available (kept from the
    original scripts so behaviour doesn't change for existing deployments)."""
    for env_path in (Path(__file__).resolve().parent / ".env", Path.cwd() / ".env"):
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


_load_env_file()
load_dotenv()  # fallback / standard method

OW_SEARCH_BASE_URL = os.environ.get("OW_SEARCH_BASE_URL", "https://api.useopenwrench.com/api/v1/supplier")
OW_AUTH_TOKEN = os.environ.get("OW_AUTH_TOKEN", "")

OW_LIFECYCLE_BASE_URL = os.environ.get(
    "OW_LIFECYCLE_BASE_URL", "https://api.useopenwrench.com/api/external/v1/supplier"
)
OW_API_KEY = os.environ.get("OW_API_KEY", "")
OW_OW_KEY = os.environ.get("OW_OW_KEY", "")
OW_USER_EMAIL = os.environ.get("OW_USER_EMAIL", "customerservice@n2gsupps.com")

OW_TIMEOUT = int(os.environ.get("OW_TIMEOUT", "60"))

# Database 
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg2://postgres:admin@localhost:5432/zendesk_openwrench"
)

#Zendesk
ZD_RAW_URL = os.environ.get("ZD_BASE_URL", "n2gsupport")
OW_TAG = os.environ.get("OW_TAG", "openwrench")
ZD_EMAIL = os.environ.get("ZD_EMAIL", "")
ZD_API_TOKEN = os.environ.get("ZD_API_TOKEN", "")
ZD_OW_FIELD_ID = os.environ.get("ZD_OW_FIELD_ID", None)
ZD_TIMEOUT = int(os.environ.get("ZD_TIMEOUT", "30"))
ZD_TICKET_FORM_ID = os.environ.get("ZD_TICKET_FORM_ID", None)
ZD_GROUP_ID = os.environ.get("ZD_GROUP_ID", None)
ZD_BRAND_ID = os.environ.get("ZD_BRAND_ID", None)
ZD_ORG_FIELD_ID = os.environ.get("ZD_ORG_FIELD_ID", "47073616382868")

if not ZD_RAW_URL.startswith("http://") and not ZD_RAW_URL.startswith("https://"):
    if ".zendesk.com" in ZD_RAW_URL:
        ZD_BASE_URL = f"https://{ZD_RAW_URL}"
    else:
        ZD_BASE_URL = f"https://{ZD_RAW_URL}.zendesk.com/api/v2"
else:
    ZD_BASE_URL = ZD_RAW_URL
ZD_BASE_URL = ZD_BASE_URL.rstrip("/")

#Sync engine (hourly background job) 
SYNC_JOB_NAME = "openwrench_work_order_sync"
SYNC_INTERVAL_HOURS = int(os.environ.get("SYNC_INTERVAL_HOURS", "1"))
INITIAL_LOOKBACK_HOURS = int(os.environ.get("INITIAL_LOOKBACK_HOURS", "24"))
SYNC_OVERLAP_MINUTES = int(os.environ.get("SYNC_OVERLAP_MINUTES", "5"))

# Webhook auth 
WEBHOOK_SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
NOTES_SYNC_INTERVAL_MINUTES = int(os.environ.get("NOTES_SYNC_INTERVAL_MINUTES", "15"))
