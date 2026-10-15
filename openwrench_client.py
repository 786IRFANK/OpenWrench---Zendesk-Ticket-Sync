import os
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any

import requests
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Column, BigInteger, DateTime, Integer, String, Text
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor

# ── Logger Setup ──────────────────────────────────────────────────────────────
logger = logging.getLogger("ow_client")
logging.basicConfig(level=logging.INFO)

# ── Environment Configuration ──────────────────────────────────────────────────
def _load_env_file() -> None:
    """Load .env from the project directory when available."""
    env_candidates = [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
        Path.cwd() / ".env",
    ]

    for env_path in env_candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())

_load_env_file()
load_dotenv()  # Fallback standard method

# OpenWrench Configuration
OW_BASE_URL   = os.environ.get("OW_BASE_URL", "https://api.useopenwrench.com/api/v1/supplier")
OW_AUTH_TOKEN = os.environ.get("OW_AUTH_TOKEN", "")          # x-auth-token header value
OW_TIMEOUT    = int(os.environ.get("OW_TIMEOUT", "30"))        # seconds

# Database Configuration
DATABASE_URL  = os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:admin@localhost:5432/zendesk_openwrench")

# Zendesk Configuration & Dynamic Fix
ZD_RAW_URL     = os.environ.get("ZD_BASE_URL", "n2gsupport")
OW_TAG         = os.environ.get("OW_TAG", "openwrench")
ZD_EMAIL       = os.environ.get("ZD_EMAIL", "")
ZD_API_TOKEN   = os.environ.get("ZD_API_TOKEN", "")
ZD_OW_FIELD_ID = os.environ.get("ZD_OW_FIELD_ID", None)
ZD_TIMEOUT     = int(os.environ.get("ZD_TIMEOUT", "30"))

# Explicit ticket placement so Zendesk doesn't fall back to the default form
# and doesn't let Answer Bot/AI auto-guess topic, intent, or language tags.
ZD_TICKET_FORM_ID = os.environ.get("ZD_TICKET_FORM_ID", None)   # e.g. "43607017737876"
ZD_GROUP_ID        = os.environ.get("ZD_GROUP_ID", None)         # e.g. "46219446958356"
ZD_BRAND_ID         = os.environ.get("ZD_BRAND_ID", None)

# Sync Engine Configuration
SYNC_JOB_NAME           = "openwrench_work_order_sync"
SYNC_INTERVAL_HOURS     = int(os.environ.get("SYNC_INTERVAL_HOURS", "1"))
INITIAL_LOOKBACK_HOURS  = int(os.environ.get("INITIAL_LOOKBACK_HOURS", "24"))
SYNC_OVERLAP_MINUTES    = int(os.environ.get("SYNC_OVERLAP_MINUTES", "5"))
NOTES_SYNC_INTERVAL_MINUTES = int(os.environ.get("NOTES_SYNC_INTERVAL_MINUTES", "15"))

# CRITICAL FIX: Base URL checking logic
if not ZD_RAW_URL.startswith("http://") and not ZD_RAW_URL.startswith("https://"):
    if ".zendesk.com" in ZD_RAW_URL:
        ZD_BASE_URL = f"https://{ZD_RAW_URL}"
    else:
        ZD_BASE_URL = f"https://{ZD_RAW_URL}.zendesk.com/api/v2"
else:
    ZD_BASE_URL = ZD_RAW_URL

# Safe cleaner for trailing slashes if passed manually
ZD_BASE_URL = ZD_BASE_URL.rstrip('/')


# ── Helper Functions ──────────────────────────────────────────────────────────
def _headers() -> dict:
    return {
        "Content-Type": "application/json",
        "x-auth-token": os.environ.get("OW_AUTH_TOKEN", OW_AUTH_TOKEN),
    }

def _make_zendesk_session() -> requests.Session:
    """Zendesk Basic Auth session initialization using email/token."""
    session = requests.Session()
    session.auth = (f"{ZD_EMAIL}/token", ZD_API_TOKEN)
    session.headers.update({"Content-Type": "application/json"})
    return session

def _raise_for_status_with_body(response: requests.Response) -> None:
    """Helper to log Zendesk API detailed error body if anything fails."""
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"[ZD] Error Response Body: {response.text}")
        raise e

def convert_ms_to_datetime_str(timestamp_ms: float) -> str:
    timestamp_seconds = timestamp_ms / 1000.0
    dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f') + '+00:00'

def convert_ms_to_datetime_obj(timestamp_ms: float) -> datetime:
    if not timestamp_ms:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

def _to_iso_z(dt: datetime) -> str:
    """Format a datetime as an ISO-8601 string with milliseconds + Z suffix,
    matching the format previously hardcoded in __main__."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


# ── Database Setup & Models ───────────────────────────────────────────────────
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

Base = declarative_base()

class WorkOrderSync(Base):
    __tablename__ = "work_order_sync"

    id = Column(Integer, primary_key=True)
    openwrench_workorder_id = Column(BigInteger, unique=True, nullable=False, index=True)
    zendesk_ticket_id = Column(BigInteger)
    zendesk_ticket_url = Column(Text)

    # Meta Mapping Columns
    location_id = Column(BigInteger)
    contractor_client_id = Column(BigInteger)
    contractor_location_id = Column(BigInteger)
    contractor_problem_type_id = Column(BigInteger)
    openwrench_created_at = Column(DateTime)
    openwrench_updated_at = Column(DateTime)
    zendesk_created_at = Column(DateTime, nullable=True)
    zendesk_updated_at = Column(DateTime, nullable=True)
    last_sync_at = Column(DateTime, nullable=True)
    subject = Column(Text)
    description = Column(Text, nullable=True)
    last_error = Column(Text, nullable=True)
    sync_status = Column(String(50), nullable=True)
    status_openwrench = Column(String(100))
    status_zendesk = Column(String(100), nullable=True)

    # Core Data Columns
    title = Column(Text)
    status = Column(String(100))
    priority = Column(String(100))
    requester_email = Column(String(255))
    requester_name = Column(String(255), nullable=True)
    location_name = Column(String(255))
    contractor_client = Column(String(255))
    created_at_ow = Column(DateTime)
    updated_at_ow = Column(DateTime)
    raw_openwrench = Column(JSONB)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class SyncState(Base):

    __tablename__ = "sync_state"

    key = Column(String(100), primary_key=True)
    value = Column(Text, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class SyncedNote(Base):

    __tablename__ = "synced_notes"

    id             = Column(Integer, primary_key=True)
    openwrench_workorder_id = Column(BigInteger, nullable=False, index=True)
    note_key       = Column(Text, nullable=False)           # "{noteAddedBy}_{noteAddedAt}"
    zendesk_ticket_id = Column(BigInteger, nullable=False)
    synced_at      = Column(DateTime, server_default=func.now())


# Initialize tables
Base.metadata.create_all(bind=engine)
print("Tables Verified / Created Successfully")


# ── Sync State Helpers (dynamic since_created_at) ─────────────────────────────
SYNC_STATE_KEY = "openwrench_work_orders_last_created_at"

def get_last_synced_timestamp(db) -> str:

    state = db.query(SyncState).filter(SyncState.key == SYNC_STATE_KEY).first()
    if state and state.value:
        logger.info(f"[SYNC] Using checkpoint from sync_state: {state.value}")
        return state.value

    latest = db.query(func.max(WorkOrderSync.openwrench_created_at)).scalar()
    if latest:
        iso = _to_iso_z(latest)
        logger.info(f"[SYNC] No checkpoint found, derived from existing records: {iso}")
        return iso

    fallback_dt = datetime.now(timezone.utc) - timedelta(hours=INITIAL_LOOKBACK_HOURS)
    iso = _to_iso_z(fallback_dt)
    logger.info(f"[SYNC] No checkpoint or existing records, using initial lookback: {iso}")
    return iso


def save_last_synced_timestamp(db, timestamp_iso: str) -> None:
    state = db.query(SyncState).filter(SyncState.key == SYNC_STATE_KEY).first()
    if state:
        state.value = timestamp_iso
    else:
        state = SyncState(key=SYNC_STATE_KEY, value=timestamp_iso)
        db.add(state)
    db.commit()
    logger.info(f"[SYNC] Checkpoint updated to: {timestamp_iso}")


# ── Zendesk Client Implementation ─────────────────────────────────────────────
def _safe_int_env(value: str | None, label: str) -> int | None:

    if not value:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(f"[CONFIG] {label} is set but not a valid integer: {value!r}. Ignoring it.")
        return None


def create_ticket(
    subject:         str,
    body:            str,
    ow_id:           int,
    priority:        str  = "normal",
    tags:            list[str]  = None,
    requester_email: str | None = None,
    requester_name:  str | None = None,
    author_id:       int | None = None,
) -> dict[str, Any]:

    url = f"{ZD_BASE_URL}/tickets"

    comment: dict[str, Any] = {"body": body}
    if author_id:
        comment["author_id"] = author_id

    all_tags = [OW_TAG, f"ow_wo_{ow_id}"] + (tags or [])

    zd_priority = "normal"
    if priority:
        p_lower = priority.lower()
        if p_lower in ["low", "normal", "high", "urgent"]:
            zd_priority = p_lower

    ticket: dict[str, Any] = {
        "subject":     subject,
        "comment":     comment,
        "priority":    zd_priority,
        "tags":        all_tags,
        "external_id": str(ow_id),
    }

    if requester_email:
        ticket["requester"] = {
            "email": requester_email,
            "name":  requester_name or requester_email,
        }

    if ZD_OW_FIELD_ID:
        field_id = _safe_int_env(ZD_OW_FIELD_ID, "ZD_OW_FIELD_ID")
        if field_id:
            ticket["custom_fields"] = [{"id": field_id, "value": str(ow_id)}]

    if ZD_TICKET_FORM_ID:
        form_id = _safe_int_env(ZD_TICKET_FORM_ID, "ZD_TICKET_FORM_ID")
        if form_id:
            ticket["ticket_form_id"] = form_id

    if ZD_GROUP_ID:
        group_id = _safe_int_env(ZD_GROUP_ID, "ZD_GROUP_ID")
        if group_id:
            ticket["group_id"] = group_id

    if ZD_BRAND_ID:
        brand_id = _safe_int_env(ZD_BRAND_ID, "ZD_BRAND_ID")
        if brand_id:
            ticket["brand_id"] = brand_id

    payload = {"ticket": ticket}

    logger.info(f"[ZD] Sending POST request to URL: {url}")

    session = _make_zendesk_session()
    resp    = session.post(url, json=payload, timeout=ZD_TIMEOUT)
    _raise_for_status_with_body(resp)
    result = resp.json()["ticket"]
    logger.info(
        "[ZD] Ticket created  id=%s  status=%s  subject=%r",
        result["id"], result["status"], result["subject"],
    )
    return result


# ── API Business Logic ────────────────────────────────────────────────────────
def fetch_work_orders(
    offset: int = 0,
    limit: int = 100,
    since_created_at: str | None = None,
) -> dict[str, Any]:

    url = f"{OW_BASE_URL}/work_order/work_orders/search"
    params = {
        "offset":  offset,
        "limit":   limit,
        "sort_by": "createdAt",
        "order":   "desc",
    }
    resp = requests.get(url, headers=_headers(), params=params, timeout=OW_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    filtered = []

    if since_created_at:
        since_dt = datetime.fromisoformat(since_created_at.replace("Z", "+00:00"))
    else:
        since_dt = datetime.min.replace(tzinfo=timezone.utc)

    for i in data.get('data', []):
        t = i.get('source', {}).get('createdAt')
        if not t:
            continue

        converted_date = convert_ms_to_datetime_str(t)

        try:
            created_dt = datetime.fromisoformat(converted_date)
        except ValueError:
            continue

        if created_dt > since_dt:
            filtered.append(i)
        else:
            break

    data["data"] = filtered
    return data


# ── OpenWrench Single Work Order Detail Fetch ─────────────────────────────────
def fetch_work_order_detail(ow_id: int) -> dict[str, Any] | None:
    """Fetch the latest full detail of a single work order from OpenWrench."""
    url = f"{OW_BASE_URL}/work_order/work_orders/{ow_id}"
    try:
        resp = requests.get(url, headers=_headers(), timeout=OW_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        # Response is {"type":"WorkOrder","data":{...}} or bare dict
        return body.get("data", body)
    except Exception as e:
        logger.error(f"[OW] Failed to fetch work order detail for {ow_id}: {e}")
        return None


# ── Zendesk Ticket Update (comment + status via PUT) ──────────────────────────
def update_zendesk_ticket(
    zd_ticket_id: int,
    comment_body: str | None = None,
    status: str | None = None,
) -> bool:

    ticket_update: dict[str, Any] = {}

    if comment_body:
        ticket_update["comment"] = {
            "body": comment_body,
            "public": True,
        }

    if status:
        ticket_update["status"] = status.lower()

    if not ticket_update:
        return True  # Nothing to update

    url = f"{ZD_BASE_URL}/tickets/{zd_ticket_id}"
    session = _make_zendesk_session()
    try:
        resp = session.put(url, json={"ticket": ticket_update}, timeout=ZD_TIMEOUT)
        _raise_for_status_with_body(resp)
        logger.info(f"[ZD] Ticket {zd_ticket_id} updated — comment={bool(comment_body)}, status={status}")
        return True
    except Exception as e:
        logger.error(f"[ZD] Failed to update ticket {zd_ticket_id}: {e}")
        return False


# ── OW → Zendesk Notes + Status Sync (runs every N minutes) ──────────────────
# OpenWrench status values that mean the job is closed/done.
OW_CLOSED_STATUSES = {
    "Completed", "WaitingForReview", "Closed", "Cancelled",
    "CompletedByServiceProvider", "ClosedByBuyer",
}

def sync_ow_updates_to_zendesk() -> None:

    db = SessionLocal()
    try:
        # Only process tickets that are not yet solved/closed on Zendesk side.
        active_records = db.query(WorkOrderSync).filter(
            WorkOrderSync.zendesk_ticket_id.isnot(None),
            WorkOrderSync.status_zendesk.notin_(["solved", "closed"]),
        ).all()

        logger.info(f"[NOTES-SYNC] Checking {len(active_records)} active work orders for OW updates.")

        for record in active_records:
            ow_id     = record.openwrench_workorder_id
            zd_id     = record.zendesk_ticket_id

            wo = fetch_work_order_detail(ow_id)
            if not wo:
                continue

            # ── 1. Sync new notes → Zendesk public comments ──────────────────
            notes = wo.get("notes") or []
            for note in notes:
                note_text      = note.get("text") or ""
                note_added_by  = note.get("noteAddedBy") or ""
                note_added_at  = note.get("noteAddedAt") or ""

                if not note_text.strip():
                    continue

                # Stable dedup key — OW doesn't expose a note ID so we use
                # author + timestamp which is unique per note in practice.
                note_key = f"{note_added_by}_{note_added_at}"

                already_synced = db.query(SyncedNote).filter(
                    SyncedNote.openwrench_workorder_id == ow_id,
                    SyncedNote.note_key == note_key,
                ).first()

                if already_synced:
                    continue  # Already pushed to Zendesk, skip.

                comment_body = (
                    f"[OpenWrench Update]\n"
                    f"By: {note_added_by}\n"
                    f"At: {note_added_at}\n\n"
                    f"{note_text}"
                )

                success = update_zendesk_ticket(zd_id, comment_body=comment_body)
                if success:
                    db.add(SyncedNote(
                        openwrench_workorder_id=ow_id,
                        note_key=note_key,
                        zendesk_ticket_id=zd_id,
                    ))
                    db.commit()
                    logger.info(f"[NOTES-SYNC] Pushed OW note to ZD ticket {zd_id} (WO {ow_id}): {note_key}")

            # ── 2. Sync closed/completed OW status → solve Zendesk ticket ────
            ow_status = wo.get("status") or ""
            if ow_status in OW_CLOSED_STATUSES:
                solved = update_zendesk_ticket(zd_id, status="solved")
                if solved:
                    record.status_zendesk    = "solved"
                    record.status_openwrench = ow_status
                    record.last_sync_at      = datetime.now(timezone.utc)
                    db.commit()
                    logger.info(
                        f"[NOTES-SYNC] OW WO {ow_id} is '{ow_status}' → "
                        f"ZD ticket {zd_id} marked solved."
                    )

            # Update raw snapshot and last_sync_at regardless
            record.raw_openwrench = wo
            record.last_sync_at   = datetime.now(timezone.utc)
            db.commit()

    except Exception as e:
        db.rollback()
        logger.error(f"[NOTES-SYNC] Cycle failed: {e}")
    finally:
        db.close()


# ── Sync Cycle (one full run of the pipeline) ─────────────────────────────────
def run_sync_cycle() -> None:

    offset = 0
    limit = 100

    db = SessionLocal()
    try:
        since_created_at = get_last_synced_timestamp(db)

        # Apply a small overlap so we don't lose anything right at the boundary.
        since_dt = datetime.fromisoformat(since_created_at.replace("Z", "+00:00"))
        since_dt_with_overlap = since_dt - timedelta(minutes=SYNC_OVERLAP_MINUTES)
        since_created_at_query = _to_iso_z(since_dt_with_overlap)

        print(f"Targeting Zendesk Endpoint: {ZD_BASE_URL}")
        print(f"Fetching work orders created after: {since_created_at_query}")

        work_orders_payload = fetch_work_orders(
            offset=offset,
            limit=limit,
            since_created_at=since_created_at_query,
        )

        records = work_orders_payload.get("data", [])

        newest_created_at_ms = None

        for wo in records:
            source = wo.get("source", {})
            wo_id = source.get("id")

            if not wo_id:
                continue

            wo_created_at_ms = source.get("createdAt")
            if wo_created_at_ms and (newest_created_at_ms is None or wo_created_at_ms > newest_created_at_ms):
                newest_created_at_ms = wo_created_at_ms

            existing = db.query(WorkOrderSync).filter(
                WorkOrderSync.openwrench_workorder_id == wo_id
            ).first()

            if existing:
                print(f"Work Order {wo_id} already synced. Skipping.")
                continue

            sub_contracted_details = source.get("subContractedToWorkOrder", [])
            if not isinstance(sub_contracted_details, list) or not sub_contracted_details:
                sub_contracted_details = [{}]

            wo_title = source.get("title") or f"Work Order #{wo_id}"
            wo_description = f"OpenWrench Work Order Sync automatically generated.\n\nStatus: {source.get('status')}\nLocation: {source.get('locationName')}"

            try:
                created_by_contact = source.get("createdByContact") or {}
                requester_name = " ".join(
                    part for part in [
                        created_by_contact.get("nameGiven"),
                        created_by_contact.get("nameFamily"),
                    ] if part
                ).strip() or None

                zendesk_ticket = create_ticket(
                    subject=wo_title,
                    body=wo_description,
                    ow_id=int(wo_id),
                    priority=source.get("woPriorityName", "normal"),
                    requester_email=source.get("createdBy"),
                    requester_name=requester_name,
                    tags=["sync_engine"]
                )
            except Exception as ticket_err:
                logger.error(f"Failed to create Zendesk ticket for Work Order {wo_id}: {ticket_err}")
                continue

            sync_record = WorkOrderSync(
                openwrench_workorder_id=source.get("id"),
                zendesk_ticket_id=zendesk_ticket.get("id"),
                location_id=source.get("locationId"),
                contractor_client_id=sub_contracted_details[0].get("contractorClientId"),
                contractor_location_id=sub_contracted_details[0].get("contractorLocationId"),
                contractor_problem_type_id=source.get("problemTypeId"),
                openwrench_created_at=convert_ms_to_datetime_obj(source.get("createdAt")),
                openwrench_updated_at=convert_ms_to_datetime_obj(source.get("updatedAt")),
                zendesk_created_at=datetime.now(timezone.utc),
                zendesk_updated_at=None,
                last_sync_at=datetime.now(timezone.utc),
                raw_openwrench=wo,
                created_at_ow=convert_ms_to_datetime_obj(source.get("createdAt")),
                updated_at_ow=convert_ms_to_datetime_obj(source.get("updatedAt")),
                zendesk_ticket_url=f"https://{ZD_BASE_URL.split('//')[-1].split('/')[0]}/agent/tickets/{zendesk_ticket.get('id')}",
                subject=wo_title,
                description=wo_description,
                requester_email=source.get("createdBy"),
                requester_name=None,
                location_name=source.get("locationName"),
                last_error=None,
                title=wo_title,
                status=source.get("status"),
                sync_status="SUCCESS",
                priority=source.get("woPriorityName"),
                status_openwrench=source.get("status"),
                status_zendesk=zendesk_ticket.get("status"),
                contractor_client=sub_contracted_details[0].get("contractorClientName"),
            )

            db.add(sync_record)
            print(f"Staged & Created Zendesk Ticket for Work Order {wo_id}.")

        db.commit()
        print("Database sync committed successfully.")

        # Advance checkpoint only if we actually saw newer work orders this run.
        if newest_created_at_ms:
            newest_dt = convert_ms_to_datetime_obj(newest_created_at_ms)
            save_last_synced_timestamp(db, _to_iso_z(newest_dt))
        else:
            logger.info("[SYNC] No new work orders found this cycle — checkpoint unchanged.")

    except Exception as e:
        db.rollback()
        logger.error(f"Error during execution pipeline: {e}")
        raise
    finally:
        db.close()


# ── Background Scheduler (runs run_sync_cycle automatically every N hours) ───
def start_scheduler() -> None:
    scheduler = BlockingScheduler(
        executors={"default": APThreadPoolExecutor(2)},  # 2 threads — one per job
        timezone="UTC",
    )

    def _new_tickets_job():
        logger.info("[SCHEDULER] New tickets sync cycle starting...")
        try:
            run_sync_cycle()
        except Exception:
            logger.error("[SCHEDULER] New tickets sync failed, will retry on next interval.")
        logger.info("[SCHEDULER] New tickets sync cycle finished.")

    def _notes_sync_job():
        logger.info("[SCHEDULER] OW → ZD notes/status sync starting...")
        try:
            sync_ow_updates_to_zendesk()
        except Exception:
            logger.error("[SCHEDULER] Notes sync failed, will retry on next interval.")
        logger.info("[SCHEDULER] OW → ZD notes/status sync finished.")

    # Job 1: Fetch new OW work orders → create Zendesk tickets (every N hours)
    scheduler.add_job(
        _new_tickets_job,
        "interval",
        hours=SYNC_INTERVAL_HOURS,
        id=SYNC_JOB_NAME,
        next_run_time=datetime.now(timezone.utc),
        max_instances=1,
        coalesce=True,
    )

    # Job 2: Poll active OW work orders → sync notes + status to Zendesk (every N minutes)
    scheduler.add_job(
        _notes_sync_job,
        "interval",
        minutes=NOTES_SYNC_INTERVAL_MINUTES,
        id="ow_notes_to_zendesk_sync",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=10),
        max_instances=1,
        coalesce=True,
    )

    logger.info(
        f"[SCHEDULER] Started. "
        f"New tickets: every {SYNC_INTERVAL_HOURS}h | "
        f"Notes/status sync: every {NOTES_SYNC_INTERVAL_MINUTES}min"
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[SCHEDULER] Stopped by user.")


# ── Entrypoint ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # No manual since_created_at needed anymore — it's derived automatically
    # from sync_state on each run. This now starts a long-running background
    # process that runs the sync every SYNC_INTERVAL_HOURS (default 1 hour).
    start_scheduler()
