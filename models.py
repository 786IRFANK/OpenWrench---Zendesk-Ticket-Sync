from sqlalchemy import Column, BigInteger, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from database import Base


class WorkOrderSync(Base):

    __tablename__ = "work_order_sync"

    id = Column(Integer, primary_key=True)
    openwrench_workorder_id = Column(BigInteger, unique=True, nullable=False, index=True)
    zendesk_ticket_id = Column(BigInteger)
    zendesk_ticket_url = Column(Text)

    location_id = Column(BigInteger)
    contractor_client_id = Column(BigInteger)
    contractor_location_id = Column(BigInteger)
    contractor_problem_type_id = Column(BigInteger)
    openwrench_created_at = Column(DateTime)
    openwrench_updated_at = Column(DateTime)
    zendesk_created_at = Column(DateTime, nullable=True)
    zendesk_updated_at = Column(DateTime, nullable=True)
    last_sync_at = Column(DateTime, nullable=True)
    last_note_synced_at = Column(DateTime, nullable=True)
    # JSON array of note fingerprints ("{noteAddedBy}|{noteAddedAt}") already
    # pushed to Zendesk. Used instead of a single timestamp so that multiple
    # notes with the same second-level timestamp never cause duplicates.
    synced_note_keys = Column(Text, nullable=True, default="[]")
    subject = Column(Text)
    description = Column(Text, nullable=True)
    last_error = Column(Text, nullable=True)
    sync_status = Column(String(50), nullable=True)
    status_openwrench = Column(String(100))
    status_zendesk = Column(String(100), nullable=True)

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


class SyncRun(Base):

    __tablename__ = "sync_run"

    id = Column(Integer, primary_key=True)
    trigger_type = Column(String(20))       # "scheduled" | "manual"
    status = Column(String(20))             # "running" | "success" | "failed"
    started_at = Column(DateTime, server_default=func.now())
    finished_at = Column(DateTime, nullable=True)
    records_found = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)


class WebhookEventLog(Base):

    __tablename__ = "webhook_event_log"

    id = Column(Integer, primary_key=True)
    received_at = Column(DateTime, server_default=func.now())
    processed_at = Column(DateTime, nullable=True)
    event_type = Column(String(100))
    zendesk_ticket_id = Column(BigInteger, nullable=True)
    openwrench_workorder_id = Column(BigInteger, nullable=True)
    status = Column(String(20), default="received")  # received|processing|success|failed|skipped
    detail = Column(Text, nullable=True)
    # Zendesk event UUID (payload["id"]) — used to detect and skip duplicate
    # webhook deliveries when Zendesk retries the same event.
    zendesk_event_id = Column(String(100), nullable=True, index=True)
