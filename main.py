import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Depends, Header
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor

import config
from database import Base, engine, get_db
from models import WorkOrderSync, SyncRun, WebhookEventLog
import sync_engine
import webhook_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

                       # add any missing columns first
Base.metadata.create_all(bind=engine)   # then create any missing tables
logger.info("Tables verified / created successfully.")

app = FastAPI(title="OpenWrench <-> Zendesk Sync")
templates = Jinja2Templates(directory="templates")

scheduler = AsyncIOScheduler(executors={"default": APThreadPoolExecutor(1)}, timezone="UTC")


def _scheduled_job() -> None:
    try:
        sync_engine.run_sync_cycle(trigger_type="scheduled")
    except Exception:
        logger.error("[SCHEDULER] Sync cycle failed; will retry on next interval.")
        raise


def _notes_sync_job() -> None:
    try:
        sync_engine.sync_ow_notes_to_zendesk()
    except Exception:
        logger.error("[SCHEDULER] Notes sync failed; will retry on next interval.")
        raise

@app.on_event("startup")
async def on_startup():
    scheduler.add_job(
        _scheduled_job,
        "interval",
        hours=config.SYNC_INTERVAL_HOURS,
        id=config.SYNC_JOB_NAME,
        next_run_time=datetime.now(timezone.utc),
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.add_job(
         _notes_sync_job,
         "interval",
         minutes=config.NOTES_SYNC_INTERVAL_MINUTES,
         id="ow_notes_to_zendesk_sync",
         next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
         max_instances=1,
         coalesce=True,
         replace_existing=True,
     )
    scheduler.start()
    logger.info(
        f"[SCHEDULER] Started. "
        f"New tickets: every {config.SYNC_INTERVAL_HOURS}h | "
        f"Notes/status sync: every {config.NOTES_SYNC_INTERVAL_MINUTES}min"
    )


@app.on_event("shutdown")
async def on_shutdown():
    scheduler.shutdown(wait=False)


# Zendesk webhook payload models 
class TicketDetail(BaseModel):
    id: str
    external_id: Optional[str] = None
    status: str
    priority: Optional[str] = None
    subject: Optional[str] = None
    tags: Optional[List[str]] = []


class ZendeskEventPayload(BaseModel):
    id: Optional[str] = None           # Zendesk event UUID — used for dedup
    account_id: Optional[int] = None
    detail: TicketDetail
    event: Optional[Dict[str, Any]] = None
    type: Optional[str] = None


@app.post("/webhook/zendesk")
async def handle_zendesk_webhook(
    payload: ZendeskEventPayload,
    background_tasks: BackgroundTasks,
    db=Depends(get_db),
    x_webhook_secret: Optional[str] = Header(default=None),
):
   # print(x_webhook_secret)
   # if config.WEBHOOK_SHARED_SECRET and x_webhook_secret != config.WEBHOOK_SHARED_SECRET:
    #    raise HTTPException(status_code=401, detail="Invalid webhook secret")

    ticket_id = int(payload.detail.id)
    logger.info(f"[WEBHOOK] Received '{payload.type}' for ticket #{ticket_id}")

    # Log every inbound event immediately, before any processing, so nothing
    # is silently lost even if OpenWrench is briefly slow/down.
    log_row = WebhookEventLog(
        event_type=payload.type or "unknown",
        zendesk_ticket_id=ticket_id,
        status="received",
        zendesk_event_id=payload.id or None,
    )
    db.add(log_row)
    db.commit()
    db.refresh(log_row)


    background_tasks.add_task(webhook_handler.process_zendesk_event, payload.dict(), log_row.id)

    return {"status": "queued", "event_log_id": log_row.id}


# ── Manual + scheduled sync control ─────────────────────────────────────────
@app.post("/sync/run")
async def trigger_manual_sync():
    try:
        result = await asyncio.to_thread(sync_engine.run_sync_cycle, "manual")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/notes")
async def trigger_manual_notes_sync():
    """Manually trigger the OW → Zendesk notes/status sync (same as the 15-min scheduler job)."""
    try:
        await asyncio.to_thread(sync_engine.sync_ow_notes_to_zendesk)
        return {"message": "Notes sync completed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Dashboard ────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, db=Depends(get_db)):
    recent_runs = db.query(SyncRun).order_by(SyncRun.started_at.desc()).limit(15).all()
    recent_events = db.query(WebhookEventLog).order_by(WebhookEventLog.received_at.desc()).limit(15).all()
    recent_work_orders = db.query(WorkOrderSync).order_by(WorkOrderSync.created_at.desc()).limit(15).all()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "recent_runs": recent_runs,
            "recent_events": recent_events,
            "recent_work_orders": recent_work_orders,
            "sync_interval_hours": config.SYNC_INTERVAL_HOURS,
        },
    )


@app.get("/api/status")
async def api_status(db=Depends(get_db)):
    recent_runs = db.query(SyncRun).order_by(SyncRun.started_at.desc()).limit(15).all()
    recent_events = db.query(WebhookEventLog).order_by(WebhookEventLog.received_at.desc()).limit(15).all()
    return {
        "runs": [
            {
                "id": r.id, "trigger_type": r.trigger_type, "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "records_found": r.records_found, "records_created": r.records_created,
                "records_skipped": r.records_skipped, "error_message": r.error_message,
            }
            for r in recent_runs
        ],
        "events": [
            {
                "id": e.id, "event_type": e.event_type, "zendesk_ticket_id": e.zendesk_ticket_id,
                "openwrench_workorder_id": e.openwrench_workorder_id, "status": e.status,
                "received_at": e.received_at.isoformat() if e.received_at else None,
                "detail": e.detail,
            }
            for e in recent_events
        ],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
