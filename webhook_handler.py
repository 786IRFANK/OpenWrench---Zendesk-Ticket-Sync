import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import ow_client
from database import SessionLocal
from models import WorkOrderSync, WebhookEventLog
logger = logging.getLogger("webhook_handler")

_locks: dict[int, asyncio.Lock] = {}
_locks_guard = asyncio.Lock()


async def _get_lock(ow_id: int) -> asyncio.Lock:
    async with _locks_guard:
        if ow_id not in _locks:
            _locks[ow_id] = asyncio.Lock()
        return _locks[ow_id]


def _resolve_ow_id(db, ticket_id: int, external_id: Optional[str]) -> Optional[int]:
    if external_id and external_id.strip().isdigit():
        return int(external_id)
    record = db.query(WorkOrderSync).filter(WorkOrderSync.zendesk_ticket_id == ticket_id).first()
    return record.openwrench_workorder_id if record else None


async def process_zendesk_event(payload: dict, log_id: int) -> None:

    db = SessionLocal()
    log_row = db.query(WebhookEventLog).filter(WebhookEventLog.id == log_id).first()
    try:
        detail = payload.get("detail", {})
        ticket_id = int(detail.get("id"))
        event_type = payload.get("type")

        ow_id = _resolve_ow_id(db, ticket_id, detail.get("external_id"))
        if not ow_id:
            logger.warning(f"No OpenWrench mapping found for Zendesk ticket #{ticket_id}")
            if log_row:
                log_row.status = "skipped"
                log_row.detail = "No linked OpenWrench work order found."
                log_row.processed_at = datetime.now(timezone.utc)
                db.commit()
            return

        if log_row:
            log_row.openwrench_workorder_id = ow_id
            log_row.status = "processing"
            db.commit()

        # Serialize per work-order so concurrent events for the same WO
        # never run their OpenWrench calls at the same time.
        lock = await _get_lock(ow_id)
        async with lock:
            await asyncio.to_thread(_handle_event_sync, db, payload, ow_id, event_type)

        if log_row:
            log_row.status = "success"
            log_row.processed_at = datetime.now(timezone.utc)
            db.commit()

    except Exception as e:
        logger.error(f"[WEBHOOK] Processing failed: {e}")
        if log_row:
            log_row.status = "failed"
            log_row.detail = str(e)
            log_row.processed_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()


def _append_attachment_links(comment_body: str, attachments) -> str:

    if not attachments:
        return comment_body

    lines = []
    for att in attachments:
        if not isinstance(att, dict):
            continue
        name = att.get("file_name") or att.get("filename") or att.get("name") or "attachment"
        url = att.get("content_url") or att.get("url") or att.get("mapped_content_url")
        if url:
            lines.append(f"- {name}: {url}")
        else:
            lines.append(f"- {name}")

    if not lines:
        return comment_body

    return f"{comment_body}\n\nAttachments:\n" + "\n".join(lines)


def _handle_event_sync(db, payload: dict, ow_id: int, event_type: str) -> None:

    detail = payload.get("detail", {})
    current_iso_time = ow_client.now_iso_z()
    ow_mapped_status = "IN_PROGRESS"

    if event_type == "zen:event-type:ticket.comment_added":
        event = payload.get("event") or {}
        comment = event.get("comment") or {}
        comment_body = comment.get("body", "")
        zd_comment_id = str(comment.get("id", ""))

        # ── LOOP PREVENTION (primary: comment ID check) ──────────────────────
        # When notes sync pushes an OW note to ZD, it stores the returned ZD
        # comment ID in WorkOrderSync.synced_note_keys as "zd_cid:{id}".
        # Checking the ID is reliable — no text matching, no prefix dependency.
        if zd_comment_id:
            import json
            record = db.query(WorkOrderSync).filter(
                WorkOrderSync.openwrench_workorder_id == ow_id
            ).first()
            if record:
                try:
                    pushed_keys = json.loads(record.synced_note_keys or "[]")
                except (ValueError, TypeError):
                    pushed_keys = []
                if f"zd_cid:{zd_comment_id}" in pushed_keys:
                    logger.info(
                        f"[WEBHOOK] Skipping comment_added for WO {ow_id} — "
                        f"ZD comment ID {zd_comment_id} was pushed by our OW sync, not a human comment."
                    )
                    return

        # ── LOOP PREVENTION (fallback: prefix check) ─────────────────────────
        # If comment_id wasn't stored (e.g. ZD didn't return it), fall back to
        # checking the [OpenWrench Note] prefix as a secondary safety net.
        if comment_body.startswith("[OpenWrench Note]"):
            logger.info(f"[WEBHOOK] Skipping comment_added for WO {ow_id} — [OpenWrench Note] prefix detected.")
            return

        # ── DUPLICATE WEBHOOK DEDUP ──────────────────────────────────────────
        zendesk_event_id = payload.get("id")
        if zendesk_event_id:
            already_done = db.query(WebhookEventLog).filter(
                WebhookEventLog.zendesk_event_id == zendesk_event_id,
                WebhookEventLog.status == "success",
            ).first()
            if already_done:
                logger.info(
                    f"[WEBHOOK] Skipping duplicate event {zendesk_event_id} "
                    f"for WO {ow_id} — already processed (log #{already_done.id})."
                )
                return

        if not comment_body:
            comment_body = "New activity comment appended via Zendesk update sync engine."

        note_text = _append_attachment_links(comment_body, comment.get("attachments"))
        ow_client.append_notes(ow_id, f"[Zendesk Comment]\n{note_text}", current_iso_time)
        return

    if event_type == "zen:event-type:ticket.attachment_linked_to_comment":
        # This is the event Zendesk actually fires for file attachments —
        # it arrives SEPARATELY from comment_added, with a single
        # `event.comment.attachment` object (not an array).
        event = payload.get("event") or {}
        comment = event.get("comment") or {}
        attachment = comment.get("attachment") or {}
        content_url = attachment.get("content_url")
        filename = attachment.get("filename") or attachment.get("file_name") or "attachment"

        if not content_url:
            logger.warning(f"[WEBHOOK] attachment_linked_to_comment for WO {ow_id} had no content_url; skipping.")
            return

        # Push it as a real photo note (OpenWrench's `note.photo` field),
        # not just a text link — this makes it render as an actual image
        # on the OpenWrench side instead of a clickable Zendesk URL.
        ow_client.append_notes(
            ow_id,
            note_text=f"[Zendesk Comment]\nAttachment from Zendesk: {filename}",
            current_iso_time=current_iso_time,
            photo_url=content_url,
        )
        return

    if event_type == "zen:event-type:ticket.status_changed":
        zd_status = (detail.get("status") or "").upper()
        logger.info(f"[WEBHOOK] WO {ow_id}: lifecycle transition to {zd_status}")

        if zd_status in ["OPEN", "HOLD", "PENDING"]:
            service_call_id = ow_client.execute_tech_scheduled(ow_id, current_iso_time)
            ow_client.execute_remote_check_in(service_call_id, current_iso_time)
            ow_mapped_status = "IN_PROGRESS"

        elif zd_status in ["SOLVED", "CLOSED"]:
            service_call_id = ow_client.execute_tech_scheduled(ow_id, current_iso_time)
            service_call_id = ow_client.execute_remote_check_in(service_call_id, current_iso_time)
            ow_client.execute_remote_checkout(service_call_id, current_iso_time)
            ow_mapped_status = "COMPLETED"
        else:
            return  # status we don't act on

        record = db.query(WorkOrderSync).filter(WorkOrderSync.openwrench_workorder_id == ow_id).first()
        if record:
            record.status_zendesk = detail.get("status")
            record.status_openwrench = ow_mapped_status
            db.commit()
