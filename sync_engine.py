
import logging
from datetime import datetime, timezone, timedelta

import config
import ow_client
from database import SessionLocal
from models import WorkOrderSync, SyncRun

logger = logging.getLogger("sync_engine")

# OW statuses that mean the job is fully closed — trigger Zendesk "solved"
OW_CLOSED_STATUSES = {
    "Completed", "WaitingForReview", "Closed", "Cancelled",
    "CompletedByServiceProvider", "ClosedByBuyer",
}


def run_sync_cycle(trigger_type: str = "scheduled") -> dict:
    offset, limit = 0, 100
    db = SessionLocal()
    run = SyncRun(trigger_type=trigger_type, status="running")
    db.add(run)
    db.commit()
    db.refresh(run)

    created, skipped = 0, 0
    try:
        since_created_at = ow_client.get_last_synced_timestamp(db)
        since_dt = datetime.fromisoformat(since_created_at.replace("Z", "+00:00"))
        since_dt_with_overlap = since_dt - timedelta(minutes=config.SYNC_OVERLAP_MINUTES)
        since_created_at_query = ow_client.to_iso_z(since_dt_with_overlap)

        logger.info(f"[SYNC #{run.id}] ({trigger_type}) Fetching work orders created after: {since_created_at_query}")

        work_orders_payload = ow_client.fetch_work_orders(
            offset=offset, limit=limit, since_created_at=since_created_at_query
        )
        records = work_orders_payload.get("data", [])
        logger.info(f"[SYNC #{run.id}] {len(records)} candidate work order(s) found.")

        newest_created_at_ms = None

        for wo in records:
            source = wo.get("source", {})
            wo_id = source.get("id")
            if not wo_id:
                continue

            wo_created_at_ms = source.get("createdAt")
            if wo_created_at_ms and (newest_created_at_ms is None or wo_created_at_ms > newest_created_at_ms):
                newest_created_at_ms = wo_created_at_ms

            existing = db.query(WorkOrderSync).filter(WorkOrderSync.openwrench_workorder_id == wo_id).first()
            if existing:
                skipped += 1
                continue

            try:
                sub_contracted_details = source.get("subContractedToWorkOrder", [])
                if not isinstance(sub_contracted_details, list) or not sub_contracted_details:
                    sub_contracted_details = [{}]
                if not isinstance(sub_contracted_details[0], dict):
                    sub_contracted_details = [{}]

                wo_title = source.get("title") or f"Work Order #{wo_id}"
                wo_description = (
                    f"OpenWrench Work Order Sync automatically generated.\n\n"
                    f"Status: {source.get('status')}\nLocation: {source.get('locationName')}"
                )

                # If the work order already has notes on the OpenWrench side
                # (e.g. added before the ticket ever existed), fold them into
                # the ticket body so they aren't lost/invisible to the agent.
                existing_notes_text = ow_client.format_notes_for_ticket(source.get("notes", []))
                if existing_notes_text:
                    wo_description = f"{wo_description}\n\n{existing_notes_text}"

                created_by_contact = source.get("createdByContact")
                if not isinstance(created_by_contact, dict):
                    created_by_contact = {}
                requester_name = " ".join(
                    p for p in [created_by_contact.get("nameGiven"), created_by_contact.get("nameFamily")] if p
                ).strip() or None

                zendesk_ticket = ow_client.create_ticket(
                    subject=wo_title,
                    body=wo_description,
                    ow_id=int(wo_id),
                    priority=source.get("woPriorityName", "normal"),
                    requester_email=source.get("createdBy"),
                    requester_name=requester_name,
                    tags=["sync_engine"],
                )
            except Exception as record_err:
                logger.error(f"[SYNC #{run.id}] Failed to process/create ticket for WO {wo_id}: {record_err}")
                continue

            sync_record = WorkOrderSync(
                openwrench_workorder_id=source.get("id"),
                zendesk_ticket_id=zendesk_ticket.get("id"),
                location_id=source.get("locationId"),
                contractor_client_id=sub_contracted_details[0].get("contractorClientId"),
                contractor_location_id=sub_contracted_details[0].get("contractorLocationId"),
                contractor_problem_type_id=source.get("problemTypeId"),
                openwrench_created_at=ow_client.convert_ms_to_datetime_obj(source.get("createdAt")),
                openwrench_updated_at=ow_client.convert_ms_to_datetime_obj(source.get("updatedAt")),
                zendesk_created_at=datetime.now(timezone.utc),
                zendesk_updated_at=None,
                last_sync_at=datetime.now(timezone.utc),
                raw_openwrench=wo,
                created_at_ow=ow_client.convert_ms_to_datetime_obj(source.get("createdAt")),
                updated_at_ow=ow_client.convert_ms_to_datetime_obj(source.get("updatedAt")),
                zendesk_ticket_url=(
                    f"https://{config.ZD_BASE_URL.split('//')[-1].split('/')[0]}"
                    f"/agent/tickets/{zendesk_ticket.get('id')}"
                ),
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
            created += 1
            logger.info(f"[SYNC #{run.id}] Created Zendesk ticket for Work Order {wo_id}.")

        db.commit()

        if newest_created_at_ms:
            newest_dt = ow_client.convert_ms_to_datetime_obj(newest_created_at_ms)
            ow_client.save_last_synced_timestamp(db, ow_client.to_iso_z(newest_dt))
        else:
            logger.info(f"[SYNC #{run.id}] No new work orders — checkpoint unchanged.")

        run.status = "success"
        run.records_found = len(records)
        run.records_created = created
        run.records_skipped = skipped
        run.finished_at = datetime.now(timezone.utc)
        db.commit()

        logger.info(f"[SYNC #{run.id}] Completed. found={len(records)} created={created} skipped={skipped}")
        return {
            "run_id": run.id, "status": "success",
            "found": len(records), "created": created, "skipped": skipped,
        }

    except Exception as e:
        db.rollback()
        run.status = "failed"
        run.error_message = str(e)
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
        logger.error(f"[SYNC #{run.id}] Failed: {e}")
        raise
    finally:
        db.close()


def sync_ow_notes_to_zendesk() -> None:
    import json

    db = SessionLocal()
    try:
        active_records = db.query(WorkOrderSync).filter(
            WorkOrderSync.zendesk_ticket_id.isnot(None),
            WorkOrderSync.status_zendesk.notin_(["solved", "closed"]),
        ).all()

        logger.info(f"[NOTES-SYNC] Checking {len(active_records)} active work order(s) for OW updates.")

        for record in active_records:
            ow_id = record.openwrench_workorder_id
            zd_id = record.zendesk_ticket_id

            # if ow_id == 2653553:
            #     logger.info(f"[NOTES-SYNC] Processing WO {ow_id} (test case).")

            try:
                wo = ow_client.fetch_work_order_details(ow_id)
            except Exception as e:
                logger.error(f"[NOTES-SYNC] Could not fetch OW WO {ow_id}: {e}")
                continue

                # Load already-pushed fingerprints from the TEXT column.
            try:
                pushed_keys: list = json.loads(record.synced_note_keys or "[]")
                if not isinstance(pushed_keys, list):
                    pushed_keys = []
            except (ValueError, TypeError):
                pushed_keys = []

            notes = wo.get("notes") or []

            for note in notes:
                if not isinstance(note, dict):
                    continue

                note_text     = (note.get("text") or "").strip()
                note_photo    = (note.get("photo") or "").strip()
                note_caption  = (note.get("caption") or "").strip()
                note_added_at = note.get("noteAddedAt") or ""
                note_added_by = note.get("noteAddedBy") or ""

                    # Must have at least a timestamp to build a fingerprint
                if not note_added_at:
                    continue

                    # Skip notes with neither text nor photo
                if not note_text and not note_photo:
                    continue

                    # Skip notes pushed from Zendesk → OW by our webhook handler
                if note_text.startswith("[Zendesk Comment]"):
                    continue

                fingerprint = f"{note_added_by}|{note_added_at}"

                if fingerprint in pushed_keys:
                    logger.debug(f"[NOTES-SYNC] Already synced, skipping: {fingerprint}")
                    continue

                    # ── Handle photo/attachment note ──────────────────────────────
                if note_photo:
                    import os
                    filename = os.path.basename(note_photo.split("?")[0]) or "attachment"
                    token = ow_client.upload_file_to_zendesk(note_photo, filename)
                    if not token:
                        logger.warning(f"[NOTES-SYNC] Could not upload OW photo to ZD for WO {ow_id}, skipping.")
                        continue

                        # Build comment body: use caption if text is empty
                    body_text = "Attachment from OpenWrench."
                    comment_body = (
                        f"[OpenWrench Note]\n"
                        f"By: {note_added_by}  |  At: {note_added_at}\n\n"
                        f"{body_text}"
                    )
                    result = ow_client.update_zendesk_ticket(
                        zd_id,
                        comment_body=comment_body,
                        uploads=[token],
                    )

                    # ── Handle plain text note ────────────────────────────────────
                else:
                    comment_body = (
                        f"[OpenWrench Note]\n"
                        f"By: {note_added_by}  |  At: {note_added_at}\n\n"
                        f"{note_text}"
                    )
                    result = ow_client.update_zendesk_ticket(zd_id, comment_body=comment_body)
                if not result["success"]:
                    logger.warning(f"[NOTES-SYNC] ZD push failed for WO {ow_id}, skipping fingerprint save.")
                    continue

                    # Save the OW note fingerprint so we don't push it again.
                pushed_keys.append(fingerprint)

                    # Also save the ZD comment ID (if returned) so the webhook
                    # handler can identify this comment by ID — more reliable than
                    # text prefix matching.
                zd_comment_id = result.get("comment_id")
                if zd_comment_id:
                    pushed_keys.append(f"zd_cid:{zd_comment_id}")
                record.synced_note_keys = json.dumps(pushed_keys)
                record.last_note_synced_at = datetime.now(timezone.utc)
                record.last_sync_at = datetime.now(timezone.utc)
                try:
                    db.commit()
                        # Re-read pushed_keys from the now-committed record so the
                        # next note sees the latest state from DB (expire_on_commit
                        # resets the attribute; we re-load it explicitly).
                    db.refresh(record)
                    try:
                        pushed_keys = json.loads(record.synced_note_keys or "[]")
                    except (ValueError, TypeError):
                         pass  # keep the in-memory list if re-read fails
                except Exception as commit_err:
                    db.rollback()
                    pushed_keys.pop()  # undo local append since it wasn't saved
                    logger.error(f"[NOTES-SYNC] DB commit failed after pushing note for WO {ow_id}: {commit_err}")
                    continue

                logger.info(f"[NOTES-SYNC] Pushed OW note to ZD ticket {zd_id} (WO {ow_id}): {fingerprint}")

                # ── Sync OW closed status → solve Zendesk ticket ─────────────────
            ow_status = wo.get("status") or ""
            if ow_status in OW_CLOSED_STATUSES:
                result = ow_client.update_zendesk_ticket(zd_id, status="solved")
                if result["success"]:
                    record.status_zendesk    = "solved"
                    record.status_openwrench = ow_status
                    record.last_sync_at      = datetime.now(timezone.utc)
                    try:
                        db.commit()
                    except Exception as commit_err:
                        db.rollback()
                        logger.error(f"[NOTES-SYNC] Failed to mark ZD ticket {zd_id} solved: {commit_err}")
                    logger.info(f"[NOTES-SYNC] OW WO {ow_id} is '{ow_status}' → ZD ticket {zd_id} marked solved.")

    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        logger.error(f"[NOTES-SYNC] Cycle failed: {e}")
    finally:
        db.close()
