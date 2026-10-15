
import logging
from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy.sql import func

import config
from models import WorkOrderSync, SyncState

logger = logging.getLogger("ow_client")


# ── Time helpers ─────────────────────────────────────────────────────────
def convert_ms_to_datetime_str(timestamp_ms: float) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    ) + "+00:00"


def convert_ms_to_datetime_obj(timestamp_ms: float):
    if not timestamp_ms:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)


def to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def now_iso_z() -> str:
    return to_iso_z(datetime.now(timezone.utc))


# ── Auth headers for each OpenWrench API family (see config.py notes) ──────
def _search_headers() -> dict:
    return {"Content-Type": "application/json", "x-auth-token": config.OW_AUTH_TOKEN}


def _lifecycle_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "X-API-KEY": config.OW_API_KEY,
        "OW-KEY": config.OW_OW_KEY,
    }


# ── Zendesk session / ticket creation ───────────────────────────────────────
def _make_zendesk_session() -> requests.Session:
    session = requests.Session()
    session.auth = (f"{config.ZD_EMAIL}/token", config.ZD_API_TOKEN)
    session.headers.update({"Content-Type": "application/json"})
    return session


def _raise_for_status_with_body(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error(f"[ZD] Error Response Body: {response.text}")
        raise e


def _safe_int_env(value, label: str):
    if not value:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(f"[CONFIG] {label} is set but not a valid integer: {value!r}. Ignoring it.")
        return None


def get_zendesk_org_by_email(email: str) -> int | None:

    if not email:
        return None
    url = f"{config.ZD_BASE_URL}/users/search.json"
    session = _make_zendesk_session()
    try:
        resp = session.get(url, params={"query": f"email:{email}"}, timeout=config.ZD_TIMEOUT)
        resp.raise_for_status()
        users = resp.json().get("users", [])
        if users and users[0].get("organization_id"):
            org_id = users[0]["organization_id"]
            logger.info(f"[ZD] Found organization_id={org_id} for email={email}")
            return org_id
        logger.info(f"[ZD] No organization linked for email={email}")
        return None
    except Exception as e:
        logger.warning(f"[ZD] Could not look up org for email={email}: {e}")
        return None


def create_ticket(
    subject: str,
    body: str,
    ow_id: int,
    priority: str = "normal",
    tags: list[str] | None = None,
    requester_email: str | None = None,
    requester_name: str | None = None,
    author_id: int | None = None,
) -> dict[str, Any]:
    url = f"{config.ZD_BASE_URL}/tickets"

    comment: dict[str, Any] = {"body": body}
    if author_id:
        comment["author_id"] = author_id

    all_tags = [config.OW_TAG, f"ow_wo_{ow_id}"] + (tags or [])

    p_lower = (priority or "normal").lower()
    zd_priority = p_lower if p_lower in ["low", "normal", "high", "urgent"] else "normal"

    ticket: dict[str, Any] = {
        "subject": subject,
        "comment": comment,
        "priority": zd_priority,
        "tags": all_tags,
        "external_id": str(ow_id),
    }

    org_custom_field: dict | None = None
    if requester_email:
        ticket["requester"] = {"email": requester_email, "name": requester_name or requester_email}
        # Look up the submitting user's organization and set it on the ticket.
        # "Organization" is a custom Lookup relationship field (field ID from
        # ZD_ORG_FIELD_ID, default 470XXXXXXXXXXXX) linked to the standard ZD
        # Organization object — so we pass the org ID as the custom field value.
        org_id = get_zendesk_org_by_email(requester_email)
        if org_id:
            ticket["organization_id"] = org_id   # standard ZD org (for routing/SLAs)
            _org_field_id = _safe_int_env(config.ZD_ORG_FIELD_ID, "ZD_ORG_FIELD_ID")
            if _org_field_id:
                org_custom_field = {"id": _org_field_id, "value": str(org_id)}

    # Build custom_fields list — includes OW work order ID field + org lookup field
    custom_fields: list = []
    if config.ZD_OW_FIELD_ID:
        field_id = _safe_int_env(config.ZD_OW_FIELD_ID, "ZD_OW_FIELD_ID")
        if field_id:
            custom_fields.append({"id": field_id, "value": str(ow_id)})

    if org_custom_field:
        custom_fields.append(org_custom_field)

    if custom_fields:
        ticket["custom_fields"] = custom_fields

    if config.ZD_TICKET_FORM_ID:
        form_id = _safe_int_env(config.ZD_TICKET_FORM_ID, "ZD_TICKET_FORM_ID")
        if form_id:
            ticket["ticket_form_id"] = form_id

    if config.ZD_GROUP_ID:
        group_id = _safe_int_env(config.ZD_GROUP_ID, "ZD_GROUP_ID")
        if group_id:
            ticket["group_id"] = group_id

    payload = {"ticket": ticket}
    logger.info(f"[ZD] Sending POST request to URL: {url}")

    session = _make_zendesk_session()
    resp = session.post(url, json=payload, timeout=config.ZD_TIMEOUT)
    _raise_for_status_with_body(resp)
    result = resp.json()["ticket"]
    logger.info(
        "[ZD] Ticket created  id=%s  status=%s  subject=%r  org=%s",
        result["id"], result["status"], result["subject"],
        result.get("organization_id"),
    )
    return result


def upload_file_to_zendesk(file_url: str, filename: str) -> str | None:

    # Step 1: Download the file from OW
    try:
        dl_resp = requests.get(file_url, timeout=config.OW_TIMEOUT)
        dl_resp.raise_for_status()
        file_content = dl_resp.content
        content_type = dl_resp.headers.get("Content-Type", "application/octet-stream")
    except Exception as e:
        logger.error(f"[ZD-UPLOAD] Failed to download file from OW URL {file_url}: {e}")
        return None

    # Step 2: Upload to Zendesk
    upload_url = f"{config.ZD_BASE_URL}/uploads.json?filename={filename}"
    session = _make_zendesk_session()
    # Override Content-Type for the upload request
    upload_headers = {"Content-Type": content_type}
    try:
        resp = session.post(
            upload_url,
            data=file_content,
            headers=upload_headers,
            timeout=config.ZD_TIMEOUT,
        )
        _raise_for_status_with_body(resp)
        token = resp.json().get("upload", {}).get("token")
        if not token:
            logger.error(f"[ZD-UPLOAD] No token in upload response: {resp.text}")
            return None
        logger.info(f"[ZD-UPLOAD] Uploaded '{filename}' → token: {token}")
        return token
    except Exception as e:
        logger.error(f"[ZD-UPLOAD] Failed to upload '{filename}' to Zendesk: {e}")
        return None


def update_zendesk_ticket(
    zd_ticket_id: int,
    comment_body: str | None = None,
    status: str | None = None,
    uploads: list[str] | None = None,   # list of ZD upload tokens for attachments
) -> dict[str, Any]:

    ticket_update: dict[str, Any] = {}

    if comment_body or uploads:
        comment: dict[str, Any] = {"body": comment_body or "", "public": True}
        if uploads:
            comment["uploads"] = uploads
        ticket_update["comment"] = comment

    if status:
        ticket_update["status"] = status.lower()

    if not ticket_update:
        return {"success": True, "comment_id": None}

    url = f"{config.ZD_BASE_URL}/tickets/{zd_ticket_id}"
    session = _make_zendesk_session()
    try:
        resp = session.put(url, json={"ticket": ticket_update}, timeout=config.ZD_TIMEOUT)
        _raise_for_status_with_body(resp)

        # Extract the ZD comment ID from the audit events in the response.
        comment_id = None
        if comment_body or uploads:
            try:
                body = resp.json()
                for event in body.get("audit", {}).get("events", []):
                    if event.get("type") == "Comment":
                        comment_id = event.get("id")
                        break
            except Exception:
                pass

        logger.info(
            f"[ZD] Ticket {zd_ticket_id} updated — comment={bool(comment_body)}, "
            f"uploads={len(uploads) if uploads else 0}, comment_id={comment_id}, status={status}"
        )
        return {"success": True, "comment_id": comment_id}
    except Exception as e:
        logger.error(f"[ZD] Failed to update ticket {zd_ticket_id}: {e}")
        return {"success": False, "comment_id": None}


# ── OpenWrench: search work orders (used by the hourly sync) ───────────────
def fetch_work_orders(offset: int = 0, limit: int = 100, since_created_at: str | None = None) -> dict[str, Any]:
    url = f"{config.OW_SEARCH_BASE_URL}/work_order/work_orders/search"
    params = {"offset": offset, "limit": limit, "sort_by": "createdAt", "order": "desc"}
    resp = requests.get(url, headers=_search_headers(), params=params, timeout=config.OW_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    since_dt = (
        datetime.fromisoformat(since_created_at.replace("Z", "+00:00"))
        if since_created_at else datetime.min.replace(tzinfo=timezone.utc)
    )

    filtered = []
    for i in data.get("data", []):
        # OpenWrench returns createdAt as epoch MILLISECONDS, not an ISO
        # string — must convert before comparing, else the since-filter
        # silently drops every record.
        t = i.get("source", {}).get("createdAt")
        if not t:
            continue
        created_dt = datetime.fromisoformat(convert_ms_to_datetime_str(t))
        if created_dt > since_dt:
            filtered.append(i)
        else:
            # results are sorted desc by createdAt, so once we hit one
            # that's <= the checkpoint, everything after it is older too.
            break

    data["data"] = filtered
    return data


# ── OpenWrench: lifecycle actions (used by the Zendesk webhook) ────────────
def fetch_work_order_details(ow_id: int) -> dict:
    url = f"{config.OW_LIFECYCLE_BASE_URL}/work_order/work_orders/{ow_id}"
    logger.info(f"[OW] Fetching Work Order details for ID: {ow_id}")
    resp = requests.get(url, headers=_lifecycle_headers(), timeout=config.OW_TIMEOUT)
    if resp.status_code != 200:
        logger.error(f"Work Order Fetch Failed: {resp.text}")
    resp.raise_for_status()

    data = resp.json().get("data", {})
    if not data:
        raise ValueError(f"Work Order {ow_id} fetch response has no 'data' field: {resp.text}")
    return data


def _extract_service_call_id(resp_json: dict, fallback: "int | None" = None) -> "int | None":

    if resp_json.get("id"):
        return resp_json["id"]

    data_field = resp_json.get("data") or {}

    # Shape: data IS the service call itself
    if data_field.get("id") and data_field.get("workOrderId"):
        return data_field["id"]

    # Shape: data is the work order, with the (just-created/updated) call
    # inside associatedServiceCalls — take the most recent non-deleted one.
    active_calls = [sc for sc in data_field.get("associatedServiceCalls", []) if not sc.get("isDeleted", False)]
    if active_calls:
        return active_calls[-1]["id"]

    # Shape: data has a convenience "lastServiceCall" pointer
    last_call = data_field.get("lastServiceCall")
    if last_call and last_call.get("id"):
        return last_call["id"]

    return fallback


def execute_tech_scheduled(ow_id: int, current_iso_time: str) -> int:
    """Reuses an existing (non-deleted) service call if one already exists on
    the work order, instead of creating a duplicate."""
    wo_data = fetch_work_order_details(ow_id)

    supplier_facility_id = wo_data.get("supplierFacilityId")
    if not supplier_facility_id or supplier_facility_id == -100:
        raise ValueError(
            f"Work Order {ow_id} has no valid supplierFacilityId (got {supplier_facility_id})."
        )

    active_calls = [sc for sc in wo_data.get("associatedServiceCalls", []) if not sc.get("isDeleted", False)]
    if active_calls:
        existing_sc_id = active_calls[-1]["id"]
        logger.info(f"[OW] Work Order {ow_id} already has a service call (ID: {existing_sc_id}); reusing it.")
        return existing_sc_id

    url = f"{config.OW_LIFECYCLE_BASE_URL}/work_order/service_calls/status_update/tech_scheduled"
    payload = {
        "workOrderId": int(ow_id),
        "supplierFacilityId": int(supplier_facility_id),
        "serviceScheduledAt": current_iso_time,
    }
    logger.info(f"[OW] Scheduling new Service Call — Work Order: {ow_id}, facility: {supplier_facility_id}")
    resp = requests.post(url, headers=_lifecycle_headers(), json=payload, timeout=config.OW_TIMEOUT)
    if resp.status_code != 200:
        logger.error(f"Tech Scheduled Failed Response Body: {resp.text}")
    resp.raise_for_status()

    service_call_id = _extract_service_call_id(resp.json())
    if not service_call_id:
        raise ValueError(f"tech_scheduled response has no service call id: {resp.text}")
    logger.info(f"[OW] Service Call Scheduled. ID: {service_call_id}")
    return service_call_id


def execute_remote_check_in(service_call_id: int, current_iso_time: str) -> int:
    url = f"{config.OW_LIFECYCLE_BASE_URL}/work_order/service_calls/status_update/remote_check_in"
    payload = {
        "id": int(service_call_id),
        "leadTechnicianEmail": config.OW_USER_EMAIL,
        "checkInByEmail": config.OW_USER_EMAIL,
        "checkInTime": current_iso_time,
        "numberOfTechs": 1,
    }
    logger.info(f"[OW] Executing Remote Check-In for Service Call ID: {service_call_id}")
    resp = requests.post(url, headers=_lifecycle_headers(), json=payload, timeout=config.OW_TIMEOUT)
    if resp.status_code != 200:
        logger.error(f"Check-In Failed Response Body: {resp.text}")
    resp.raise_for_status()

    updated_sc_id = _extract_service_call_id(resp.json(), fallback=service_call_id)
    logger.info(f"[OW] Check-In Success. Service Call ID: {updated_sc_id}")
    return updated_sc_id


def execute_remote_checkout(service_call_id: int, current_iso_time: str) -> None:
    url = f"{config.OW_LIFECYCLE_BASE_URL}/work_order/service_calls/status_update/remote_check_out"
    payload = {
        "id": int(service_call_id),
        "checkOutStatus": "WaitingForReview",
        "numberOfTechs": 1,
        "checkOutByEmail": config.OW_USER_EMAIL,
        "checkOutImages": [],
    }
    logger.info(f"[OW] Executing Remote Checkout for Service Call ID: {service_call_id}")
    resp = requests.post(url, headers=_lifecycle_headers(), json=payload, timeout=config.OW_TIMEOUT)
    if resp.status_code != 200:
        logger.error(f"Checkout Failed Response Body: {resp.text}")
    resp.raise_for_status()
    logger.info("[OW] Checkout Successfully Completed.")


def append_notes(ow_id: int, note_text: str, current_iso_time: str, photo_url: "str | None" = None) -> None:

    url = f"{config.OW_LIFECYCLE_BASE_URL}/work_order/work_orders/append_notes"
    note: dict = {"text": note_text, "noteAddedBy": config.OW_USER_EMAIL, "noteAddedAt": current_iso_time}
    if photo_url:
        note["photo"] = photo_url

    payload = {"id": int(ow_id), "note": note}
    resp = requests.patch(url, headers=_lifecycle_headers(), json=payload, timeout=config.OW_TIMEOUT)
    if resp.status_code != 200:
        logger.error(f"Append Notes Failed Response Body: {resp.text}")
    resp.raise_for_status()
    logger.info(f"[OW] Notes appended to Work Order {ow_id}" + (" (with photo)" if photo_url else ""))


def format_notes_for_ticket(notes: list) -> str:

    if not notes or not isinstance(notes, list):
        return ""

    lines = []
    for note in notes:
        if not isinstance(note, dict):
            continue
        text = (note.get("text") or "").strip()
        if not text:
            continue
        author = note.get("noteAddedBy") or "Unknown"
        added_at = note.get("noteAddedAt") or ""
        lines.append(f"- [{added_at}] {author}: {text}")

    if not lines:
        return ""

    return "Existing OpenWrench Notes:\n" + "\n".join(lines)


# ── Sync-state checkpoint helpers (dynamic since_created_at) ──────────────
SYNC_STATE_KEY = "openwrench_work_orders_last_created_at"


def get_last_synced_timestamp(db) -> str:

    state = db.query(SyncState).filter(SyncState.key == SYNC_STATE_KEY).first()
    if state and state.value:
        logger.info(f"[SYNC] Using checkpoint from sync_state: {state.value}")
        return state.value

    latest = db.query(func.max(WorkOrderSync.openwrench_created_at)).scalar()
    if latest:
        iso = to_iso_z(latest)
        logger.info(f"[SYNC] No checkpoint found, derived from existing records: {iso}")
        return iso

    from datetime import timedelta
    fallback_dt = datetime.now(timezone.utc) - timedelta(hours=config.INITIAL_LOOKBACK_HOURS)
    iso = to_iso_z(fallback_dt)
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
