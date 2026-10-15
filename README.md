# OpenWrench <-> Zendesk Bidirectional Integration

A robust, custom bidirectional synchronization service between **OpenWrench** (field-service platform) and **Zendesk** (support ticketing platform). Built with Python (FastAPI), PostgreSQL, and APScheduler, this integration ensures seamless real-time and scheduled data alignment across both platforms without manual intervention.

---

## 📌 Features

* **Bidirectional Syncing:** Automatically creates Zendesk tickets from OpenWrench work orders and pushes Zendesk comments/status back to OpenWrench.
* **Scheduled Operations:** 
  * Hourly check for new OpenWrench work orders.
  * Frequent polling (every 15–30 mins) for notes, attachments, and status updates.
* **Real-time Webhook Handling:** Instant processing of Zendesk actions (agent comments, status changes, and attachments).
* **Built-in Admin Dashboard:** Monitoring UI to view sync logs, trigger manual sync runs, and check webhook events.
* **Deduplication & Safety:** Tracks synced event UUIDs and note keys to prevent duplicate entries across systems.

---

## 🛠️ Tech Stack

* **Language:** Python 3.11+
* **Framework:** FastAPI (with Uvicorn)
* **Database:** PostgreSQL (SQLAlchemy ORM + `psycopg2`)
* **Scheduler:** APScheduler (Blocking Scheduler with Thread Pool)
* **HTTP Client:** `requests`
* **Deployment target:** AWS EC2 (Recommended: Ubuntu 22.04 LTS, `t3.small`)

---

## 📁 Project Structure

```text
ow_zendesk_sync/
├── main.py                  # FastAPI app entry point, scheduler startup, webhook routes
├── config.py                # Configuration & environment variables loader
├── database.py              # SQLAlchemy engine, session factory, auto-migrations
├── models.py                # Database ORM models/table definitions
├── ow_client.py             # API clients for OpenWrench and Zendesk
├── sync_engine.py           # Background sync jobs (tickets, notes, status)
├── webhook_handler.py       # Zendesk incoming webhook logic
├── templates/
│   └── dashboard.html       # Admin monitoring Web UI
└── .env                     # Configuration secrets (DO NOT COMMIT)