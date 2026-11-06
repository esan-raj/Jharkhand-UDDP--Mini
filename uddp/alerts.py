import json
import logging
from datetime import datetime, timezone
from urllib import request

logger = logging.getLogger("uddp")


def send_alert(settings, message, severity="ERROR", batch_id=None, conn=None, commit=False):
    """Failure hook. Always logs locally. Posts to a webhook only when one is configured."""
    level = logging.ERROR if severity == "ERROR" else logging.WARNING
    logger.log(level, "ALERT %s", message)
    settings["log_dir"].mkdir(parents=True, exist_ok=True)
    event = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "severity": severity,
        "message": message,
    }
    with (settings["log_dir"] / "alerts.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")

    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.alert_event (batch_id, severity, message)
                VALUES (%s, %s, %s)
                """,
                (batch_id, severity, message),
            )
        if commit:
            conn.commit()

    webhook = (settings.get("pipeline") or {}).get("alert_webhook_url") or ""
    if webhook:
        payload = json.dumps(event).encode("utf-8")
        req = request.Request(
            webhook,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with request.urlopen(req, timeout=15) as response:
            response.read()
