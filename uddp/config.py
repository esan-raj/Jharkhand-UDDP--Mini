import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_settings(path=None):
    settings_path = Path(path) if path else ROOT / "config" / "settings.yaml"
    with settings_path.open(encoding="utf-8") as handle:
        settings = yaml.safe_load(handle)

    database = settings["database"]
    database["host"] = os.environ.get("UDDP_DB_HOST", database["host"])
    database["port"] = int(os.environ.get("UDDP_DB_PORT", database["port"]))
    database["name"] = os.environ.get("UDDP_DB_NAME", database["name"])
    database["user"] = os.environ.get("UDDP_DB_USER", database["user"])
    database["password"] = os.environ.get("UDDP_DB_PASSWORD", database["password"])

    webhook = os.environ.get("UDDP_ALERT_WEBHOOK_URL")
    if webhook:
        settings["pipeline"]["alert_webhook_url"] = webhook
    api_key = os.environ.get("UDDP_DATA_GOV_API_KEY")
    if api_key:
        settings["sources"]["pension"]["api_key"] = api_key

    settings["root"] = ROOT
    settings["raw_dir"] = ROOT / "data" / "raw"
    settings["log_dir"] = ROOT / "logs"
    settings["report_dir"] = ROOT / "reports"
    settings["sql_dir"] = ROOT / "sql"
    return settings


def database_url(settings):
    database = settings["database"]
    return (
        f"postgresql://{database['user']}:{database['password']}"
        f"@{database['host']}:{database['port']}/{database['name']}"
        "?connect_timeout=5"
    )
