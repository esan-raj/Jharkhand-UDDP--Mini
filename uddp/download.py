import csv
import hashlib
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger("uddp")


@dataclass
class SourcePayload:
    rows: list
    fieldnames: list
    batch_path: Path
    source_url: str
    sha256: str
    acquisition_mode: str
    reference_period: str
    original_payload_path: Path | None
    byte_size: int


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = [name.strip() for name in (reader.fieldnames or [])]
        return list(reader), fieldnames


def _fetch(url, timeout=90):
    request = Request(url, headers={"User-Agent": "JharkhandMiniUDDP/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def download_file(url, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(_fetch(url))
    return destination


def download_pension(source, destination):
    """Save the NSAP resource. An API fallback also keeps the original JSON."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    json_path = destination.with_suffix(".json")
    try:
        payload = _fetch(source["direct_url"])
        if payload and not payload.lstrip().startswith(b"<"):
            destination.write_bytes(payload)
            logger.info("Saved NSAP file from the direct data.gov.in link")
            return destination, None, "download"
        logger.warning("Direct NSAP link did not return a data file. Using the OGD API.")
    except (HTTPError, URLError, TimeoutError) as exc:
        logger.warning("Direct NSAP download failed (%s). Using the OGD API.", exc)

    url = (
        f"{source['api_url']}?api-key={source['api_key']}"
        "&format=json&limit=1000&offset=0"
    )
    raw = _fetch(url)
    json_path.write_bytes(raw)
    payload = json.loads(raw.decode("utf-8"))
    columns = [(item["id"], item["name"]) for item in payload.get("field") or []]
    records = payload.get("records") or []
    if not columns or not records:
        raise RuntimeError("NSAP API returned no records")
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[name for _fid, name in columns])
        writer.writeheader()
        for record in records:
            row = {}
            for field_id, name in columns:
                value = record.get(field_id)
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                row[name] = "" if value is None else str(value)
            writer.writerow(row)
    logger.info("Saved %s NSAP rows from the OGD API", len(records))
    return destination, json_path, "api_fallback"


def _payload(path, source, mode, original_payload_path=None):
    rows, fieldnames = read_csv(path)
    return SourcePayload(
        rows=rows,
        fieldnames=fieldnames,
        batch_path=path,
        source_url=source.get("url") or source.get("direct_url") or source.get("page_url") or "",
        sha256=sha256_file(path),
        acquisition_mode=mode,
        reference_period=str(source.get("reference_period") or ""),
        original_payload_path=original_payload_path,
        byte_size=path.stat().st_size,
    )


def load_source_files(settings, batch_id, skip_download=False):
    """Read the three sources and keep a batch-specific copy of each payload."""
    raw_dir = settings["raw_dir"]
    raw_dir.mkdir(parents=True, exist_ok=True)
    batch_dir = raw_dir / "batches" / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    payloads = {}

    for key in ("demographic", "housing"):
        source = settings["sources"][key]
        bundled = raw_dir / source["filename"]
        batch_path = batch_dir / source["filename"]
        if skip_download:
            if not bundled.exists():
                raise FileNotFoundError(bundled)
            shutil.copyfile(bundled, batch_path)
            mode = "local_ingest"
        else:
            download_file(source["url"], batch_path)
            shutil.copyfile(batch_path, bundled)
            mode = "download"
        payloads[key] = _payload(batch_path, source, mode)

    pension = settings["sources"]["pension"]
    bundled_pension = raw_dir / pension["filename"]
    batch_pension = batch_dir / pension["filename"]
    original_json = None
    if skip_download:
        if not bundled_pension.exists():
            raise FileNotFoundError(bundled_pension)
        shutil.copyfile(bundled_pension, batch_pension)
        bundled_json = bundled_pension.with_suffix(".json")
        if bundled_json.exists():
            original_json = batch_dir / bundled_json.name
            shutil.copyfile(bundled_json, original_json)
        mode = "local_ingest"
    else:
        batch_pension, original_json, mode = download_pension(pension, batch_pension)
        shutil.copyfile(batch_pension, bundled_pension)
        if original_json is not None:
            shutil.copyfile(original_json, bundled_pension.with_suffix(".json"))
    payloads["pension"] = _payload(batch_pension, pension, mode, original_json)
    if mode == "api_fallback":
        payloads["pension"].source_url = pension.get("api_url") or payloads["pension"].source_url
    return payloads
