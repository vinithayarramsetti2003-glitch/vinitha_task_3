import logging
import os
import re
import json
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient

# CONFIG via local.settings.json or Function App settings
BLOB_CONN = os.environ.get('BLOB_CONN')
COSMOS_URI = os.environ.get('COSMOS_URI')
COSMOS_KEY = os.environ.get('COSMOS_KEY')
COSMOS_DB = os.environ.get('COSMOS_DB', 'mydb')
COSMOS_CONTAINER = os.environ.get('COSMOS_CONTAINER', 'Documents')

_blob_service = None
_cosmos_client = None
_cosmos_container = None

def get_blob_service():
    global _blob_service
    if _blob_service is None:
        if not BLOB_CONN:
            raise RuntimeError("BLOB_CONN not set")
        _blob_service = BlobServiceClient.from_connection_string(BLOB_CONN)
    return _blob_service

def get_cosmos_container():
    global _cosmos_client, _cosmos_container
    if _cosmos_container is None:
        if not COSMOS_URI or not COSMOS_KEY:
            raise RuntimeError("COSMOS_URI or COSMOS_KEY missing")
        _cosmos_client = CosmosClient(COSMOS_URI, credential=COSMOS_KEY)
        _cosmos_container = _cosmos_client.get_database_client(COSMOS_DB).get_container_client(COSMOS_CONTAINER)
    return _cosmos_container

def is_text_blob(content_type, blob_name):
    if content_type:
        if content_type.startswith('text/'):
            return True
        if content_type in ('application/json',):
            return True
    lower = blob_name.lower()
    return lower.endswith(('.txt', '.md', '.csv', '.log', '.html', '.htm'))

def extract_title_and_wordcount(content_bytes, content_type, blob_name):
    try:
        text = content_bytes.decode('utf-8', errors='ignore')
    except Exception:
        return "", 0

    # HTML: first <h1>
    if (content_type and 'html' in content_type) or blob_name.lower().endswith(('.html', '.htm', '.md')):
        m = re.search(r'<h1[^>]*>(.*?)</h1>', text, re.IGNORECASE | re.DOTALL)
        if m:
            title = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', '', m.group(1))).strip()
        else:
            title = next((line.strip() for line in text.splitlines() if line.strip()), "")
    else:
        title = next((line.strip() for line in text.splitlines() if line.strip()), "")

    words = re.findall(r'\S+', text)
    word_count = len(words)
    return title, word_count

def parse_blob_url(blob_url):
    u = urlparse(blob_url)
    path = u.path.lstrip('/')
    parts = path.split('/', 1)
    container = parts[0]
    blob_name = parts[1] if len(parts) > 1 else ''
    return container, unquote(blob_name)

def safe_upsert_document(doc):
    cont = get_cosmos_container()
    cont.upsert_item(doc)

def main(event: func.EventGridEvent):
    logging.info('EventGrid event received')
    try:
        data = event.get_json()
        # Storage event structure: data.url contains the blob URL
        blob_url = data.get('url') or data.get('data', {}).get('url') or data.get('blobUrl')
        if not blob_url:
            logging.error("No blob URL in event data: %s", data)
            return

        container_name, blob_name = parse_blob_url(blob_url)
        logging.info("Blob created: %s/%s", container_name, blob_name)

        blob_service = get_blob_service()
        blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

        props = blob_client.get_blob_properties()
        content_type = props.content_settings.content_type if props.content_settings else None
        size = props.size
        url = blob_client.url

        title = ""
        word_count = 0
        if is_text_blob(content_type, blob_name):
            content = blob_client.download_blob().readall()
            title, word_count = extract_title_and_wordcount(content, content_type, blob_name)

        uploaded_on = datetime.now(timezone.utc).isoformat()
        doc = {
            "id": blob_name,
            "url": url,
            "container": container_name,
            "size": int(size) if size is not None else None,
            "contentType": content_type,
            "title": title,
            "wordCount": int(word_count),
            "uploadedOn": uploaded_on
        }

        safe_upsert_document(doc)
        logging.info("Indexed blob into Cosmos: id=%s", blob_name)

    except Exception:
        logging.exception("Error processing Event Grid blob event")
