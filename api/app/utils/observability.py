from prometheus_client import Counter, Gauge, start_http_server
import logging
import json
import sys
import threading

metrics = {
    "redis_messages_total": Counter(
        "redis_messages_total", "Total messages pushed to Redis stream"
    ),
    "redis_messages_ack": Counter(
        "redis_messages_ack", "Total messages acknowledged (ACK) by worker"
    ),
    "redis_messages_pending": Gauge(
        "redis_messages_pending", "Total messages still pending / not ACK"
    )
}

logger = logging.getLogger("app_logger")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def log_event(event: str, **kwargs):
    log_data = {
        "event": event,
        "fields": kwargs
    }
    logger.info(json.dumps(log_data))


def start_metrics_server(port: int = 9000):
    thread = threading.Thread(target=start_http_server, args=(port,), daemon=True)
    thread.start()
    log_event("metrics_server_started", port=port)

