# kafka_producer.py
import json
import logging
from typing import Any, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import KAFKA_BOOTSTRAP_SERVERS, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def _value_serializer(value: Dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def _key_serializer(key: Optional[str]) -> Optional[bytes]:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key
    return str(key).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    value_serializer=_value_serializer,
    key_serializer=_key_serializer,
    acks="all",
    linger_ms=10,
    retries=5,
)


def _on_send_success(record_metadata):
    logger.debug(
        "Message sent: topic=%s partition=%s offset=%s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def _on_send_error(excp: KafkaError):
    logger.error("Failed to send message to Kafka", exc_info=excp)


def send_message(
    topic: str,
    value: Dict[str, Any],
    key: Optional[str] = None,
    timestamp_ms: Optional[int] = None,
    flush: bool = False,
) -> None:
    """
    Kafka로 메시지 전송.
    - topic: 토픽 이름
    - key: 파티션 결정용 key (없으면 None)
    - value: 실제 payload (dict → JSON)
    - timestamp_ms: Kafka 레코드 timestamp (S3 time partition 등에 사용)
    - flush=True: 호출 후 즉시 flush
    """
    future = producer.send(topic, key=key, value=value, timestamp_ms=timestamp_ms)
    future.add_callback(_on_send_success)
    future.add_errback(_on_send_error)

    if flush:
        producer.flush()
