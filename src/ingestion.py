"""
ingestion.py

Handles consuming messages from Kafka and uploading raw batches to S3.
"""

import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Any

import boto3
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class KafkaToS3Ingester:
    """Consumes from a Kafka topic and flushes batches to S3 as newline-delimited JSON."""

    def __init__(self, bootstrap_servers: str, topic: str, consumer_group: str,
                 s3_bucket: str, s3_prefix: str, batch_size: int = 500):
        self.topic = topic
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.batch_size = batch_size

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=10_000,
        )
        self.s3 = boto3.client("s3")
        logger.info("KafkaToS3Ingester initialised for topic '%s'", topic)

    def _upload_batch(self, records: List[Dict[str, Any]]) -> str:
        ts = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        key = f"{self.s3_prefix}/{ts}/batch_{int(time.time())}.jsonl"
        body = "
".join(json.dumps(r) for r in records)
        self.s3.put_object(Bucket=self.s3_bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("Uploaded %d records to s3://%s/%s", len(records), self.s3_bucket, key)
        return key

    def run(self, max_batches: int = None) -> int:
        """Poll Kafka and flush to S3. Returns total records processed."""
        buffer: List[Dict] = []
        total = 0
        batches = 0

        for message in self.consumer:
            buffer.append(message.value)
            if len(buffer) >= self.batch_size:
                self._upload_batch(buffer)
                self.consumer.commit()
                total += len(buffer)
                batches += 1
                buffer = []
                if max_batches and batches >= max_batches:
                    break

        # flush remaining
        if buffer:
            self._upload_batch(buffer)
            self.consumer.commit()
            total += len(buffer)

        logger.info("Ingestion complete. Total records: %d", total)
        return total

    def close(self):
        self.consumer.close()
