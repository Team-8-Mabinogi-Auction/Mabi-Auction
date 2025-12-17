import os
import json
import logging
import gzip
from datetime import datetime, timezone

from kafka_auction.config.settings import KAFKA_BOOTSTRAP_SERVERS, LOG_LEVEL,TOPIC_LISTINGS_EQUIPMENTS

import boto3
from kafka import KafkaConsumer

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def safe_cycle_id(cycle_id: str) -> str:
    """
    cycle_id가 "2025-12-15T01:23:45+00:00" 같은 형태여도
    윈도우 파일명에 안전하도록 치환.
    """
    return (
        str(cycle_id)
        .replace(":", "")
        .replace("+", "p")
        .replace("-", "")
        .replace(".", "")
        .replace("Z", "")
    )

def main():
    
    GROUP_ID = os.getenv("KAFKA_GROUP_ID", "auction-listings-to-s3")
    BUCKET = os.getenv("S3_BUCKET", "mabinogi-auction-test")
    PREFIX = os.getenv("S3_PREFIX", "raw/listings")

    SPOOL_DIR = os.getenv("SPOOL_DIR", "./spool")
    os.makedirs(SPOOL_DIR, exist_ok=True)

    # # (안전장치) 사이클이 너무 커져서 메모리를 먹는 걸 방지
    # MAX_RECORDS_PER_CYCLE = int(os.getenv("MAX_RECORDS_PER_CYCLE", "300000"))

    s3 = boto3.client("s3")

    consumer = KafkaConsumer(
        TOPIC_LISTINGS_EQUIPMENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # ✅ S3 업로드 성공 후에만 커밋
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    current_cycle_id = None
    current_file_handle = None
    current_local_path = None
    record_count = 0
    cycle_started_at = None

    def open_cycle_file(cycle_id: str):
        """새 사이클 파일을 열고 파일 핸들 반환"""
        cid_safe = safe_cycle_id(cycle_id)
        fname = f"{TOPIC_LISTINGS_EQUIPMENTS}.cycle={cid_safe}.jsonl"
        local_path = os.path.join(SPOOL_DIR, fname)
        return open(local_path, "w", encoding="utf-8"), local_path

    def get_time_partition_path(dt: datetime) -> str:
        """시간 기반 파티션 경로 생성 (year=YYYY/month=MM/day=DD/hour=HH/minute=MM)"""
        return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/minute={dt.minute:02d}"

    def close_and_upload():
        """현재 파일을 닫고 gzip 압축 후 S3에 업로드"""
        nonlocal current_file_handle, current_local_path, current_cycle_id, record_count, cycle_started_at
        
        if current_file_handle is None or current_local_path is None:
            return False
        
        # 파일 닫기
        current_file_handle.close()
        current_file_handle = None
        
        # 원본 파일 크기 계산
        original_size_bytes = os.path.getsize(current_local_path)
        original_size_mb = original_size_bytes / (1024 * 1024)
        
        # cycle_id에서 시간 파싱 (ISO 형식: 2025-12-15T08:00:00.123456+00:00)
        try:
            cycle_dt = datetime.fromisoformat(current_cycle_id.replace('Z', '+00:00'))
            if cycle_dt.tzinfo is None:
                cycle_dt = cycle_dt.replace(tzinfo=timezone.utc)
        except Exception:
            # 파싱 실패 시 현재 시간 사용
            cycle_dt = datetime.now(timezone.utc)
        
        # 시간 기반 파티션 경로 생성
        time_partition = get_time_partition_path(cycle_dt)
        
        # gzip 압축
        compressed_path = current_local_path + ".gz"
        with open(current_local_path, "rb") as f_in:
            with gzip.open(compressed_path, "wb") as f_out:
                f_out.writelines(f_in)
        
        # 압축 파일 크기 계산
        compressed_size_bytes = os.path.getsize(compressed_path)
        compressed_size_mb = compressed_size_bytes / (1024 * 1024)
        compression_ratio = (1 - compressed_size_bytes / original_size_bytes) * 100 if original_size_bytes > 0 else 0
        
        # 경과 시간 계산
        elapsed_time = None
        elapsed_seconds = None
        if cycle_started_at is not None:
            cycle_ended_at = datetime.now(timezone.utc)
            elapsed_time = cycle_ended_at - cycle_started_at
            elapsed_seconds = elapsed_time.total_seconds()
        
        # S3 업로드 (압축 파일, 시간 파티션 경로 포함)
        filename = os.path.basename(compressed_path)
        s3_key = f"{PREFIX}/{time_partition}/{filename}"
        upload_start = datetime.now(timezone.utc)
        s3.upload_file(compressed_path, BUCKET, s3_key)
        upload_elapsed = (datetime.now(timezone.utc) - upload_start).total_seconds()
        
        logger.info(
            "Cycle completed and uploaded. cycle_id=%s, total_records=%d, "
            "original_size=%.2f MB, compressed_size=%.2f MB (%.1f%% reduction), "
            "processing_time=%.2f seconds (%.2f minutes), upload_time=%.2f seconds, s3_key=%s",
            current_cycle_id,
            record_count,
            original_size_mb,
            compressed_size_mb,
            compression_ratio,
            elapsed_seconds if elapsed_seconds is not None else 0,
            (elapsed_seconds / 60.0) if elapsed_seconds is not None else 0,
            upload_elapsed,
            f"s3://{BUCKET}/{s3_key}",
        )
        
        # 로컬 파일 삭제 (원본 + 압축 파일)
        os.remove(current_local_path)
        os.remove(compressed_path)
        current_local_path = None
        return True

    try:
        for msg in consumer:
            event = json.loads(msg.value)
            event_type = event.get("type")

            # LISTING 수집
            if event_type == "LISTING":
                cycle_id = event.get("cycle_id")
                if cycle_id is None:
                    # cycle_id 없는 LISTING은 버리거나 별도 처리(여기선 스킵)
                    continue

                # 새 사이클이면 파일 열기
                if current_cycle_id is None or current_cycle_id != cycle_id:
                    # 이전 사이클 파일이 열려있으면 닫고 업로드
                    if current_file_handle is not None:
                        close_and_upload()
                        consumer.commit()
                    
                    current_cycle_id = cycle_id
                    current_file_handle, current_local_path = open_cycle_file(cycle_id)
                    record_count = 0
                    cycle_started_at = datetime.now(timezone.utc)
                    
                    logger.info(
                        "Starting new cycle. cycle_id=%s, file=%s",
                        cycle_id,
                        os.path.basename(current_local_path),
                    )

                # 메시지를 즉시 파일에 append (메모리 절감)
                current_file_handle.write(msg.value + "\n")
                record_count += 1
                
                # 주기적으로 진행 상황 로그 (1000개마다)
                if record_count % 1000 == 0:
                    elapsed = (datetime.now(timezone.utc) - cycle_started_at).total_seconds() if cycle_started_at else 0
                    logger.info(
                        "Processing cycle_id=%s: records_processed=%d, elapsed_time=%.2f seconds",
                        current_cycle_id,
                        record_count,
                        elapsed,
                    )

            # END_OF_CYCLE 도착 -> 파일 닫고 업로드
            elif event_type == "END_OF_CYCLE":
                # 파일이 열려있으면 닫고 업로드
                if close_and_upload():
                    # 업로드 성공 후에만 커밋
                    consumer.commit()
                
                current_cycle_id = None
                record_count = 0

            else:
                # 다른 타입은 필요 시 처리
                pass

    finally:
        # 마지막으로 열려있는 파일이 있으면 닫고 업로드
        if current_file_handle is not None:
            try:
                close_and_upload()
                consumer.commit()
            except Exception as e:
                print(f"Error during final cleanup: {e}")
        consumer.close()


if __name__ == "__main__":
    main()
