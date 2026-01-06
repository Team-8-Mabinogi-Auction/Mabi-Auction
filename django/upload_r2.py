import os
import sys
import boto3
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 1. .env 파일 로드
load_dotenv()

# 2. 환경변수 가져오기
ACCESS_KEY = os.getenv('R2_ACCESS_KEY')
SECRET_KEY = os.getenv('R2_SECRET_KEY')
ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
BUCKET_NAME = os.getenv('R2_BUCKET_NAME')
LOCAL_FOLDER_PATH = 'D:/Users/Downloads/mabinogi_details_chunks/mabinogi_details_chunks'

# [설정] 동시에 올릴 파일 개수 (너무 높으면 에러 날 수 있음. 30~50 추천)
MAX_WORKERS = 50

if not all([ACCESS_KEY, SECRET_KEY, ENDPOINT_URL, BUCKET_NAME]):
    print("❌ .env 설정 오류")
    sys.exit(1)

# S3 클라이언트 생성 (스레드에서 공유)
session = boto3.session.Session()
s3_client = session.client('s3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def upload_single_file(file_info):
    local_path, s3_path = file_info
    try:
        s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
        return True
    except Exception as e:
        return f"{s3_path} 실패: {str(e)}"

def upload_files_parallel():
    files_to_upload = []

    print("📂 파일 목록 스캔 중...")
    for root, dirs, files in os.walk(LOCAL_FOLDER_PATH):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, LOCAL_FOLDER_PATH)
            s3_path = relative_path.replace("\\", "/") # 윈도우 경로 보정
            
            files_to_upload.append((local_path, s3_path))

    total_files = len(files_to_upload)
    print(f"🚀 총 {total_files}개 파일 업로드 시작 (동시 작업: {MAX_WORKERS}개)")

    # 멀티스레드 업로드 시작
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 작업을 스레드풀에 등록
        futures = [executor.submit(upload_single_file, f) for f in files_to_upload]
        
        # tqdm으로 진행바 표시
        for future in tqdm(as_completed(futures), total=total_files, unit="file"):
            result = future.result()
            if result is not True:
                print(f"\n❌ 오류 발생: {result}")

    print("\n✅ 모든 업로드 작업 완료!")

if __name__ == "__main__":
    upload_files_parallel()