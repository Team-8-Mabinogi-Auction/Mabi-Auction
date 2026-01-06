import os
import boto3
from dotenv import load_dotenv

# 1. 설정 로드
load_dotenv()

ACCESS_KEY = os.getenv('R2_ACCESS_KEY')
SECRET_KEY = os.getenv('R2_SECRET_KEY')
ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
BUCKET_NAME = os.getenv('R2_BUCKET_NAME')
LOCAL_FOLDER_PATH = 'D:/Users/Downloads/mabinogi_details_chunks/mabinogi_details_chunks'

def count_files():
    # ----------------------------------
    # 1. 로컬 파일 개수 세기
    # ----------------------------------
    print("🖥️  내 컴퓨터(로컬) 파일 세는 중...")
    local_count = 0
    for root, dirs, files in os.walk(LOCAL_FOLDER_PATH):
        local_count += len(files)
    
    print(f"   -> 로컬 파일: {local_count:,}개")

    # ----------------------------------
    # 2. R2 버킷 파일 개수 세기
    # ----------------------------------
    print("\n☁️  Cloudflare R2 버킷 스캔 중... (잠시만 기다려주세요)")
    
    session = boto3.session.Session()
    s3 = session.client('s3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    r2_count = 0
    paginator = s3.get_paginator('list_objects_v2')
    
    # R2는 한 번에 1000개씩만 알려주므로, 페이지를 넘기며 계속 세야 합니다.
    try:
        for page in paginator.paginate(Bucket=BUCKET_NAME):
            if 'Contents' in page:
                r2_count += len(page['Contents'])
                print(f"\r   ... 현재 {r2_count:,}개 발견", end="") # 진행 상황 표시
    except Exception as e:
        print(f"\n❌ 에러 발생: {e}")
        return

    print(f"\n   -> R2 버킷 파일: {r2_count:,}개")

    # ----------------------------------
    # 3. 결과 비교
    # ----------------------------------
    print("\n" + "="*30)
    if local_count == r2_count:
        print("✅ 완벽합니다! 모든 파일이 업로드되었습니다.")
    else:
        diff = local_count - r2_count
        print(f"⚠️ 개수가 다릅니다!")
        print(f"   - 차이: {abs(diff):,}개")
        if diff > 0:
            print(f"   - {diff:,}개의 파일이 아직 안 올라갔습니다. 업로드를 다시 돌려주세요.")
        else:
            print(f"   - R2에 파일이 더 많습니다. (이전 테스트 파일 등이 섞여 있을 수 있음)")
    print("="*30)

if __name__ == "__main__":
    count_files()