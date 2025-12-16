from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time 
import os
import pandas as pd 
import requests 
import re 

# --- 사용자 설정 변수 ---
# 1. 입력/출력 파일 및 범위 설정
INPUT_FILENAME = "name_link.csv" 
# ⭐️ CSV 파일을 저장할 폴더 이름 ⭐️
OUTPUT_DIR = "mabinogi_details_chunks" 

# ⭐️ 썸네일 이미지 저장 폴더 (기존 이미지 경로) ⭐️
THUMBNAIL_IMAGE_DIR = os.path.join(OUTPUT_DIR, "thumbnail_images") 
# ⭐️ 인게임 아이템 이미지 저장 폴더 (새로 추가) ⭐️
ITEM_IMAGE_DIR = os.path.join(OUTPUT_DIR, "item_images") 

# ⭐️ 크롤링할 시작 행 (0부터 시작)
START_ROW = 0 
# ⭐️ 크롤링할 끝 행 (포함되지 않음, None이면 끝까지)
END_ROW = 20798
# ⭐️ 청크 크기 설정 (1000개 단위로 분할 저장) ⭐️
CHUNK_SIZE = 1000 

# 2. 상세 페이지 XPath 설정
ITEM_NAME_DETAIL_XPATH = '/html/body/div/main/section/div/div/section/h1'
# 썸네일 이미지 XPath (기존)
ITEM_IMAGE_THUMBNAIL_XPATH = '/html/body/div/main/section/div/div/section/div[1]/div[1]/img' 
# ⭐️ 인게임 아이템 이미지 XPath (새로 추가) ⭐️
ITEM_IMAGE_INGAME_XPATH = '/html/body/div/main/section/div/div/section/div[6]/div/div/img'

# ⭐️ 설명 추출 XPath 패턴 목록 (가장 마지막 요소가 <p> 태그) ⭐️
DESCRIPTION_PATTERNS = [
    '/html/body/div/main/section/div/div/section/div[1]/div[2]/div[2]/p',
    '/html/body/div/main/section/div/div/section/div[1]/div[2]/div[2]/p[1]',
    '/html/body/div/main/section/div/div/section/div[1]/div[2]/div/p',
    '/html/body/div/main/section/div/div/section/div[1]/div[2]/div/p[1]',
    '/html/body/div/main/section/div/div/section/div[1]/div/p',
]

# 3. 로딩 대기 시간
TIMEOUT = 10 

# --- 함수 정의 ---

def clean_description(text):
    """설명 텍스트에서 줄바꿈과 불필요한 공백을 제거하고 클렌징"""
    if not text:
        return ""
    text = text.replace('\n', ' ').replace('\r', ' ').strip()
    return ' '.join(text.split())

def clean_filename(name):
    """파일 이름으로 사용할 수 없는 문자를 제거하고 클렌징"""
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    return name[:150].strip()

def download_image(image_url, item_name, image_dir_path):
    """
    이미지를 다운로드하고 로컬 경로를 반환하는 함수 (실패 시 'NULL' 반환)
    """
    if not image_url or "N/A" in image_url:
        return "NULL"

    try:
        # 파일 이름 생성 및 경로 정의
        ext = os.path.splitext(image_url.split('?')[0])[-1] or '.png'
        if not ext.startswith('.'):
             ext = '.png'
             
        safe_name = clean_filename(item_name)
        filename = f"{safe_name}{ext}"
        save_path = os.path.join(image_dir_path, filename)

        # 이미지 다운로드
        response = requests.get(image_url, stream=True, timeout=10)
        response.raise_for_status() 

        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        # 저장 성공 시 로컬 경로 반환 (상대 경로: 폴더명/파일명)
        return os.path.join(os.path.basename(image_dir_path), filename).replace('\\', '/') 
        
    except requests.exceptions.RequestException:
        return "NULL"
    except Exception:
        return "NULL"


def crawl_details(driver, item_link, original_name):
    """
    단일 아이템 링크에 접속하여 상세 정보를 추출하고 이미지를 다운로드하는 함수
    """
    description = "NULL"
    image_url_thumbnail = "N/A"
    image_path_thumbnail = "NULL"
    image_url_ingame = "N/A"
    image_path_ingame = "NULL" 
    detail_item_name = original_name
    
    try:
        # 상세 페이지 이동
        driver.get(item_link)
        time.sleep(1) 

        # 1. 아이템명 추출 (로딩 대기 기준)
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, ITEM_NAME_DETAIL_XPATH))
        )
        
        name_element = driver.find_element(By.XPATH, ITEM_NAME_DETAIL_XPATH)
        detail_item_name = name_element.text.strip()
        
        # 2. 썸네일 이미지 추출 및 다운로드
        try:
            img_element = driver.find_element(By.XPATH, ITEM_IMAGE_THUMBNAIL_XPATH)
            image_url_thumbnail = img_element.get_attribute('src')
        except NoSuchElementException:
            image_url_thumbnail = "N/A"
            
        image_path_thumbnail = download_image(image_url_thumbnail, detail_item_name, THUMBNAIL_IMAGE_DIR)

        # 3. 인게임 이미지 추출 및 다운로드
        try:
            img_element_ingame = driver.find_element(By.XPATH, ITEM_IMAGE_INGAME_XPATH)
            image_url_ingame = img_element_ingame.get_attribute('src')
        except NoSuchElementException:
            image_url_ingame = "N/A" 

        image_path_ingame = download_image(image_url_ingame, detail_item_name, ITEM_IMAGE_DIR)
            
        # 4. 설명 추출 로직
        raw_description_parts = []
        for xpath_pattern in DESCRIPTION_PATTERNS:
            try:
                p_elements = driver.find_elements(By.XPATH, xpath_pattern)
                for p_element in p_elements:
                    text = p_element.text.strip()
                    if text and text not in raw_description_parts:
                        raw_description_parts.append(text)
                
                if raw_description_parts:
                    break 
            except Exception:
                continue
        
        if raw_description_parts:
            raw_description = "\n".join(raw_description_parts)
            cleaned_desc = clean_description(raw_description)
            description = cleaned_desc if cleaned_desc else "NULL"
        else:
            description = "NULL"


    except TimeoutException:
        print(f"   [상세 오류] {original_name} 로딩 시간 시간 초과. 스킵.")
    except Exception as e:
        print(f"   [상세 오류] {original_name} 크롤링 중 예상치 못한 오류: {e}")
        description = "NULL" 

    return {
        '아이템명': detail_item_name,
        '아이템설명': description,
        '아이템이미지_썸네일': image_path_thumbnail,      
        '아이템이미지_인게임': image_path_ingame,         
        '아이템링크': item_link
    }

def save_chunk_to_csv(chunk_data, start_idx, end_idx, output_dir):
    """
    청크 데이터를 CSV 파일로 저장하는 함수
    """
    if not chunk_data:
        return
        
    df_output = pd.DataFrame(chunk_data)
    
    filename = f"details_{start_idx}_{end_idx}.csv"
    output_path = os.path.join(output_dir, filename)
    
    # CSV 컬럼 순서
    df_output.to_csv(output_path, index=False, encoding='utf-8-sig', 
                      columns=['아이템명', '아이템설명', '아이템이미지_썸네일', '아이템이미지_인게임', '아이템링크'])
    
    print(f"✅ 저장 완료: {output_path} (총 {len(chunk_data)}개 항목)")


# --- 메인 실행 블록 ---

if __name__ == "__main__":
    driver = None
    total_start_time = time.time() 
    
    # 1. 입력 파일 로드
    if not os.path.exists(INPUT_FILENAME):
        print(f"🚨 오류: 입력 파일 '{INPUT_FILENAME}'을 찾을 수 없습니다. 크롤링을 중단합니다.")
        exit()
        
    try:
        df_input = pd.read_csv(INPUT_FILENAME, encoding='utf-8-sig')
    except Exception as e:
        print(f"🚨 오류: CSV 파일을 읽는 중 오류 발생: {e}")
        exit()

    # 2. 크롤링 범위 지정
    df_range = df_input.iloc[START_ROW:END_ROW].reset_index(drop=True)
    total_items = len(df_range)
    
    # 3. 출력 폴더 및 이미지 폴더 준비
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(THUMBNAIL_IMAGE_DIR, exist_ok=True) 
    os.makedirs(ITEM_IMAGE_DIR, exist_ok=True)      
    
    # 4. 정보 출력
    actual_end_row_index = START_ROW + total_items - 1 
    print(f"\n--- 1. 크롤링 준비 완료 ---")
    print(f"입력 파일: {INPUT_FILENAME}")
    print(f"출력 폴더: {OUTPUT_DIR}")
    print(f"썸네일 저장 폴더: {THUMBNAIL_IMAGE_DIR}")
    print(f"인게임 이미지 저장 폴더: {ITEM_IMAGE_DIR}")
    print(f"크롤링 범위: {START_ROW}행부터 {actual_end_row_index}행까지 ({total_items}개 항목)")
    print(f"분할 저장 단위: {CHUNK_SIZE}개")

    try:
        if total_items == 0:
            print("\n🛑 크롤링할 항목이 없습니다 (시작/끝 행 범위 오류).")
            raise Exception("크롤링할 항목 없음")

        print("\n--- 2. WebDriver 초기화 및 상세 크롤링 시작 ---")
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
        total_saved_items = 0

        # 청크 단위로 분할하여 반복
        for i in range(0, total_items, CHUNK_SIZE):
            chunk_start_time = time.time() 
            
            chunk_start_idx_local = i
            chunk_end_idx_local = min(i + CHUNK_SIZE, total_items)
            
            chunk_start_idx_global = START_ROW + chunk_start_idx_local
            chunk_end_idx_global = START_ROW + chunk_end_idx_local - 1 

            current_chunk = df_range.iloc[chunk_start_idx_local:chunk_end_idx_local]
            chunk_results = []
            
            print(f"\n=== 📦 청크 크롤링 시작: {chunk_start_idx_global}행 ~ {chunk_end_idx_global}행 (총 {len(current_chunk)}개) ===")

            for index, row in current_chunk.iterrows():
                item_name = row.iloc[0] 
                item_link = row.iloc[1] 
                
                current_idx = index - chunk_start_idx_local + 1
                global_idx = chunk_start_idx_global + current_idx - 1
                
                if current_idx % 20 == 0 or current_idx == 1 or current_idx == len(current_chunk):
                    print(f"   [진행] {current_idx}/{len(current_chunk)} (전체 {global_idx + 1}/{START_ROW + total_items}) - {item_name}...")
                
                # 상세 정보 추출 및 이미지 다운로드
                details = crawl_details(driver, item_link, item_name)
                chunk_results.append(details)
            
            # 청크 결과 저장
            save_chunk_to_csv(chunk_results, chunk_start_idx_global, chunk_end_idx_global, OUTPUT_DIR)
            total_saved_items += len(chunk_results)
            
            # 청크 소요 시간 계산 및 출력 
            chunk_end_time = time.time()
            time_taken = chunk_end_time - chunk_start_time
            time_per_item = time_taken / len(current_chunk) if len(current_chunk) > 0 else 0
            
            print(f"⏱️ 청크 소요 시간: {time_taken:.2f}초 (항목당 평균 {time_per_item:.2f}초)")

    except WebDriverException as e:
        print(f"\n🚨 최종 실행 오류 (WebDriver): 드라이버 초기화 또는 실행 문제. {e}")
    except Exception as e:
        print(f"🚨 최종 실행 오류: {e}")
    
    finally:
        total_end_time = time.time()
        total_time_taken = total_end_time - total_start_time
        
        print(f"\n=============================================")
        print(f"🎉 최종 완료! 총 {total_saved_items}개의 항목을 수집하고 이미지를 다운로드했습니다.")
        print(f"크롤링 범위: {START_ROW}행부터 {START_ROW + total_items - 1}행까지.")
        print(f"⏰ 전체 작업 소요 시간: {total_time_taken:.2f}초") 
        if driver:
            driver.quit()