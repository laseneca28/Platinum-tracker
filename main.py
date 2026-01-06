import pandas as pd
import requests
import io
import os
import re
import sys

# 1. 파일 다운로드 및 설정
url = "https://www.cmegroup.com/delivery_reports/PA-PL_Stck_Rprt.xls"
headers = {"User-Agent": "Mozilla/5.0"}

print("--- [1단계] 데이터 다운로드 시작 ---")
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    raw_data = response.content.decode('utf-8', errors='ignore')
    
    # 엔진 시도: HTML -> Excel 순서
    try:
        dfs = pd.read_html(io.StringIO(raw_data))
        df_raw = dfs[0]
        print("성공: HTML 형식으로 데이터를 읽었습니다.")
    except:
        df_raw = pd.read_excel(io.BytesIO(response.content), engine='xlrd')
        print("성공: Excel 엔진으로 데이터를 읽었습니다.")

except Exception as e:
    print(f"오류: 다운로드 또는 파싱 실패 - {e}")
    sys.exit(1)

# 2. 데이터 추출 로직
data_rows = []
activity_date = None
temp_depository = "" 
is_platinum = False

# 수치 데이터 클리닝 함수
def clean_val(x):
    s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
    try: return float(s)
    except: return 0.0

# [수정 1] "DEPOSITORY"를 제외 목록에서 제거 (창고 이름에 포함될 수 있으므로)
# "TOTAL"은 합계 행을 피하기 위해 유지
exclude_list = ["TOTAL", "TROY OUNCE", "REPORT DATE", "ACTIVITY DATE", "NAN", "NEW YORK", "COMEX"]

print("\n--- [2단계] 데이터 추출 및 창고명 탐색 시작 ---")

for index, row in df_raw.iterrows():
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0] # 첫 번째 컬럼 값

    # A. 날짜 추출
    if not activity_date:
        combined_row = " ".join(vals)
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            activity_date = match.group(1)
            print(f"-> 기준 날짜 확인: {activity_date}")

    # B. PLATINUM 섹션 확인
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        print("-> PLATINUM 섹션 진입")
        continue
    if "PALLADIUM" in first_val.upper():
        print("-> PALLADIUM 섹션 도달 (종료)")
        break 

    if not is_platinum:
        continue

    # [수정 2] 찐 헤더인 "DEPOSITORY" 단어만 정확히 일치할 때 건너뛰기
    if first_val.upper() == "DEPOSITORY":
        continue

    # C. 데이터 추출 로직
    # 1) 'Registered' 또는 'Eligible' 행 처리
    if first_val in ["Registered", "Eligible"]:
        if not temp_depository:
            # 창고명이 아직 없으면 스킵
            continue
            
        try:
            data_rows.append({
                'Date': activity_date,
                'Region_Type': f"{temp_depository} {first_val}",
                'PREV_TOTAL': clean_val(row.iloc[2]),
                'RECEIVED': clean_val(row.iloc[3]),
                'WITHDRAWN': clean_val(row.iloc[4]),
                'NET_CHANGE': clean_val(row.iloc[5]),
                'ADJUSTMENT': clean_val(row.iloc[6]),
                'TOTAL_TODAY': clean_val(row.iloc[7])
            })
        except:
            continue

    # 2) 창고명(Depository) 찾기
    # 조건: 'nan' 아님, 3글자 이상, 제외키워드 없음, 'Registered' 아님
    elif first_val != "nan" and len(first_val) > 3:
        # 제외 키워드(exclude_list)에 포함된 단어가 있으면 스킵
        if not any(k in first_val.upper() for k in exclude_list):
            # 숫자가 포함되지 않은 텍스트를 창고명으로 간주
            if not any(char.isdigit() for char in first_val):
                temp_depository = first_val
                print(f"-> [창고 발견] {temp_depository}")

# 3. 저장 로직
print("\n--- [3단계] 파일 저장 시도 ---")
file_name = 'platinum_daily_stock.csv'

if data_rows:
    new_df = pd.DataFrame(data_rows)
    
    # 합계(TOTAL) 행 제거 (대소문자 무시 옵션 추가하여 오류 방지)
    new_df = new_df[~new_df['Region_Type'].str.contains("TOTAL", case=False, na=False)]
    
    print(f"-> 추출된 데이터: 총 {len(new_df)}행")

    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        if str(activity_date) in existing_df['Date'].astype(str).values:
            print(f"알림: {activity_date} 데이터는 이미 파일에 존재합니다. 저장을 건너뜁니다.")
        else:
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
            print(f"성공: 기존 파일에 {activity_date} 데이터를 추가했습니다.")
    else:
        new_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"성공: 새 파일({file_name})을 생성했습니다.")
        
    # 결과 미리보기
    if not new_df.empty:
        print("\n[저장된 데이터 예시]")
        print(new_df[['Date', 'Region_Type', 'TOTAL_TODAY']].head())
else:
    print("실패: 유효한 데이터를 하나도 찾지 못했습니다.")
    print("-> 원인: 엑셀 파일 형식이 변경되었거나, 창고 이름을 인식하지 못했습니다.")
