import pandas as pd
import requests
import io
import os
import re
import sys

# 1. 파일 다운로드
url = "https://www.cmegroup.com/delivery_reports/PA-PL_Stck_Rprt.xls"
headers = {"User-Agent": "Mozilla/5.0"}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    try:
        dfs = pd.read_html(io.BytesIO(response.content))
        df_raw = dfs[0]
    except:
        df_raw = pd.read_excel(io.BytesIO(response.content))
except Exception as e:
    print(f"오류: {e}")
    sys.exit(1)

# 2. 데이터 추출 로직
data_rows = []
activity_date = None
current_depository = "Unknown"
is_platinum = False

# 행 순회
for _, row in df_raw.iterrows():
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0] if vals else "nan"

    # A. Activity Date 찾기 (리포트 상단의 Activity Date: MM/DD/YYYY 패턴)
    if not activity_date:
        combined_row = " ".join(vals)
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            activity_date = match.group(1)
            print(f"확인된 Activity Date: {activity_date}")

    # B. PLATINUM 섹션 범위 지정
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        continue
    if "PALLADIUM" in first_val.upper():
        is_platinum = False
        break

    if not is_platinum:
        continue

    # C. 데이터 필터링 및 추출
    exclude_keywords = ["TOTAL REGISTERED", "TOTAL ELIGIBLE", "TOTAL TODAY", "TROY OUNCE", "DEPOSITORY", "TOTAL"]
    
    # Registered 또는 Eligible 행 처리
    if first_val in ["Registered", "Eligible"]:
        # 합계(Total)가 포함된 행은 제외 (한 번 더 검증)
        if "TOTAL" in first_val.upper():
            continue
            
        try:
            def clean_val(x):
                s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
                return float(s) if s else 0.0

            data_rows.append({
                'Date': activity_date,
                'Region_Type': f"{current_depository} {first_val}",
                'PREV_TOTAL': clean_val(row.iloc[2]),
                'RECEIVED': clean_val(row.iloc[3]),
                'WITHDRAWN': clean_val(row.iloc[4]),
                'NET_CHANGE': clean_val(row.iloc[5]),
                'ADJUSTMENT': clean_val(row.iloc[6]),
                'TOTAL_TODAY': clean_val(row.iloc[7])
            })
        except:
            continue
    
    # 창고명 업데이트 (숫자가 아니고 유효한 텍스트일 때)
    elif first_val != "nan" and len(first_val) > 2 and not any(k in first_val.upper() for k in exclude_keywords):
        current_depository = first_val

# 3. 중복 체크 및 파일 저장
file_name = 'platinum_daily_stock.csv'

# (main.py의 3번 저장 부분만 아래로 교체해 보세요)
if data_rows:
    new_df = pd.DataFrame(data_rows)
    file_name = 'platinum_daily_stock.csv'
    
    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        # 테스트를 위해 중복 체크를 하지 않고 일단 합쳐봅니다.
        final_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(subset=['Date', 'Region_Type'], keep='last')
    else:
        final_df = new_df
        
    final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"성공: {activity_date} 데이터 저장 완료")
else:
    print("데이터를 추출하지 못했습니다.")
    sys.exit(1)
