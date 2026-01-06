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
    
    # 인코딩 오류 방지를 위해 바이너리 데이터를 직접 처리
    content = response.content
    
    try:
        # 시도 1: HTML 파싱 (CME 리포트의 흔한 형식)
        dfs = pd.read_html(io.BytesIO(content))
        df_raw = dfs[0]
    except Exception as e:
        print(f"HTML 파싱 실패, Excel 엔진으로 재시도: {e}")
        # 시도 2: 실제 구형 Excel(.xls) 형식일 경우
        df_raw = pd.read_excel(io.BytesIO(content), engine='xlrd')
        
except Exception as e:
    print(f"다운로드 또는 파싱 오류: {e}")
    sys.exit(1)

# 2. 데이터 추출 로직 (창고명 매칭 강화)
data_rows = []
activity_date = None
temp_depository = "" 
is_platinum = False

for _, row in df_raw.iterrows():
    # 행 데이터를 문자열 리스트로 변환 (NaN은 'nan'으로 처리)
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0]

    # A. Activity Date 추출
    if not activity_date:
        combined_row = " ".join(vals)
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            activity_date = match.group(1)

    # B. PLATINUM 섹션 체크
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        continue
    if "PALLADIUM" in first_val.upper():
        break

    if not is_platinum:
        continue

    # C. 창고명 및 데이터 추출
    exclude_list = ["TOTAL", "TROY OUNCE", "DEPOSITORY", "REPORT DATE", "ACTIVITY DATE", "NAN"]
    
    if first_val in ["Registered", "Eligible"]:
        try:
            def clean_val(x):
                s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
                return float(s) if s and s != '0.0' else 0.0

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

    elif first_val != "nan" and len(first_val) > 2:
        if not any(k in first_val.upper() for k in exclude_list):
            # 숫자로만 된 행 제외
            if not first_val.replace('.', '').replace(',', '').isdigit():
                temp_depository = first_val

# 3. 저장 및 중복 체크
file_name = 'platinum_daily_stock.csv'
if data_rows:
    new_df = pd.DataFrame(data_rows)
    # 합계 행 최종 필터링
    new_df = new_df[~new_df['Region_Type'].str.contains("Total|TOTAL", na=False)]

    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        if activity_date in existing_df['Date'].astype(str).values:
            print(f"{activity_date} 데이터가 이미 존재합니다. 종료.")
            sys.exit(0)
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df
    
    final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"성공: {activity_date} 저장 완료")
else:
    print("데이터 추출 실패")
    sys.exit(1)
