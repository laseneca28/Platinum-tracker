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
        # CME 파일은 HTML 형식이 많으므로 read_html 우선 사용
        dfs = pd.read_html(io.BytesIO(response.content))
        df_raw = dfs[0]
    except:
        df_raw = pd.read_excel(io.BytesIO(response.content))
except Exception as e:
    print(f"다운로드 오류: {e}")
    sys.exit(1)

# 2. 데이터 추출 로직
data_rows = []
activity_date = None
current_depository = ""
is_platinum = False

for _, row in df_raw.iterrows():
    # 행 데이터를 리스트로 변환
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0] if vals else "nan"

    # A. Activity Date 추출
    if not activity_date:
        combined_row = " ".join(vals)
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            activity_date = match.group(1)

    # B. PLATINUM 섹션 진입/종료 체크
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        continue
    if "PALLADIUM" in first_val.upper():
        is_platinum = False
        break

    if not is_platinum:
        continue

    # C. 데이터 행 분석
    # 제외 키워드 리스트
    exclude_list = ["TOTAL", "TROY OUNCE", "DEPOSITORY", "REPORT DATE", "ACTIVITY DATE", "nan"]
    
    # 1) 'Registered' 또는 'Eligible' 행을 만나면 데이터 저장
    if first_val in ["Registered", "Eligible"]:
        if "TOTAL" in first_val.upper(): # 'Total Registered' 같은 합계행 제외
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

    # 2) 창고 이름 업데이트 로직 (핵심 수정 부분)
    # - 숫자가 아니고
    # - 제외 키워드가 포함되지 않았으며
    # - 비어있지 않은 문자열인 경우 새로운 창고명으로 인식
    elif first_val != "nan" and len(first_val) > 3:
        if not any(k in first_val.upper() for k in exclude_list):
            # 숫자로만 이루어진 문자열인지 체크 (수치 행 오인 방지)
            if not first_val.replace('.', '').replace(',', '').isdigit():
                current_depository = first_val
                print(f"창고 인식: {current_depository}")

# 3. 중복 체크 및 저장
file_name = 'platinum_daily_stock.csv'
if data_rows:
    new_df = pd.DataFrame(data_rows)
    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        if activity_date in existing_df['Date'].astype(str).values:
            print(f"이미 {activity_date} 데이터가 존재합니다. 덮어쓰지 않고 종료합니다.")
            sys.exit(0)
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df
    
    final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"성공: {activity_date} 데이터 {len(data_rows)}행 저장 완료")
else:
    print("추출된 데이터가 없습니다.")
    sys.exit(1)
