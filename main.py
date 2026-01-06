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
    # HTML 형식으로 읽는 것이 가장 정확함
    dfs = pd.read_html(io.BytesIO(response.content))
    df_raw = dfs[0]
except Exception as e:
    print(f"다운로드 또는 파싱 오류: {e}")
    sys.exit(1)

# 2. 데이터 추출
data_rows = []
activity_date = None
temp_depository = ""  # 창고명을 임시 저장
is_platinum = False

for _, row in df_raw.iterrows():
    # 전체 행을 문자열 리스트로 변환
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0]

    # A. Activity Date 추출
    if not activity_date:
        combined_row = " ".join(vals)
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            activity_date = match.group(1)

    # B. PLATINUM 섹션 범위
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        continue
    if "PALLADIUM" in first_val.upper():
        break

    if not is_platinum:
        continue

    # C. 창고명 및 데이터 추출 로직
    # 1) 제외할 키워드 (데이터가 아닌 행들)
    exclude_list = ["TOTAL", "TROY OUNCE", "DEPOSITORY", "REPORT DATE", "ACTIVITY DATE", "NAN"]
    
    # 2) 데이터 행(Registered, Eligible) 처리
    if first_val in ["Registered", "Eligible"]:
        try:
            def clean_val(x):
                s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
                return float(s) if s and s != '0.0' else 0.0

            # 데이터를 찾으면 현재까지 저장된 temp_depository를 사용하여 저장
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

    # 3) 창고명 후보 포착
    # 첫 번째 열에 글자가 있고, 제외 키워드가 없으며, 숫자가 아닌 경우에만 창고명 후보로 갱신
    elif first_val != "nan" and len(first_val) > 3:
        if not any(k in first_val.upper() for k in exclude_list):
            # 숫자로만 된 행(페이지 번호 등) 제외
            if not first_val.replace('.', '').replace(',', '').isdigit():
                temp_depository = first_val
                print(f"새로운 창고 후보 포착: {temp_depository}")

# 3. 저장 및 중복 체크
file_name = 'platinum_daily_stock.csv'
if data_rows:
    new_df = pd.DataFrame(data_rows)
    
    # 날짜별로 정렬이 안 될 수 있으므로 합계 행(Total Registered 등)이 포함되었는지 최종 필터링
    new_df = new_df[~new_df['Region_Type'].str.contains("Total|TOTAL", na=False)]

    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        if activity_date in existing_df['Date'].astype(str).values:
            print(f"{activity_date} 데이터가 이미 존재합니다. 저장을 건너뜁니다.")
            sys.exit(0)
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df
    
    final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"저장 성공: {activity_date} ({len(new_df)}개 창고 데이터)")
else:
    print("데이터 추출 실패")
    sys.exit(1)
