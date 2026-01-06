import pandas as pd
import requests
import io
import os
import re

# 1. 파일 다운로드 (브라우저인 척 하기)
url = "https://www.cmegroup.com/delivery_reports/PA-PL_Stck_Rprt.xls"
headers = {"User-Agent": "Mozilla/5.0"}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # CME .xls는 실제로는 HTML 테이블인 경우가 많으므로 두 방식 모두 시도
    try:
        # 먼저 HTML 테이블로 읽기 시도
        dfs = pd.read_html(io.BytesIO(response.content))
        df_raw = dfs[0]
        print("HTML 형식으로 데이터를 읽었습니다.")
    except:
        # 실패하면 일반 엑셀로 읽기
        df_raw = pd.read_excel(io.BytesIO(response.content))
        print("Excel 형식으로 데이터를 읽었습니다.")
except Exception as e:
    print(f"다운로드 오류: {e}")
    exit(1)

# 2. 데이터 처리
data_rows = []
report_date = None
current_depository = None
is_platinum = False

for _, row in df_raw.iterrows():
    # 모든 셀을 문자열로 변환하여 리스트화
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0] if vals else ""

    # 날짜 찾기
    if not report_date:
        combined_row = " ".join(vals)
        match = re.search(r'Report Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            report_date = match.group(1)

    # PLATINUM 섹션 시작/종료 감지
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        continue
    if "PALLADIUM" in first_val.upper() or "TOTAL" == first_val.upper():
        if is_platinum and "TOTAL" not in first_val.upper(): # 진짜 종료 지점
            is_platinum = False

    if not is_platinum: continue

    # 창고명 및 데이터 추출
    if first_val in ["Registered", "Eligible"]:
        if current_depository:
            try:
                # 숫자 정제 (쉼표 제거 등)
                def to_f(x):
                    s = str(x).replace(',', '').replace('nan', '0')
                    return float(s) if s else 0.0

                data_rows.append({
                    'Date': report_date,
                    'Region_Type': f"{current_depository} {first_val}",
                    'PREV_TOTAL': to_f(row.iloc[2]),
                    'RECEIVED': to_f(row.iloc[3]),
                    'WITHDRAWN': to_f(row.iloc[4]),
                    'NET_CHANGE': to_f(row.iloc[5]),
                    'ADJUSTMENT': to_f(row.iloc[6]),
                    'TOTAL_TODAY': to_f(row.iloc[7])
                })
            except: continue
    elif first_val != "nan" and "DEPOSITORY" not in first_val.upper() and "TROY" not in first_val.upper():
        if len(first_val) > 3: # 너무 짧은 글자는 무시 (창고 이름 저장)
            current_depository = first_val

# 3. 저장 (데이터가 있을 때만 실행)
if data_rows:
    new_df = pd.DataFrame(data_rows)
    file_name = 'platinum_daily_stock.csv'
    
    if os.path.exists(file_name):
        old_df = pd.read_csv(file_name)
        final_df = pd.concat([old_df, new_df]).drop_duplicates(subset=['Date', 'Region_Type'], keep='last')
    else:
        final_df = new_df
        
    final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"성공: {len(data_rows)}개의 데이터를 저장했습니다.")
else:
    print("실패: 추출된 데이터가 없습니다.")
    exit(1) # 에러를 발생시켜서 로봇이 멈추게 함
