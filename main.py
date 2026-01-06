import pandas as pd
import requests
import io
import os
import re
import sys

# 1. 파일 다운로드 및 인코딩 강제 해결
url = "https://www.cmegroup.com/delivery_reports/PA-PL_Stck_Rprt.xls"
headers = {"User-Agent": "Mozilla/5.0"}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # 파일을 텍스트로 읽되, 오류가 나면 인코딩을 무시하거나 라틴 인코딩 시도
    raw_data = response.content.decode('utf-8', errors='ignore') 
    
    # HTML 내에서 테이블만 추출
    try:
        dfs = pd.read_html(io.StringIO(raw_data))
        df_raw = dfs[0]
        print("HTML 엔진으로 데이터를 성공적으로 읽었습니다.")
    except:
        # HTML로 실패 시 구형 엑셀(xlrd) 엔진 시도
        df_raw = pd.read_excel(io.BytesIO(response.content), engine='xlrd')
        print("Excel 엔진으로 데이터를 읽었습니다.")
        
except Exception as e:
    print(f"다운로드 또는 파싱 오류 발생: {e}")
    sys.exit(1)

# 2. 데이터 추출 로직 (가장 정교한 창고명 매칭)
data_rows = []
activity_date = None
temp_depository = "" 
is_platinum = False

for _, row in df_raw.iterrows():
    # 데이터 정제: 모든 열의 값을 문자열로 바꾸고 공백 제거
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

    # C. 데이터 및 창고명 추출
    # 필터링할 키워드들
    exclude_list = ["TOTAL", "TROY OUNCE", "DEPOSITORY", "REPORT DATE", "ACTIVITY DATE", "NAN", "NEW YORK"]

    # 1) 'Registered' 또는 'Eligible' 행을 만나면 데이터 저장
    if first_val in ["Registered", "Eligible"]:
        try:
            # 수치 데이터 클리닝
            def clean_val(x):
                s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
                try: return float(s)
                except: return 0.0

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

    # 2) 창고명 업데이트 (핵심)
    # 글자수가 있고, 제외 리스트에 없으며, 숫자로만 된 행이 아닌 경우
    elif first_val != "nan" and len(first_val) > 3:
        if not any(k in first_val.upper() for k in exclude_list):
            # 숫자가 섞여있지 않은 순수 텍스트 위주인 경우에만 창고명으로 인정
            if not any(char.isdigit() for char in first_val):
                temp_depository = first_val
                print(f"현재 창고 감지: {temp_depository}")

# 3. 저장 및 중복 방지
file_name = 'platinum_daily_stock.csv'
if data_rows:
    new_df = pd.DataFrame(data_rows)
    
    # 혹시 모를 중복 합계 행(TOTAL) 제거
    new_df = new_df[~new_df['Region_Type'].str.contains("Total|TOTAL", na=False)]

    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        if activity_date in existing_df['Date'].astype(str).values:
            print(f"이미 {activity_date} 데이터가 저장되어 있습니다.")
            sys.exit(0)
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df
    
    final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"성공: {activity_date} 데이터 저장 완료")
else:
    print("데이터 추출 실패: 리포트 형식을 다시 확인하세요.")
    sys.exit(1)
