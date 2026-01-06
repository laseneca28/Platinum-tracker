import pandas as pd
import requests
import io
import os
import re
import sys

# ---------------------------------------------------------
# [설정] 엑셀 라이브러리 확인
# ---------------------------------------------------------
try:
    import openpyxl
except ImportError:
    print("!!! 경고: 'openpyxl' 라이브러리가 없습니다. 엑셀 대신 CSV로 저장합니다.")
    print("    (설치 방법: pip install openpyxl)")

# ---------------------------------------------------------
# 1. 파일 다운로드
# ---------------------------------------------------------
url = "https://www.cmegroup.com/delivery_reports/PA-PL_Stck_Rprt.xls"
headers = {"User-Agent": "Mozilla/5.0"}

print("--- [1단계] 데이터 다운로드 시작 ---")
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    raw_data = response.content.decode('utf-8', errors='ignore')
    
    try:
        dfs = pd.read_html(io.StringIO(raw_data))
        df_raw = dfs[0]
        print("성공: HTML 형식으로 데이터를 읽었습니다.")
        # 디버깅: 데이터 앞부분 출력
        print(f"데이터 크기: {df_raw.shape}")
        print("데이터 미리보기(상위 3행):")
        print(df_raw.head(3))
    except:
        df_raw = pd.read_excel(io.BytesIO(response.content), engine='xlrd')
        print("성공: Excel 엔진으로 데이터를 읽었습니다.")

except Exception as e:
    print(f"오류: 다운로드 또는 파싱 실패 - {e}")
    sys.exit(1)

# ---------------------------------------------------------
# 2. 데이터 추출 로직
# ---------------------------------------------------------
data_rows = []
activity_date = None
temp_depository = "" 
is_platinum = False

def clean_val(x):
    s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
    try: return float(s)
    except: return 0.0

exclude_list = ["TOTAL", "TROY OUNCE", "REPORT DATE", "ACTIVITY DATE", "NAN", "NEW YORK", "COMEX"]

print("\n--- [2단계] 데이터 추출 및 창고명 탐색 시작 ---")

for index, row in df_raw.iterrows():
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0]

    # 날짜 추출
    if not activity_date:
        combined_row = " ".join(vals)
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            activity_date = match.group(1)
            print(f"-> 기준 날짜 확인: {activity_date}")

    # 섹션 확인
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        print("-> PLATINUM 섹션 진입 (데이터 수집 시작)")
        continue
    if "PALLADIUM" in first_val.upper():
        print("-> PALLADIUM 섹션 도달 (수집 종료)")
        break 

    if not is_platinum:
        continue

    # 헤더 "DEPOSITORY" 건너뛰기
    if first_val.upper() == "DEPOSITORY":
        continue

    # A. 데이터 행 (Registered/Eligible)
    if first_val in ["Registered", "Eligible"]:
        if not temp_depository: 
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
        except: continue

    # B. 창고명 탐색
    elif first_val != "nan" and len(first_val) > 3:
        if not any(k in first_val.upper() for k in exclude_list):
            if not any(char.isdigit() for char in first_val):
                temp_depository = first_val
                # print(f"-> [창고 발견] {temp_depository}") # 너무 많으면 주석 처리

# ---------------------------------------------------------
# 3. 데이터 저장 (엑셀 실패 시 CSV 자동 전환)
# ---------------------------------------------------------
print("\n--- [3단계] 데이터 저장 시도 ---")

if data_rows:
    # 데이터 프레임 생성
    new_df = pd.DataFrame(data_rows)
    new_df = new_df[~new_df['Region_Type'].str.contains("TOTAL", case=False, na=False)]
    print(f"-> 추출된 데이터: 총 {len(new_df)}행")

    # 요약 데이터 생성
    pattern = r'^(.*)\s+(Registered|Eligible)$'
    summary_prep = new_df.copy()
    summary_prep[['Depository', 'Status']] = summary_prep['Region_Type'].str.extract(pattern)
    summary_df = summary_prep.pivot_table(
        index='Depository', columns='Status', values='TOTAL_TODAY', aggfunc='sum', fill_value=0
    )
    
    # 컬럼 안전장치
    if 'Registered' not in summary_df.columns: summary_df['Registered'] = 0
    if 'Eligible' not in summary_df.columns: summary_df['Eligible'] = 0
    summary_df['Total_Stock'] = summary_df['Registered'] + summary_df['Eligible']
    summary_df = summary_df.sort_values(by='Total_Stock', ascending=False)

    # 엑셀 저장 시도
    excel_file = 'platinum_daily_report.xlsx'
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            new_df.to_excel(writer, sheet_name='Daily_Data', index=False)
            summary_df.to_excel(writer, sheet_name='Summary_By_Depository')
        print(f"\n[성공] 엑셀 파일이 생성되었습니다: {excel_file}")
        
    except Exception as e:
        print(f"\n[실패] 엑셀 저장 중 오류 발생: {e}")
        print("-> 대신 CSV 파일로 저장을 시도합니다.")
        
        csv_file = 'platinum_daily_backup.csv'
        new_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"[성공] CSV 파일로 저장되었습니다: {csv_file}")
        
        # 요약본도 CSV로 저장
        summary_csv = 'platinum_summary_backup.csv'
        summary_df.to_csv(summary_csv, encoding='utf-8-sig')
        print(f"[성공] 요약 CSV 파일로 저장되었습니다: {summary_csv}")

else:
    print("!!! 실패: 데이터를 하나도 찾지 못했습니다.")
    print("-> 원인: 웹사이트 형식이 변경되었거나, 'PLATINUM' 섹션을 찾지 못했습니다.")
