import pandas as pd
import requests
import io
import os
import re
import sys

# ---------------------------------------------------------
# 1. 파일 다운로드 및 설정
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

# 제외 키워드에서 "DEPOSITORY" 제거 (창고명 포함 이슈 해결)
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
        print("-> PLATINUM 섹션 진입")
        continue
    if "PALLADIUM" in first_val.upper():
        print("-> PALLADIUM 섹션 도달 (종료)")
        break 

    if not is_platinum:
        continue

    # 헤더 "DEPOSITORY"만 정확히 건너뛰기
    if first_val.upper() == "DEPOSITORY":
        continue

    # A. 데이터 행 (Registered/Eligible)
    if first_val in ["Registered", "Eligible"]:
        if not temp_depository: continue
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
                print(f"-> [창고 발견] {temp_depository}")

# ---------------------------------------------------------
# 3. 데이터 가공 및 엑셀 저장 (시트 분리)
# ---------------------------------------------------------
print("\n--- [3단계] 데이터 요약 및 저장 ---")

if data_rows:
    # (1) 오늘의 기본 데이터 생성
    new_df = pd.DataFrame(data_rows)
    new_df = new_df[~new_df['Region_Type'].str.contains("TOTAL", case=False, na=False)]
    
    print(f"-> 추출된 데이터: 총 {len(new_df)}행")

    # (2) 요약 데이터(Summary) 생성
    # 정규식으로 창고명과 상태 분리
    pattern = r'^(.*)\s+(Registered|Eligible)$'
    summary_prep = new_df.copy()
    summary_prep[['Depository', 'Status']] = summary_prep['Region_Type'].str.extract(pattern)

    # 피벗 테이블 생성 (창고별 합계)
    summary_df = summary_prep.pivot_table(
        index='Depository', 
        columns='Status', 
        values='TOTAL_TODAY', 
        aggfunc='sum', 
        fill_value=0
    )

    # 컬럼이 없는 경우를 대비해 안전하게 합계 계산
    cols = summary_df.columns
    if 'Registered' not in cols: summary_df['Registered'] = 0
    if 'Eligible' not in cols: summary_df['Eligible'] = 0
    
    # 총합 컬럼 추가 및 정렬
    summary_df['Total_Stock'] = summary_df['Registered'] + summary_df['Eligible']
    summary_df = summary_df.sort_values(by='Total_Stock', ascending=False)

    print("\n[창고별 재고 요약 미리보기]")
    print(summary_df)

    # (3) 엑셀 파일로 저장 (.xlsx)
    excel_file = 'platinum_daily_report.xlsx'
    
    # 기존 데이터 로드 (누적을 위함)
    final_history_df = new_df
    if os.path.exists(excel_file):
        try:
            existing_df = pd.read_excel(excel_file, sheet_name='Daily_Data')
            # 중복 날짜 체크
            if str(activity_date) in existing_df['Date'].astype(str).values:
                print(f"\n알림: {activity_date} 데이터가 이미 존재합니다. 덮어쓰거나 기존 데이터를 유지합니다.")
                final_history_df = existing_df # 중복이면 기존 것 유지 (또는 new_df로 교체 가능)
            else:
                final_history_df = pd.concat([existing_df, new_df], ignore_index=True)
                print(f"\n성공: 기존 데이터에 {activity_date} 내역을 추가했습니다.")
        except:
            print("\n알림: 기존 파일에서 시트를 읽지 못해 새로 생성합니다.")

    # 엑셀 쓰기 (두 개의 시트 생성)
    # mode='w'는 파일을 새로 씁니다. (히스토리는 위에서 합쳐두었음)
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        # 시트 1: 전체 히스토리 데이터
        final_history_df.to_excel(writer, sheet_name='Daily_Data', index=False)
        
        # 시트 2: 오늘 날짜 기준 요약 (창고별)
        summary_df.to_excel(writer, sheet_name='Summary_By_Depository')

    print(f"\n파일 저장 완료: {excel_file}")
    print("  - 시트1 (Daily_Data): 전체 일별 데이터")
    print("  - 시트2 (Summary_By_Depository): 창고별 재고 합계 요약")

else:
    print("실패: 유효한 데이터를 찾지 못했습니다.")
