import pandas as pd
import requests
import io
import os
import re
import sys

# 1. 파일 다운로드 및 설정
url = "https://www.cmegroup.com/delivery_reports/PA-PL_Stck_Rprt.xls"
headers = {"User-Agent": "Mozilla/5.0"}

try:
    print("데이터 다운로드 중...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # 파일을 텍스트로 읽되, 인코딩 오류 시 무시
    raw_data = response.content.decode('utf-8', errors='ignore') 
    
    # HTML 내에서 테이블만 추출 (CME 엑셀은 종종 HTML 형식을 띔)
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

# 2. 데이터 추출 로직
data_rows = []
activity_date = None
temp_depository = "" 
is_platinum = False

# 수치 데이터 클리닝 함수 (콤마 제거, Nan 처리)
def clean_val(x):
    s = str(x).replace(',', '').replace('nan', '0').replace('None', '0')
    try: 
        return float(s)
    except: 
        return 0.0

# 제외할 키워드 리스트 (창고명이 아닌 헤더나 요약 행)
exclude_list = ["TOTAL", "TROY OUNCE", "DEPOSITORY", "REPORT DATE", "ACTIVITY DATE", "NAN", "NEW YORK"]

for _, row in df_raw.iterrows():
    # 모든 값을 문자열로 변환 및 공백 제거
    vals = [str(v).strip() for v in row.values]
    first_val = vals[0]

    # A. Activity Date 추출 (한 번만 추출하면 됨)
    if not activity_date:
        combined_row = " ".join(vals)
        # 날짜 형식 찾기 (MM/DD/YYYY)
        match = re.search(r'Activity Date:\s*(\d{1,2}/\d{1,2}/\d{4})', combined_row)
        if match:
            activity_date = match.group(1)

    # B. PLATINUM 섹션 진입/이탈 체크
    if "PLATINUM" in first_val.upper():
        is_platinum = True
        continue
    if "PALLADIUM" in first_val.upper():
        break # Palladium 섹션이 나오면 루프 종료

    if not is_platinum:
        continue

    # C. 데이터 및 창고명 추출 로직
    
    # 1) 'Registered' 또는 'Eligible' 행을 만나면 데이터 저장 시도
    if first_val in ["Registered", "Eligible"]:
        
        # [핵심 수정] 창고 이름(temp_depository)을 아직 못 찾았다면 저장하지 않고 건너뜀
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
        except Exception as e:
            # 인덱스 에러 등 예외 발생 시 무시
            continue

    # 2) 창고명(Depository) 업데이트 로직
    # 값이 있고, 길이가 3자 이상이며, 'Registered/Eligible'이 아닌 경우
    elif first_val != "nan" and len(first_val) > 3:
        # 제외 키워드가 포함되지 않았는지 확인
        if not any(k in first_val.upper() for k in exclude_list):
            # 숫자가 포함되지 않은 순수 텍스트(창고명)인 경우만 인정
            if not any(char.isdigit() for char in first_val):
                temp_depository = first_val
                # print(f"창고 감지됨: {temp_depository}") # 디버깅용

# 3. 저장 및 중복 방지 로직
file_name = 'platinum_daily_stock.csv'

if data_rows:
    new_df = pd.DataFrame(data_rows)
    
    # 혹시 모를 중복 합계 행(TOTAL 포함)이 들어갔다면 제거
    new_df = new_df[~new_df['Region_Type'].str.upper().contains("TOTAL", na=False)]

    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        
        # 날짜 기준으로 이미 데이터가 있는지 확인 (문자열로 비교)
        if str(activity_date) in existing_df['Date'].astype(str).values:
            print(f"알림: {activity_date} 데이터는 이미 저장되어 있습니다. 종료합니다.")
            sys.exit(0)
            
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df
    
    # CSV 저장 (한글 깨짐 방지 utf-8-sig)
    final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"성공: {activity_date} 일자 데이터 {len(new_df)}건 저장 완료")
    
    # 결과 확인용 출력 (상위 5개)
    print("\n[저장된 데이터 미리보기]")
    print(new_df.head())

else:
    print("데이터 추출 실패: 유효한 데이터 행을 찾지 못했습니다.")
    sys.exit(1)
