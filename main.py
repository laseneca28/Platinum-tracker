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
    if first_val in ["Registered", "
