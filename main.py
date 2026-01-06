import pandas as pd
import requests
import io
import os
from datetime import datetime
import re

# 1. 파일 다운로드 및 로드
url = "https://www.cmegroup.com/delivery_reports/PA-PL_Stck_Rprt.xls"

try:
    response = requests.get(url)
    response.raise_for_status()
    # CME 엑셀 파일은 때때로 HTML 형식이거나 구형 xls일 수 있어 엔진을 지정하거나 자동 감지하게 합니다.
    # 일반적인 read_excel로 시도합니다.
    df_raw = pd.read_excel(io.BytesIO(response.content))
except Exception as e:
    print(f"파일 다운로드 또는 읽기 실패: {e}")
    exit()

# 2. 데이터 처리를 위한 준비
data_rows = []
report_date = None
current_depository = None
is_platinum_section = False

# 데이터프레임을 순회하며 파싱 (행 단위 처리)
for index, row in df_raw.iterrows():
    # 첫 번째 열의 값을 문자열로 변환 (NaN 방지)
    first_col = str(row.iloc[0]).strip()
    
    # A. 날짜 추출 (Report Date: MM/DD/YYYY 형식 찾기)
    # 엑셀 상단에 위치하므로 섹션 진입 전에 찾습니다.
    if report_date is None:
        # 행의 모든 셀을 문자열로 합쳐서 날짜 패턴 검색
        row_str = " ".join([str(x) for x in row.values if pd.notna(x)])
        date_match = re.search(r'Report Date:\s*(\d{1,2}/\d{1,2}/\d{4})', row_str)
        if date_match:
            report_date = date_match.group(1)
            continue

    # B. 섹션 확인 (PLATINUM 시작과 PALLADIUM 종료)
    if "PLATINUM" in first_col:
        is_platinum_section = True
        continue
    if "PALLADIUM" in first_col:
        is_platinum_section = False
        break # 더 이상 볼 필요 없음
    
    # PLATINUM 섹션이 아니면 건너뜀
    if not is_platinum_section:
        continue

    # C. 데이터 추출 로직
    # 헤더나 빈 줄은 건너뛰기
    if first_col == "nan" or "DEPOSITORY" in first_col or "Troy Ounce" in first_col:
        continue
    
    # 로직:
    # 1. Depository 이름인 경우 (데이터 컬럼인 2번째 열부터가 비어있거나 0인 경우 등 구조적 특징 이용)
    #    보통 Depository 이름 행은 수치 데이터가 없습니다.
    # 2. Registered / Eligible 인 경우 -> 데이터 저장
    
    # Registered 또는 Eligible 행인지 확인
    if first_col.startswith("Registered") or first_col.startswith("Eligible"):
        if current_depository and report_date:
            # 카테고리 (Region + Type) 생성
            # 예: BRINK'S, INC. Registered
            region_type = f"{current_depository} {first_col}"
            
            # 수치 데이터 추출 (열 인덱스 2~7에 해당: PREV, REC, WITH, NET, ADJ, TOTAL)
            # 엑셀 파일 구조상:
            # Col 0: Label, Col 1: (Empty), Col 2: PREV ...
            
            try:
                prev_total = row.iloc[2]
                received = row.iloc[3]
                withdrawn = row.iloc[4]
                net_change = row.iloc[5]
                adjustment = row.iloc[6]
                total_today = row.iloc[7]
                
                # 결과 리스트에 추가
                data_rows.append({
                    'Date': report_date,
                    'Region_Type': region_type,
                    'PREV_TOTAL': prev_total,
                    'RECEIVED': received,
                    'WITHDRAWN': withdrawn,
                    'NET_CHANGE': net_change,
                    'ADJUSTMENT': adjustment,
                    'TOTAL_TODAY': total_today
                })
            except IndexError:
                continue
                
    elif "Total" in first_col:
        # Depository별 Total 행은 제외 (필요시 포함 가능)
        continue
        
    elif "TOTAL REGISTERED" in first_col or "TOTAL ELIGIBLE" in first_col:
        # 전체 합계 행 제외 (필요시 로직 수정 가능)
        continue
        
    else:
        # 위의 키워드들이 아니고 텍스트가 있다면 Depository 이름으로 간주
        # (예: BRINK'S, INC.)
        if len(first_col) > 1:
            current_depository = first_col

# 3. 데이터프레임 생성 및 저장
if data_rows:
    new_df = pd.DataFrame(data_rows)
    
    # 파일 저장 설정
    file_name = 'platinum_daily_stock.csv'
    
    if os.path.exists(file_name):
        # 기존 파일이 있으면 불러와서 중복 확인 후 병합
        existing_df = pd.read_csv(file_name)
        
        # 날짜 형식이 다를 수 있으므로 통일 (선택 사항)
        
        # 이번 데이터가 이미 존재하는지 날짜로 확인 (중복 실행 방지)
        # 만약 같은 날짜 데이터가 이미 있다면 삭제하고 최신으로 덮어쓰거나, 추가하지 않음
        # 여기서는 단순히 append 모드로 하되, 같은 날짜+Region_Type이 겹치지 않게 처리 추천
        
        # 병합 (기존 데이터 + 새 데이터)
        combined_df = pd.concat([existing_df, new_df])
        
        # 중복 제거 (같은 날짜, 같은 창고의 데이터가 중복되면 최신 것 남김)
        combined_df.drop_duplicates(subset=['Date', 'Region_Type'], keep='last', inplace=True)
        
        combined_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"업데이트 완료: {file_name} (데이터 {len(new_df)}건 추가됨)")
    else:
        # 파일이 없으면 새로 생성
        new_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"새 파일 생성: {file_name} (데이터 {len(new_df)}건)")
else:
    print("추출된 데이터가 없습니다. 파일 구조가 변경되었는지 확인하세요.")
