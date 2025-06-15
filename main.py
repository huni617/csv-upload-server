from fastapi import FastAPI, UploadFile, File
import pandas as pd
import os
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    # 상대경로로 설정
    amazon_reports_path = "AmazonReports"
    merged_csv_path = "MergedData/total.csv"

    # 디렉토리 생성
    os.makedirs(amazon_reports_path, exist_ok=True)
    os.makedirs(os.path.dirname(merged_csv_path), exist_ok=True)

    # 파일 저장
    uploaded_path = os.path.join(amazon_reports_path, file.filename)
    with open(uploaded_path, "wb") as f:
        content = await file.read()
        f.write(content)

    try:
        df_new = pd.read_csv(uploaded_path)
    except Exception as e:
        return {"error": f"파일 읽기 실패: {e}"}

    # 날짜 계산
    upload_date = datetime.now().date()
    target_date = upload_date - timedelta(days=2)
    df_new["Date"] = target_date.strftime("%Y-%m-%d")

    if "Month Week" not in df_new.columns:
        df_new["Month Week"] = ""

    default_columns = [
        "Total Session", "Total Page View",
        "Total Units Ordered", "Total Ordered Product Sales", "Total Conversion Rate"
    ]
    for col in default_columns:
        if col not in df_new.columns:
            df_new[col] = 0

    # 기존 데이터 병합
    if os.path.exists(merged_csv_path):
        df_total = pd.read_csv(merged_csv_path)
        df_merged = pd.concat([df_total, df_new], ignore_index=True)
    else:
        df_merged = df_new

    # 저장
    df_merged.to_csv(merged_csv_path, index=False)

    return {
        "✅ 추가된 row 수": len(df_new),
        "✅ 병합된 총 row 수": len(df_merged),
        "✅ 저장 완료": os.path.abspath(merged_csv_path)
    }
