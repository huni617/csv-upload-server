from fastapi import FastAPI, UploadFile, File
import pandas as pd
import os
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    # 경로 설정
    amazon_reports_path = "/AmazonReports"
    merged_csv_path = "/MergedData/total.csv"

    # 📁 폴더 생성 (없을 경우)
    os.makedirs(amazon_reports_path, exist_ok=True)
    os.makedirs(os.path.dirname(merged_csv_path), exist_ok=True)

    # 📥 업로드 파일 저장
    uploaded_path = os.path.join(amazon_reports_path, file.filename)
    with open(uploaded_path, "wb") as f:
        content = await file.read()
        f.write(content)

    try:
        # 📖 파일 로드
        df_new = pd.read_csv(uploaded_path)

        # 📅 업로드 기준 날짜 -2일
        target_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        df_new.insert(0, "Date", target_date)

        # 📆 Month Week 컬럼 추가 (빈 값)
        if "Month Week" not in df_new.columns:
            df_new.insert(1, "Month Week", "")

        # 🔢 문자열 숫자 변환
        numeric_cols = [
            "Sessions - Total", "Sessions - Total - B2B",
            "Page Views - Total", "Page Views - Total - B2B",
            "Units Ordered", "Units Ordered - B2B",
            "Ordered Product Sales", "Ordered Product Sales - B2B"
        ]
        for col in numeric_cols:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors='coerce').fillna(0)

        # ✅ 집계 컬럼 계산
        title_index = df_new.columns.get_loc("Title") if "Title" in df_new.columns else 3  # fallback

        df_new.insert(title_index + 1, "Total Session", df_new["Sessions - Total"] + df_new["Sessions - Total - B2B"])
        df_new.insert(title_index + 2, "Total Page View", df_new["Page Views - Total"] + df_new["Page Views - Total - B2B"])
        df_new.insert(title_index + 3, "Total Units Ordered", df_new["Units Ordered"] + df_new["Units Ordered - B2B"])
        df_new.insert(title_index + 4, "Total Ordered Product Sales", df_new["Ordered Product Sales"] + df_new["Ordered Product Sales - B2B"])
        df_new.insert(title_index + 5, "Total Conversion Rate", round((df_new["Total Units Ordered"] / df_new["Total Session"]) * 100, 2).fillna(0))

    except Exception as e:
        return {"error": f"CSV 처리 중 오류 발생: {e}"}

    # 📊 total.csv 병합
    if os.path.exists(merged_csv_path):
        df_total = pd.read_csv(merged_csv_path)
        df_merged = pd.concat([df_total, df_new], ignore_index=True)
    else:
        df_merged = df_new

    # 💾 저장
    df_merged.to_csv(merged_csv_path, index=False)

    return {
        "✅ 추가된 row 수": len(df_new),
        "✅ 병합된 총 row 수": len(df_merged),
        "✅ 저장 완료": merged_csv_path
    }
