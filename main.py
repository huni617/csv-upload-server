from fastapi import FastAPI, UploadFile, File
import shutil, os
import pandas as pd
from datetime import datetime, timedelta

app = FastAPI()

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    try:
        # [1] 업로드 파일 저장
        os.makedirs("uploads", exist_ok=True)
        filepath = os.path.join("uploads", file.filename)
        with open(filepath, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # [2] 업로드 기준 날짜 계산 (오늘 - 2일)
        upload_date = datetime.today() - timedelta(days=2)
        date_str = upload_date.strftime("%Y-%m-%d")

        # [3] CSV 로드
        df = pd.read_csv(filepath)

        # [4] Date, Month Week 컬럼 삽입
        df.insert(0, "Date", date_str)
        df.insert(1, "Month Week", "")

        # [5] Title 기준으로 계산된 컬럼들 삽입
        if "Title" in df.columns:
            title_index = df.columns.get_loc("Title")
        else:
            return {"status": "error", "detail": "Title column not found."}

        # Total 계산 열 삽입
        df.insert(title_index + 1, "Total Session", df["Sessions - Total"] + df["Sessions - Total - B2B"])
        df.insert(title_index + 2, "Total Page View", df["Page Views - Total"] + df["Page Views - Total - B2B"])
        df.insert(title_index + 3, "Total Units Ordered", df["Units Ordered"] + df["Units Ordered - B2B"])
        df.insert(title_index + 4, "Total Ordered Product Sales", df["Ordered Product Sales"] + df["Ordered Product Sales - B2B"])
        df.insert(title_index + 5, "Total Conversion Rate", round((df["Total Units Ordered"] / df["Total Session"]) * 100, 2))

        # [6] 기존 total.csv와 병합
        total_path = "uploads/total.csv"
        if os.path.exists(total_path):
            total_df = pd.read_csv(total_path)
            merged_df = pd.concat([total_df, df], ignore_index=True)
        else:
            merged_df = df

        # [7] 저장
        merged_df.to_csv(total_path, index=False)

        return {"status": "success", "filename": file.filename}

    except Exception as e:
        return {"status": "error", "detail": str(e)}
