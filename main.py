from fastapi import FastAPI, UploadFile, File
import shutil, os
import pandas as pd
from datetime import datetime, timedelta

app = FastAPI()

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    try:
        # 1. 파일 저장
        os.makedirs("uploads", exist_ok=True)
        filepath = os.path.join("uploads", file.filename)
        with open(filepath, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # 2. 날짜 계산 (오늘 - 2일)
        upload_date = datetime.today() - timedelta(days=2)
        date_str = upload_date.strftime("%Y-%m-%d")

        # 3. CSV 로드
        df = pd.read_csv(filepath)

        # 4. 기본 컬럼 삽입
        df.insert(0, "Date", date_str)
        df.insert(1, "Month Week", "")

        # 5. Title 기준 위치
        if "Title" not in df.columns:
            return {"status": "error", "detail": "Title column not found."}
        title_index = df.columns.get_loc("Title")

        # 6. 컬럼 계산
        try:
            df.insert(title_index + 1, "Total Session",
                df["Sessions - Mobile App"].fillna(0) +
                df["Sessions - Browser"].fillna(0) +
                df["Sessions - Mobile APP - B2B"].fillna(0) +
                df["Sessions - Browser - B2B"].fillna(0)
            )

            df.insert(title_index + 2, "Total Page View",
                df["Page Views - Mobile App"].fillna(0) +
                df["Page Views - Browser"].fillna(0) +
                df["Page Views - Mobile APP - B2B"].fillna(0) +
                df["Page Views - Browser - B2B"].fillna(0)
            )

            df.insert(title_index + 3, "Total Units Ordered",
                df["Units Ordered"].fillna(0) +
                df["Units Ordered - B2B"].fillna(0)
            )

            df.insert(title_index + 4, "Total Ordered Product Sales",
                df["Ordered Product Sales"].fillna(0) +
                df["Ordered Product Sales - B2B"].fillna(0)
            )

            df["Total Conversion Rate"] = round(
                (df["Total Units Ordered"] / df["Total Session"]) * 100, 2
            )

        except KeyError as e:
            return {"status": "error", "detail": f"Missing column: {str(e)}"}

        # 7. total.csv와 병합
        total_path = "uploads/total.csv"
        if os.path.exists(total_path):
            total_df = pd.read_csv(total_path)
            merged_df = pd.concat([total_df, df], ignore_index=True)
        else:
            merged_df = df

        # 8. 저장
        merged_df.to_csv(total_path, index=False)

        return {"status": "success", "filename": file.filename}

    except Exception as e:
        return {"status": "error", "detail": str(e)}
