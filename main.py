from fastapi import FastAPI, UploadFile, File
import pandas as pd
import os
import shutil
from datetime import datetime, timedelta

app = FastAPI()

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    os.makedirs("temp", exist_ok=True)
    new_path = os.path.join("temp", file.filename)

    with open(new_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # 날짜 계산: 오늘 -2일
    upload_date = datetime.today().date() - timedelta(days=2)
    date_str = upload_date.strftime("%Y-%m-%d")

    # CSV 로딩
    df = pd.read_csv(new_path)

    # 컬럼 추가
    df.insert(0, "Date", date_str)
    df.insert(1, "Month Week", "")
    df["Total Session"] = df["Sessions - Total"] + df["Sessions - Total - B2B"]
    df["Total Page View"] = df["Page Views - Total"] + df["Page Views - Total - B2B"]
    df["Total Units Ordered"] = df["Units Ordered"] + df["Units Ordered - B2B"]
    df["Total Ordered Product Sales"] = df["Ordered Product Sales"] + df["Ordered Product Sales - B2B"]
    df["Total Conversion Rate"] = (df["Total Units Ordered"] / df["Total Session"]) * 100

    # 병합 파일 경로
    master_path = "merged/total.csv"
    os.makedirs("merged", exist_ok=True)

    # 병합
    if os.path.exists(master_path):
        master_df = pd.read_csv(master_path)
        combined_df = pd.concat([master_df, df], ignore_index=True)
    else:
        combined_df = df

    # 저장
    combined_df.to_csv(master_path, index=False)

    return {"status": "success", "appended_rows": len(df)}
