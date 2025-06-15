from fastapi import FastAPI, UploadFile, File
import shutil, os
import pandas as pd
from datetime import datetime, timedelta
import traceback

app = FastAPI()

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    try:
        os.makedirs("uploads", exist_ok=True)
        filepath = os.path.join("uploads", file.filename)
        with open(filepath, "wb") as f:
            shutil.copyfileobj(file.file, f)

        upload_date = datetime.today() - timedelta(days=2)
        date_str = upload_date.strftime("%Y-%m-%d")

        df = pd.read_csv(filepath)

        df.insert(0, "Date", date_str)
        df.insert(1, "Month Week", "")

        # 수치형으로 변환
        num_cols = [
            "Sessions - Total",
            "Sessions - Total - B2B",
            "Page Views - Total",
            "Page Views - Total - B2B",
            "Units Ordered",
            "Units Ordered - B2B",
            "Ordered Product Sales",
            "Ordered Product Sales - B2B"
        ]

        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        if "Title" in df.columns:
            title_index = df.columns.get_loc("Title")
        else:
            return {"status": "error", "detail": "Title column not found."}

        df.insert(title_index + 1, "Total Session", df["Sessions - Total"] + df["Sessions - Total - B2B"])
        df.insert(title_index + 2, "Total Page View", df["Page Views - Total"] + df["Page Views - Total - B2B"])
        df.insert(title_index + 3, "Total Units Ordered", df["Units Ordered"] + df["Units Ordered - B2B"])
        df.insert(title_index + 4, "Total Ordered Product Sales", df["Ordered Product Sales"] + df["Ordered Product Sales - B2B"])

        df["Total Conversion Rate"] = round((df["Total Units Ordered"] / df["Total Session"]) * 100, 2)

        # total.csv 병합
        total_path = "uploads/total.csv"
        if os.path.exists(total_path):
            total_df = pd.read_csv(total_path)
            print("✅ 기존 row 수:", len(total_df))
            merged_df = pd.concat([total_df, df], ignore_index=True)
        else:
            merged_df = df

        print("✅ 추가된 row 수:", len(df))
        print("✅ 병합된 총 row 수:", len(merged_df))

        merged_df.to_csv(total_path, index=False)
        print("✅ 저장 완료:", total_path)

        return {"status": "success", "filename": file.filename}

    except Exception as e:
        print("❌ 예외 발생:")
        traceback.print_exc()
        return {"status": "error", "detail": str(e)}
