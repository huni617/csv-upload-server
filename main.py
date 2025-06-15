from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import pandas as pd
import os
import dropbox

# Dropbox Access Token
DROPBOX_ACCESS_TOKEN = "sl.u.AFy4l1nm9q4IAEad-2cFqCN-1yeORU-lqGTSLPrWvf2ptpA-LqaWcWXLiHSJJ-PeRmbZFUw9PgS0bExQ0yGe_GG1uKxdRhHjCq-INy9GYOOSrtlmkXlQlaCTi_gtgJ9c9WexpJ8O80d-Z2YCrB8TFKzIQZTSeS5DRzdv0oEYs9CmtpjSieHimRZste8eQZc1j4gfO2o5AYHbk0OsIIRz2UX7M8gUcrs8T7caLlIYh9eHw5InW0nSktjA71FfmhhvudqJ7N9SCz7vaU5ZpJTNB94838gg5VCAZhjFPFeZE5_T8diWK-l8NSUTPqYr3TrjlDu8InSomqdN3qrHnc3v1XVzn3vMK4ckzYYa5xarRMWE9RekIRuXi3jX_bQ3jOQeqis4LzR4IWutWJfiz2-p4f4ITdO2RlcqAEKISbH89reNPGsB9POT6d0-r7BFAU3sbMYokFXrjPpGaAMWrGdHpiYxVKVnmNB3I_ldn1uCi1lpC4-7SpJTjloghHXEoKHjLILTdApaW57Qos9aEhIStsabBg0_M0PLntnSNdg7OhR2JTpc81XpQOnuff2iU9eVBMATyyUPnO8dWUEWh8R8f4ROzG8IQB6gYv_eqimy5QJmvRKGq404Y8hWE7qyErfDf8zIexdJYYx2cfm6t-90ZDUwC84qB5tyH6zNoC6rsqFrDn7mntJXmsMc8tCraQPUKq_sQHDgLuFKjqjcbrSrlaCtPoAPFMG6wxmvahy_m8ndCnYFcYa7sHYbjJUGDAvICROxa7y-YRY7b8t0Kh5wxhKEdINcSe-TTHrlRKymbvPYTOLQE4MSU2OtDK4y35-UMUOz_VGM_EYJKeUB0x-gcqXQ8aBz6gN-H3IVlQrrKT85LMea9P4Nx6U566UP5s6DeVxj6ryqVgGrGN0rdbkJpymBJIjzYAgFuR4TaCg_6fkBeyaeEEQaNuhWs95lfc-2E_irr1Q8vu2-RJR9GWOX94wuxmVWSg7eEaqF3YoTGPzAVx6k1jcb9VQ-BgZuupxIWxioMu0hGYeeyKRqzKljvUHvwfcxOnumAe0zCMTl7PVpbJ2CdpAzOmsaP4zSWM5qqcLS_Je_evO2nBB3TiXB5RREAqFtpSopkOGdILdBghU5IO3yo0OD7CKzZ6FmnVmnqoyaiqlnCKlSovuk2Stbip3diOuDY9FSUtqtJIgp1mEX8n4tg-VoRYdK0RHbNHg7vdLt_vG9LUwTAHtkPx28058VYk3FATSLba0q_kSPyFJuPDuspdGPhqNzr3SfFrtb7mmkBA2cCWW92fB8E_kYbEJU9GsoL4XUV_kvIRAVVI_pkYgthHnS5djQU00rZY3NiuZgKLVJz2P4XDsLL1oY-f0Q5YuUaMTyusqoORH5_H3NiEaAbvnN1KluOyfSdwWZGLQcz67nW1UpNYUl_BsTiJ1y-8uzHSdjFc1jBKL9TDY8DA"
dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

# CORS 설정
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 경로 정의 (Dropbox 앱 폴더 내 경로)
UPLOAD_PATH = "/mnt/data/AmazonReports"
MERGED_PATH = "/mnt/data/MergedData/total.csv"

os.makedirs(UPLOAD_PATH, exist_ok=True)
os.makedirs(os.path.dirname(MERGED_PATH), exist_ok=True)

# 보조 함수: Dropbox 경로에 폴더 존재 확인 후 없으면 생성
def ensure_dropbox_folder(path):
    try:
        dbx.files_get_metadata(path)
    except dropbox.exceptions.ApiError:
        dbx.files_create_folder_v2(path)

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    try:
        # 날짜 설정
        today = datetime.now().date()
        target_date = today - timedelta(days=2)
        date_str = target_date.strftime("%Y-%m-%d")

        # 경로 보장
        ensure_dropbox_folder("/csv file merge/AmazonReports")
        ensure_dropbox_folder("/csv file merge/MergedData")

        # 업로드 파일 내용 로딩
        contents = await file.read()
        df = pd.read_csv(pd.compat.StringIO(contents.decode('utf-8')))

        # Date 및 Month Week 추가
        df.insert(0, "Date", date_str)
        df.insert(1, "Month Week", "")

        # 숫자형 컬럼 변환
        numeric_columns = [
            "Sessions - Total", "Sessions - Total - B2B",
            "Page Views - Total", "Page Views - Total - B2B",
            "Units Ordered", "Units Ordered - B2B",
            "Ordered Product Sales", "Ordered Product Sales - B2B"
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        # Total 계산
        if "Title" in df.columns:
            idx = df.columns.get_loc("Title")
            df.insert(idx + 1, "Total Session", df["Sessions - Total"] + df["Sessions - Total - B2B"])
            df.insert(idx + 2, "Total Page View", df["Page Views - Total"] + df["Page Views - Total - B2B"])
            df.insert(idx + 3, "Total Units Ordered", df["Units Ordered"] + df["Units Ordered - B2B"])
            df.insert(idx + 4, "Total Ordered Product Sales", df["Ordered Product Sales"] + df["Ordered Product Sales - B2B"])
            df["Total Conversion Rate"] = round((df["Total Units Ordered"] / df["Total Session"].replace(0, 1)) * 100, 2)
        else:
            return {"error": "'Title' 컬럼이 없습니다."}

        # 기존 total.csv 다운로드 시도
        try:
            _, res = dbx.files_download(MERGED_PATH)
            existing_df = pd.read_csv(res.raw)
            df_merged = pd.concat([existing_df, df], ignore_index=True)
        except dropbox.exceptions.ApiError:
            df_merged = df  # 기존 파일 없음

        # 새로 병합한 total.csv Dropbox에 업로드
        from io import StringIO
        csv_buffer = StringIO()
        df_merged.to_csv(csv_buffer, index=False)
        dbx.files_upload(csv_buffer.getvalue().encode(), MERGED_PATH, mode=dropbox.files.WriteMode.overwrite)

        return {
            "✅ 추가된 row 수": len(df),
            "✅ 병합된 총 row 수": len(df_merged),
            "✅ 저장 완료 위치": MERGED_PATH
        }

    except Exception as e:
        return {"error": str(e)}
