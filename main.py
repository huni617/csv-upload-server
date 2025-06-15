from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import dropbox
from datetime import datetime, timedelta
from io import BytesIO

app = FastAPI()

# CORS 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dropbox Access Token
DROPBOX_ACCESS_TOKEN = "sl.u.AFy4l1nm9q4IAEad-2cFqCN-1yeORU-lqGTSLPrWvf2ptpA-LqaWcWXLiHSJJ-PeRmbZFUw9PgS0bExQ0yGe_GG1uKxdRhHjCq-INy9GYOOSrtlmkXlQlaCTi_gtgJ9c9WexpJ8O80d-Z2YCrB8TFKzIQZTSeS5DRzdv0oEYs9CmtpjSieHimRZste8eQZc1j4gfO2o5AYHbk0OsIIRz2UX7M8gUcrs8T7caLlIYh9eHw5InW0nSktjA71FfmhhvudqJ7N9SCz7vaU5ZpJTNB94838gg5VCAZhjFPFeZE5_T8diWK-l8NSUTPqYr3TrjlDu8InSomqdN3qrHnc3v1XVzn3vMK4ckzYYa5xarRMWE9RekIRuXi3jX_bQ3jOQeqis4LzR4IWutWJfiz2-p4f4ITdO2RlcqAEKISbH89reNPGsB9POT6d0-r7BFAU3sbMYokFXrjPpGaAMWrGdHpiYxVKVnmNB3I_ldn1uCi1lpC4-7SpJTjloghHXEoKHjLILTdApaW57Qos9aEhIStsabBg0_M0PLntnSNdg7OhR2JTpc81XpQOnuff2iU9eVBMATyyUPnO8dWUEWh8R8f4ROzG8IQB6gYv_eqimy5QJmvRKGq404Y8hWE7qyErfDf8zIexdJYYx2cfm6t-90ZDUwC84qB5tyH6zNoC6rsqFrDn7mntJXmsMc8tCraQPUKq_sQHDgLuFKjqjcbrSrlaCtPoAPFMG6wxmvahy_m8ndCnYFcYa7sHYbjJUGDAvICROxa7y-YRY7b8t0Kh5wxhKEdINcSe-TTHrlRKymbvPYTOLQE4MSU2OtDK4y35-UMUOz_VGM_EYJKeUB0x-gcqXQ8aBz6gN-H3IVlQrrKT85LMea9P4Nx6U566UP5s6DeVxj6ryqVgGrGN0rdbkJpymBJIjzYAgFuR4TaCg_6fkBeyaeEEQaNuhWs95lfc-2E_irr1Q8vu2-RJR9GWOX94wuxmVWSg7eEaqF3YoTGPzAVx6k1jcb9VQ-BgZuupxIWxioMu0hGYeeyKRqzKljvUHvwfcxOnumAe0zCMTl7PVpbJ2CdpAzOmsaP4zSWM5qqcLS_Je_evO2nBB3TiXB5RREAqFtpSopkOGdILdBghU5IO3yo0OD7CKzZ6FmnVmnqoyaiqlnCKlSovuk2Stbip3diOuDY9FSUtqtJIgp1mEX8n4tg-VoRYdK0RHbNHg7vdLt_vG9LUwTAHtkPx28058VYk3FATSLba0q_kSPyFJuPDuspdGPhqNzr3SfFrtb7mmkBA2cCWW92fB8E_kYbEJU9GsoL4XUV_kvIRAVVI_pkYgthHnS5djQU00rZY3NiuZgKLVJz2P4XDsLL1oY-f0Q5YuUaMTyusqoORH5_H3NiEaAbvnN1KluOyfSdwWZGLQcz67nW1UpNYUl_BsTiJ1y-8uzHSdjFc1jBKL9TDY8DA"
dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

# 경로 정의
UPLOAD_PATH = "/AmazonReports"
MERGED_PATH = "/MergedData/total.csv"

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    try:
        filename = file.filename
        file_bytes = await file.read()

        # 📌 1. 새 CSV 업로드 → Dropbox 저장
        upload_target = f"{UPLOAD_PATH}/{filename}"
        dbx.files_upload(file_bytes, upload_target, mode=dropbox.files.WriteMode.overwrite)

        # 📌 2. 업로드된 CSV 파일 불러오기
        downloaded_file = dbx.files_download(upload_target)[1].content
        df_new = pd.read_csv(BytesIO(downloaded_file))

        # 📌 3. 날짜 계산 및 열 추가
        date_str = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")
        df_new.insert(0, "Date", date_str)
        df_new.insert(1, "Month Week", "")

        # 📌 4. 숫자형 컬럼 변환
        columns_to_sum = [
            "Sessions - Total", "Sessions - Total - B2B",
            "Page Views - Total", "Page Views - Total - B2B",
            "Units Ordered", "Units Ordered - B2B",
            "Ordered Product Sales", "Ordered Product Sales - B2B"
        ]
        for col in columns_to_sum:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors='coerce').fillna(0)
            else:
                df_new[col] = 0

        # 📌 5. 합산 열 추가
        if "Title" in df_new.columns:
            title_index = df_new.columns.get_loc("Title")
        else:
            return {"error": "❌ 'Title' 컬럼을 찾을 수 없습니다."}

        df_new.insert(title_index + 1, "Total Session", df_new["Sessions - Total"] + df_new["Sessions - Total - B2B"])
        df_new.insert(title_index + 2, "Total Page View", df_new["Page Views - Total"] + df_new["Page Views - Total - B2B"])
        df_new.insert(title_index + 3, "Total Units Ordered", df_new["Units Ordered"] + df_new["Units Ordered - B2B"])
        df_new.insert(title_index + 4, "Total Ordered Product Sales", df_new["Ordered Product Sales"] + df_new["Ordered Product Sales - B2B"])
        df_new["Total Conversion Rate"] = round((df_new["Total Units Ordered"] / df_new["Total Session"]) * 100, 2)

        # 📌 6. 기존 total.csv 다운로드
        try:
            merged_data = dbx.files_download(MERGED_PATH)[1].content
            df_total = pd.read_csv(BytesIO(merged_data))
            df_merged = pd.concat([df_total, df_new], ignore_index=True)
        except dropbox.exceptions.ApiError:
            # 처음 업로드인 경우
            df_merged = df_new

        # 📌 7. 병합된 total.csv 다시 Dropbox에 업로드
        output_buffer = BytesIO()
        df_merged.to_csv(output_buffer, index=False)
        output_buffer.seek(0)
        dbx.files_upload(output_buffer.read(), MERGED_PATH, mode=dropbox.files.WriteMode.overwrite)

        return {
            "✅ 추가된 row 수": len(df_new),
            "✅ 병합된 총 row 수": len(df_merged),
            "✅ 저장 완료": MERGED_PATH
        }

    except Exception as e:
        return {"error": str(e)}
