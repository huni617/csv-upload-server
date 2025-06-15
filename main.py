from fastapi import FastAPI, UploadFile, File
import shutil, os

app = FastAPI()

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    os.makedirs("uploads", exist_ok=True)
    filepath = os.path.join("uploads", file.filename)
    with open(filepath, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"status": "received", "filename": file.filename}
