ifrom fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Quiz works"}

@app.get("/ping")
def ping():
    return "OK"

