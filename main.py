# main.py
import uvicorn
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# config 및 api, services 임포트
from config import API_TITLE, API_VERSION, API_DESCRIPTION, CORS_ALLOW_ORIGINS
from api import health_api, analysis_api, database_api, pipeline_api, simulation_api
from services import rag_service, database_service, simulation_service

# FastAPI 앱 생성
app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description=API_DESCRIPTION,
)

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- [핵심] 정적 파일(Static Files) 설정 ---
# 이 부분이 static 폴더를 웹에 노출시키는 역할을 합니다.
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# API 라우터 포함
app.include_router(health_api.router, tags=["Health Check"])
app.include_router(pipeline_api.router, tags=["Pipeline"])
app.include_router(analysis_api.router, tags=["Analysis"])
app.include_router(simulation_api.router, tags=["Simulation"])
app.include_router(database_api.router, tags=["Database"])

# --- 루트 경로('/')를 프론트엔드 메인 페이지로 리디렉션 ---
@app.get("/")
async def root():
    return RedirectResponse(url="/static/index.html", status_code=302)

# --- 서버 시작/종료 이벤트 ---
@app.on_event("startup")
async def startup_event():
    print("🚀 서버 시작: 서비스 초기화를 진행합니다...")
    print("📁 static_dir 절대경로:", static_dir.resolve())
    print("📄 index.html 존재 여부:", (static_dir / "index.html").exists())
    database_service.initialize()
    rag_service.initialize()
    simulation_service.initialize()
    print("✅ 모든 서비스가 성공적으로 초기화되었습니다.")

@app.on_event("shutdown")
def shutdown_event():
    print("👋 서버를 종료합니다.")

# --- 메인 실행 ---
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
   