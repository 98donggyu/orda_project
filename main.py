# main.py
import uvicorn
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# config 및 api, services 임포트
from config import API_TITLE, API_VERSION, API_DESCRIPTION, CORS_ALLOW_ORIGINS
from api import health_api, analysis_api, database_api, news_api, simulation_api
from services import database_service

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

# 정적 파일 설정
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# API 라우터 등록 (수정됨)
app.include_router(health_api.router, tags=["Health Check"])
app.include_router(news_api.router, prefix="/api/news", tags=["News"])  # 새로 추가
app.include_router(analysis_api.router, prefix="/api/analysis", tags=["Analysis"])
app.include_router(simulation_api.router, prefix="/api/simulation", tags=["Simulation"])
app.include_router(database_api.router, prefix="/api/database", tags=["Database"])

# 루트 경로를 메인 페이지로 리디렉션
@app.get("/")
async def root():
    return RedirectResponse(url="/static/index.html")

# 서버 시작/종료 이벤트 (간소화)
@app.on_event("startup")
async def startup_event():
    print("🚀 서버 시작: MySQL 데이터베이스 연결을 확인합니다...")
    try:
        database_service.initialize()
        print("✅ 데이터베이스 연결 성공")
    except Exception as e:
        print(f"⚠️ 데이터베이스 연결 실패: {e}")
        print("📝 백그라운드 파이프라인이 데이터를 생성할 때까지 기다립니다.")

@app.on_event("shutdown")
def shutdown_event():
    print("👋 서버를 종료합니다.")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )