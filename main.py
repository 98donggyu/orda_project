# main.py - lifespan 이벤트로 수정된 버전
import uvicorn
import asyncio
import threading
import time
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# config 및 api, services 임포트
from config import API_TITLE, API_VERSION, API_DESCRIPTION, CORS_ALLOW_ORIGINS
from api import health_api, analysis_api, database_api, news_api, simulation_api
from services import database_service

# 백그라운드 파이프라인 import
from background_pipeline import BackgroundPipelineExecutor

# 백그라운드 파이프라인 전역 변수
pipeline_executor = None
pipeline_thread = None

def run_background_pipeline():
    """백그라운드 스레드에서 파이프라인 실행"""
    global pipeline_executor
    
    try:
        print("🔄 백그라운드 파이프라인 스레드 시작...")
        
        # 파이프라인 실행기 초기화
        pipeline_executor = BackgroundPipelineExecutor()
        
        # 시작 시 즉시 1회 실행
        print("🎬 서버 시작 시 초기 파이프라인 실행...")
        pipeline_executor.run_once()
        
        # 30분마다 실행하는 루프
        print("⏰ 30분 간격 스케줄러 시작...")
        while True:
            time.sleep(1800)  # 30분 = 1800초
            print("🔔 30분 경과 - 파이프라인 재실행...")
            try:
                pipeline_executor.run_once()
            except Exception as e:
                print(f"❌ 스케줄 실행 실패: {e}")
                # 에러가 발생해도 스케줄링은 계속
                continue
                
    except Exception as e:
        print(f"❌ 백그라운드 파이프라인 스레드 실패: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan 이벤트 핸들러"""
    global pipeline_thread
    
    # Startup
    print("🚀 서버 시작: 서비스들을 초기화합니다...")
    
    # 1. 서비스 초기화
    try:
        from services import initialize_all_services
        success = initialize_all_services()
        if success:
            print("✅ 모든 서비스 초기화 완료")
        else:
            print("⚠️ 일부 서비스 초기화 실패 - 백그라운드에서 재시도")
    except Exception as e:
        print(f"⚠️ 서비스 초기화 오류: {e}")
        print("📝 백그라운드 파이프라인이 데이터를 생성할 때까지 기다립니다.")
    
    # 2. 백그라운드 파이프라인 시작
    print("🔄 백그라운드 파이프라인 시작...")
    pipeline_thread = threading.Thread(target=run_background_pipeline, daemon=True)
    pipeline_thread.start()
    print("✅ 백그라운드 파이프라인 스레드 시작됨")
    
    yield  # 서버 실행
    
    # Shutdown
    print("👋 서버를 종료합니다...")
    
    # 백그라운드 파이프라인 안전 종료
    if pipeline_executor:
        try:
            pipeline_executor.shutdown()
            print("✅ 백그라운드 파이프라인 종료 완료")
        except Exception as e:
            print(f"⚠️ 백그라운드 파이프라인 종료 실패: {e}")
    
    print("✅ 서버 종료 완료")

# FastAPI 앱 생성 (lifespan 추가)
app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description=API_DESCRIPTION,
    lifespan=lifespan
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

# API 라우터 등록
app.include_router(health_api.router, tags=["Health Check"])
app.include_router(news_api.router, prefix="/api/news", tags=["News"])
app.include_router(analysis_api.router, prefix="/api/analysis", tags=["Analysis"])
app.include_router(simulation_api.router, prefix="/api/simulation", tags=["Simulation"])
app.include_router(database_api.router, prefix="/api/database", tags=["Database"])

# 루트 경로를 메인 페이지로 리디렉션
@app.get("/")
async def root():
    return RedirectResponse(url="/static/index.html")

# 수동 파이프라인 트리거 API
@app.post("/api/pipeline/trigger")
async def trigger_pipeline_manually():
    """수동으로 파이프라인을 즉시 실행합니다."""
    global pipeline_executor
    
    if pipeline_executor is None:
        return {
            "success": False,
            "message": "백그라운드 파이프라인이 초기화되지 않았습니다."
        }
    
    if pipeline_executor.is_running:
        return {
            "success": False,
            "message": "파이프라인이 이미 실행 중입니다. 잠시 후 다시 시도해주세요."
        }
    
    try:
        # 별도 스레드에서 실행 (API 응답 지연 방지)
        def run_async():
            pipeline_executor.run_once()
        
        threading.Thread(target=run_async, daemon=True).start()
        
        return {
            "success": True,
            "message": "백그라운드 파이프라인이 수동으로 시작되었습니다.",
            "estimated_time": "약 5-10분 소요 예상"
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"파이프라인 실행 실패: {e}"
        }

@app.get("/api/pipeline/status")
async def get_pipeline_status():
    """백그라운드 파이프라인의 현재 상태를 조회합니다."""
    global pipeline_executor
    
    if pipeline_executor is None:
        return {
            "success": True,
            "data": {
                "initialized": False,
                "is_running": False,
                "message": "백그라운드 파이프라인이 초기화되지 않았습니다."
            }
        }
    
    try:
        # 최근 파이프라인 실행 로그 조회 (데이터베이스에서)
        db_service = database_service.get_database_service()
        latest_log = db_service.get_latest_pipeline_log()
        
        return {
            "success": True,
            "data": {
                "initialized": True,
                "is_running": pipeline_executor.is_running,
                "latest_execution": latest_log,
                "schedule": "30분마다 자동 실행",
                "manual_trigger_available": not pipeline_executor.is_running
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"상태 조회 실패: {e}"
        }

if __name__ == "__main__":
    print("🎯 오르다 투자 학습 플랫폼 API 서버")
    print("=" * 50)
    print("📊 MySQL + Docker + 백그라운드 자동 파이프라인")
    print("🌐 http://localhost:8000")
    print("📋 API 문서: http://localhost:8000/docs")
    print("🏠 프론트엔드: http://localhost:8000/static/index.html")
    print("🔄 백그라운드 파이프라인: 30분마다 자동 실행")
    print("=" * 50)
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # reload=False로 변경 (백그라운드 스레드 충돌 방지)
        log_level="info"
    )