# api/pipeline_api.py
from fastapi import APIRouter, HTTPException
from typing import Dict
import subprocess
import os
from pathlib import Path

from services.database_service import DatabaseService

router = APIRouter()

@router.post("/trigger-update")
async def trigger_background_update():
    """
    백그라운드 파이프라인을 수동으로 트리거합니다.
    (실제로는 별도 프로세스로 실행됨)
    """
    try:
        # 백그라운드 파이프라인 스크립트 실행
        script_path = Path(__file__).parent.parent / "background_pipeline.py"
        
        if not script_path.exists():
            return {
                "success": False,
                "message": "백그라운드 파이프라인 스크립트가 없습니다. background_pipeline.py를 생성해주세요."
            }
        
        # 비동기로 백그라운드 프로세스 시작
        process = subprocess.Popen([
            "python", str(script_path)
        ], cwd=str(script_path.parent))
        
        return {
            "success": True,
            "message": "백그라운드 파이프라인이 시작되었습니다.",
            "process_id": process.pid,
            "estimated_time": "약 5-10분 소요 예상"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파이프라인 실행 실패: {e}")

@router.get("/status")
async def get_pipeline_status():
    """백그라운드 파이프라인의 현재 상태를 조회합니다."""
    try:
        db_service = DatabaseService()
        
        # 최근 파이프라인 실행 로그 조회
        latest_log = await db_service.get_latest_pipeline_log()
        
        # 현재 실행 중인 프로세스 확인 (예시)
        is_running = _check_pipeline_process_running()
        
        return {
            "success": True,
            "data": {
                "is_running": is_running,
                "latest_execution": latest_log,
                "next_scheduled": "30분마다 자동 실행"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"상태 조회 실패: {e}")

def _check_pipeline_process_running() -> bool:
    """백그라운드 파이프라인 프로세스가 실행 중인지 확인"""
    try:
        # ps 명령어로 background_pipeline.py 실행 중인지 확인
        result = subprocess.run(
            ["pgrep", "-f", "background_pipeline.py"],
            capture_output=True,
            text=True
        )
        return len(result.stdout.strip()) > 0
    except:
        return False

# =============================================================================
# api/analysis_api.py (수정된 버전 - 단순 조회용)
# =============================================================================
from fastapi import APIRouter, HTTPException
from typing import Dict, List

from services.database_service import DatabaseService

router = APIRouter()

@router.get("/summary")
async def get_analysis_summary():
    """전체 뉴스 분석 결과 요약을 반환합니다."""
    try:
        db_service = DatabaseService()
        
        # 산업별 이슈 수 통계
        industry_stats = await db_service.get_industry_analysis_stats()
        
        # RAG 분석 신뢰도 분포
        confidence_stats = await db_service.get_confidence_distribution()
        
        # 최근 트렌드
        recent_trends = await db_service.get_recent_trend_analysis()
        
        return {
            "success": True,
            "data": {
                "industry_distribution": industry_stats,
                "confidence_distribution": confidence_stats,
                "recent_trends": recent_trends,
                "summary": "AI 기반 RAG 분석으로 뉴스와 관련 산업/과거 이슈를 연결하여 투자 인사이트를 제공합니다."
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"분석 요약 조회 실패: {e}")

@router.get("/industries/{industry_name}")
async def get_industry_analysis(industry_name: str):
    """특정 산업에 대한 분석 결과를 반환합니다."""
    try:
        db_service = DatabaseService()
        
        # 해당 산업 관련 뉴스들
        related_news = await db_service.get_news_by_industry(industry_name)
        
        # 산업 상세 정보
        industry_info = await db_service.get_industry_info(industry_name)
        
        return {
            "success": True,
            "data": {
                "industry_info": industry_info,
                "related_news": related_news,
                "analysis_count": len(related_news)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"산업 분석 조회 실패: {e}")