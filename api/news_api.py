from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Optional
from datetime import datetime
import json
from pathlib import Path

from services.database_service import get_database_service

router = APIRouter()

@router.get("/latest")
async def get_latest_news_issues():
    """최신 뉴스 이슈들을 MySQL에서 조회합니다."""
    try:
        db_service = get_database_service()
        
        news_issues = await db_service.get_latest_news_issues()
        
        if not news_issues:
            # MySQL에 데이터가 없으면 fallback: 최신 JSON 파일에서 로드
            fallback_data = _load_fallback_data()
            if fallback_data:
                return {
                    "success": True,
                    "data": {
                        "issues": fallback_data,
                        "count": len(fallback_data),
                        "source": "파일 백업 데이터",
                        "last_updated": "백그라운드 업데이트 대기 중"
                    }
                }
            else:
                return {
                    "success": True,
                    "data": {
                        "issues": [],
                        "count": 0,
                        "source": "데이터 없음",
                        "message": "백그라운드 파이프라인이 첫 실행을 완료할 때까지 기다려주세요."
                    }
                }
        
        return {
            "success": True,
            "data": {
                "issues": news_issues,
                "count": len(news_issues),
                "source": "MySQL 실시간 데이터",
                "last_updated": news_issues[0].get("updated_at") if news_issues else None
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"뉴스 조회 실패: {e}")

@router.get("/pipeline-status")
async def get_pipeline_status():
    """백그라운드 파이프라인의 최근 실행 상태를 조회합니다."""
    try:
        db_service = get_database_service()
        latest_log = await db_service.get_latest_pipeline_log()
        
        return {
            "success": True,
            "data": latest_log or {
                "status": "대기 중",
                "message": "백그라운드 파이프라인이 아직 실행되지 않았습니다."
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파이프라인 상태 조회 실패: {e}")