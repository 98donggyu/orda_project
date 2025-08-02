# api/news_api.py

from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Optional
from datetime import datetime
import json
from pathlib import Path

from services.database_service import DatabaseService
from models.schemas import NewsIssue, NewsListResponse

router = APIRouter()

@router.get("/latest", response_model=NewsListResponse)
async def get_latest_news_issues():
    """
    최신 뉴스 이슈들을 MySQL에서 조회합니다.
    백그라운드 파이프라인에서 생성된 RAG 분석 결과를 포함합니다.
    """
    try:
        db_service = DatabaseService()
        
        # MySQL에서 최신 뉴스 조회
        news_issues = await db_service.get_latest_news_issues()
        
        if not news_issues:
            # MySQL에 데이터가 없으면 fallback: 최신 JSON 파일에서 로드
            fallback_data = _load_fallback_data()
            if fallback_data:
                return NewsListResponse(
                    success=True,
                    data={
                        "issues": fallback_data,
                        "count": len(fallback_data),
                        "source": "파일 백업 데이터",
                        "last_updated": "백그라운드 업데이트 대기 중"
                    }
                )
            else:
                return NewsListResponse(
                    success=True,
                    data={
                        "issues": [],
                        "count": 0,
                        "source": "데이터 없음",
                        "message": "백그라운드 파이프라인이 첫 실행을 완료할 때까지 기다려주세요."
                    }
                )
        
        return NewsListResponse(
            success=True,
            data={
                "issues": news_issues,
                "count": len(news_issues),
                "source": "MySQL 실시간 데이터",
                "last_updated": news_issues[0].get("updated_at") if news_issues else None
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"뉴스 조회 실패: {e}")

@router.get("/issue/{issue_id}", response_model=Dict)
async def get_issue_detail(issue_id: int):
    """특정 이슈의 상세 정보를 조회합니다 (관련 산업, 과거 이슈 포함)."""
    try:
        db_service = DatabaseService()
        issue_detail = await db_service.get_issue_with_relations(issue_id)
        
        if not issue_detail:
            raise HTTPException(status_code=404, detail="해당 이슈를 찾을 수 없습니다.")
        
        return {
            "success": True,
            "data": issue_detail
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"이슈 상세 조회 실패: {e}")

@router.get("/pipeline-status")
async def get_pipeline_status():
    """백그라운드 파이프라인의 최근 실행 상태를 조회합니다."""
    try:
        db_service = DatabaseService()
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

def _load_fallback_data() -> List[Dict]:
    """MySQL 데이터가 없을 때 최신 JSON 파일에서 백업 데이터 로드"""
    try:
        data_dir = Path(__file__).parent.parent / "data2"
        
        # 최신 RAG Enhanced 파일 찾기
        rag_files = list(data_dir.glob("*_RealRAG_Enhanced_*issues.json"))
        if rag_files:
            latest_file = max(rag_files, key=lambda f: f.stat().st_mtime)
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('selected_issues', [])
        
        # RAG 파일이 없으면 필터링된 파일 찾기
        filtered_files = list(data_dir.glob("*_StockFiltered_*issues.json"))
        if filtered_files:
            latest_file = max(filtered_files, key=lambda f: f.stat().st_mtime)
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('selected_issues', [])
        
        return []
        
    except Exception as e:
        print(f"백업 데이터 로드 실패: {e}")
        return []