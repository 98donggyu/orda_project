# api/pipeline_api.py
from fastapi import APIRouter, BackgroundTasks, HTTPException
from typing import List

from services import pipeline_service
from models.schemas import CurrentIssue

router = APIRouter()

# --- [핵심 수정] pipeline_service 호출 시 'await' 추가 ---
@router.get("/today-issues", response_model=List[CurrentIssue])
async def get_today_issues():
    """
    오늘의 주요 이슈 5개를 RAG 분석 결과와 함께 반환합니다.
    캐시된 최신 데이터를 반환하며, 데이터가 없으면 파이프라인을 실행합니다.
    """
    try:
        # get_latest_analyzed_issues가 async 함수가 되었으므로 await로 호출
        issues = await pipeline_service.get_latest_analyzed_issues()
        if not issues:
            return []
        return issues
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"이슈 조회 실패: {e}")

@router.post("/refresh-issues")
async def refresh_all_issues(background_tasks: BackgroundTasks):
    """
    백그라운드에서 전체 데이터 파이프라인을 실행하여 오늘의 이슈를 새로고침합니다.
    (크롤링 -> 필터링 -> 분석)
    """
    # BackgroundTasks는 비동기 함수도 잘 처리합니다.
    background_tasks.add_task(pipeline_service.run_full_pipeline)
    return {"message": "오늘의 이슈 데이터 새로고침을 시작합니다. 약 3~5분 소요됩니다."}