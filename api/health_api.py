# api/health_api.py
from fastapi import APIRouter
from datetime import datetime

from models.schemas import HealthResponse
from services import database_service, rag_service, simulation_service, crawling_service

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    API 서버와 모든 백엔드 서비스의 상태를 확인합니다.
    """
    components = {
        "database": database_service.get_health(),
        "crawling": crawling_service.get_health(),
        "rag": await rag_service.get_health(),
        "simulation": simulation_service.get_health(),
    }

    is_healthy = all(comp["status"] == "ok" for comp in components.values())
    
    return HealthResponse(
        status="ok" if is_healthy else "degraded",
        timestamp=datetime.now().isoformat(),
        components=components,
    )