# api/simulation_api.py
from fastapi import APIRouter, HTTPException
from typing import List, Optional  # [수정] Optional을 여기서 import 합니다.

from models.schemas import (
    SimulationRequest, SimulationResponse, ValidationResponse,
    Scenario, RecommendedStockInfo
)
from services import simulation_service, database_service

router = APIRouter()

@router.get("/scenarios", response_model=List[Scenario])
async def get_scenarios():
    """사용 가능한 모든 모의투자 시나리오 목록을 반환합니다."""
    if not simulation_service.is_initialized():
        raise HTTPException(status_code=503, detail="시뮬레이션 서비스가 준비되지 않았습니다.")
    return simulation_service.engine.get_available_scenarios()

@router.get("/scenarios/{scenario_id}/recommended-stocks", response_model=RecommendedStockInfo)
async def get_recommended_stocks(scenario_id: str):
    """특정 시나리오에 대한 추천 종목 목록을 반환합니다."""
    if not simulation_service.is_initialized():
        raise HTTPException(status_code=503, detail="시뮬레이션 서비스가 준비되지 않았습니다.")
        
    stocks = simulation_service.engine.get_recommended_stocks_for_scenario(scenario_id)
    if not stocks:
        raise HTTPException(status_code=404, detail="해당 시나리오의 추천 종목을 찾을 수 없습니다.")
    return {"scenario_id": scenario_id, "recommended_stocks": stocks}

@router.post("/validate-simulation", response_model=ValidationResponse)
async def validate_simulation(request: SimulationRequest):
    """모의투자 실행 전 입력값을 검증합니다."""
    if not simulation_service.is_initialized():
        raise HTTPException(status_code=503, detail="시뮬레이션 서비스가 준비되지 않았습니다.")
    
    validation_result = await simulation_service.engine.validate_simulation_inputs(
        request.scenario_id,
        request.investment_amount,
        request.investment_period,
        request.selected_stocks,
    )
    return validation_result

@router.post("/run-simulation", response_model=SimulationResponse)
async def run_simulation(request: SimulationRequest):
    """모의투자를 실행하고 그 결과를 반환합니다."""
    if not simulation_service.is_initialized() or not database_service.is_initialized():
        raise HTTPException(status_code=503, detail="필요한 서비스가 준비되지 않았습니다.")

    validation = await simulation_service.engine.validate_simulation_inputs(
        request.scenario_id, request.investment_amount, request.investment_period, request.selected_stocks
    )
    if not validation["valid"]:
        raise HTTPException(status_code=400, detail=validation)

    try:
        result = await simulation_service.engine.run_simulation(
            request.scenario_id, request.investment_amount, request.investment_period, request.selected_stocks
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"시뮬레이션 실행 중 오류: {e}")

    try:
        await database_service.db_api.save_simulation_result(request, result)
    except Exception as e:
        print(f"Warning: 시뮬레이션 결과 저장 실패 - {e}")

    return result

# [추가된 API] 기업 목록 제공
@router.get("/companies")
async def get_companies_for_simulation(sector: Optional[str] = None, query: Optional[str] = None):
    """모의투자에 사용될 기업 목록을 반환합니다."""
    if not simulation_service.is_initialized():
        raise HTTPException(status_code=503, detail="시뮬레이션 서비스가 준비되지 않았습니다.")
    
    # 예시 데이터 (실제로는 DB 연동 필요)
    all_companies = [
        {"code": "005930", "name": "삼성전자", "sector": "반도체", "market_cap": "447조원", "per": "15.2", "pbr": "1.4", "price": 74800, "change": "+1200", "change_rate": "+1.63%"},
        {"code": "000660", "name": "SK하이닉스", "sector": "반도체", "market_cap": "65조원", "per": "12.8", "pbr": "1.1", "price": 89600, "change": "-800", "change_rate": "-0.88%"},
        {"code": "010950", "name": "S-OIL", "sector": "정유", "market_cap": "8.2조원", "per": "8.5", "pbr": "0.9", "price": 68900, "change": "+2100", "change_rate": "+3.14%"},
        {"code": "047810", "name": "한국항공우주", "sector": "방위산업", "market_cap": "1.9조원", "per": "18.2", "pbr": "2.1", "price": 42350, "change": "+1850", "change_rate": "+4.57%"},
        {"code": "051910", "name": "LG화학", "sector": "화학", "market_cap": "27조원", "per": "22.1", "pbr": "1.8", "price": 385000, "change": "-5000", "change_rate": "-1.28%"},
        {"code": "035720", "name": "카카오", "sector": "IT 서비스", "market_cap": "25조원", "per": "N/A", "pbr": "2.3", "price": 58400, "change": "+900", "change_rate": "+1.56%"},
    ]

    filtered_companies = all_companies
    if sector:
        filtered_companies = [c for c in filtered_companies if c['sector'] == sector]
    
    if query:
        query_lower = query.lower()
        filtered_companies = [
            c for c in filtered_companies 
            if query_lower in c['name'].lower() or query_lower in c['code']
        ]

    return {"success": True, "data": filtered_companies}