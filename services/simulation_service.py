# services/simulation_service.py
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional
import asyncio
from dataclasses import dataclass

# --- Pydantic Schemas for API ---
# API 통신에 사용되는 Pydantic 모델만 schemas 파일에서 가져옵니다.
from models.schemas import SimulationRequest, StockSelection

# --- Configuration ---
from config import YFINANCE_TICKER_SUFFIX_KOSPI, YFINANCE_TICKER_SUFFIX_KOSDAQ

# --- Service-Internal Data Structures ---
# 서비스 내부 로직에서만 사용할 데이터 클래스를 여기에 직접 정의합니다.
@dataclass
class SimulationStock:
    """시뮬레이션용 종목 정보 (내부용)"""
    code: str
    name: str
    industry: str
    allocation: float
    amount: int

@dataclass
class SimulationScenario:
    """시뮬레이션 시나리오 (내부용)"""
    id: str
    name: str
    description: str
    start_date: str
    end_date: str
    related_industries: List[str]

class SimulationEngine:
    """과거 데이터를 기반으로 모의투자를 실행하는 엔진"""
    def __init__(self):
        self.scenarios: Dict[str, SimulationScenario] = self._load_scenarios()
        self.stock_code_mapping: Dict[str, str] = self._load_stock_mapping()
        self.market_indices = {"KOSPI": f"^{YFINANCE_TICKER_SUFFIX_KOSPI[1:]}"} # 예: ^KS11

    def _load_scenarios(self) -> Dict[str, SimulationScenario]:
        return {
            "PN_006": SimulationScenario(
                id="PN_006", name="일본 반도체 소재 수출 규제",
                description="일본의 반도체 핵심 소재 수출 규제로 국내 반도체 업계 타격 및 국산화 동력 발생",
                start_date="2019-07-01", end_date="2020-01-31",
                related_industries=["반도체", "화학", "IT 서비스"]
            ),
            "PN_005": SimulationScenario(
                id="PN_005", name="이란 솔레이마니 제거 사건",
                description="미군의 이란 쿠드스 부대 사령관 제거로 중동 긴장 고조",
                start_date="2020-01-01", end_date="2020-04-30",
                related_industries=["정유", "방위산업", "금융"]
            ),
            "PN_004": SimulationScenario(
                id="PN_004", name="코로나19 재확산과 델타·오미크론 등장",
                description="변이 바이러스로 리오프닝 지연, 비대면 산업 재조명",
                start_date="2021-07-01", end_date="2021-12-31",
                related_industries=["운송·창고", "의료·정밀기기", "IT 서비스"]
            )
        }

    def _load_stock_mapping(self) -> Dict[str, str]:
        return {
            "005930": f"005930{YFINANCE_TICKER_SUFFIX_KOSPI}", # 삼성전자
            "000660": f"000660{YFINANCE_TICKER_SUFFIX_KOSPI}", # SK하이닉스
            "051910": f"051910{YFINANCE_TICKER_SUFFIX_KOSPI}", # LG화학
            "010950": f"010950{YFINANCE_TICKER_SUFFIX_KOSPI}", # S-OIL
            "047810": f"047810{YFINANCE_TICKER_SUFFIX_KOSPI}", # 한국항공우주
            "105560": f"105560{YFINANCE_TICKER_SUFFIX_KOSPI}", # KB금융
            "035720": f"035720{YFINANCE_TICKER_SUFFIX_KOSPI}", # 카카오
            "145020": f"145020{YFINANCE_TICKER_SUFFIX_KOSDAQ}", # 휴젤
        }
        
    def get_available_scenarios(self) -> List[Dict]:
        """[수정] .dict() 대신 수동으로 딕셔너리를 생성합니다."""
        scenarios_list = []
        for s in self.scenarios.values():
            scenarios_list.append({
                "id": s.id,
                "name": s.name,
                "description": s.description,
                "period": f"{s.start_date} ~ {s.end_date}",
                "related_industries": s.related_industries
            })
        return scenarios_list

    def get_recommended_stocks_for_scenario(self, scenario_id: str) -> Dict:
        recommendations = {
            "PN_006": {"반도체": [{"code": "005930", "name": "삼성전자"}, {"code": "000660", "name": "SK하이닉스"}]},
            "PN_005": {"정유": [{"code": "010950", "name": "S-OIL"}], "방위산업": [{"code": "047810", "name": "한국항공우주"}]},
            "PN_004": {"IT 서비스": [{"code": "035720", "name": "카카오"}], "의료·정밀기기": [{"code": "145020", "name": "휴젤"}]}
        }
        return recommendations.get(scenario_id, {})

    async def _fetch_data(self, tickers: List[str], start: str, end: str) -> pd.DataFrame:
        loop = asyncio.get_event_loop()
        # yfinance의 출력을 숨기기 위해 progress=False 추가
        return await loop.run_in_executor(None, yf.download, tickers, start, end, progress=False)

    async def run_simulation(self, scenario_id: str, investment_amount: int, investment_period: int, selected_stocks: List[StockSelection]) -> Dict:
        scenario = self.scenarios[scenario_id]
        sim_start_date = pd.to_datetime(scenario.start_date)
        sim_end_date = sim_start_date + pd.DateOffset(months=investment_period)
        
        stock_codes = [s.code for s in selected_stocks]
        tickers = [self.stock_code_mapping.get(c) for c in stock_codes if self.stock_code_mapping.get(c)]
        
        data = await self._fetch_data(tickers + list(self.market_indices.values()), sim_start_date - pd.DateOffset(days=5), sim_end_date + pd.DateOffset(days=5))
        prices = data['Adj Close'].loc[sim_start_date:sim_end_date]
        
        if prices.empty:
            raise ValueError("선택된 기간에 대한 주가 데이터를 가져올 수 없습니다.")

        portfolio_df = pd.DataFrame(index=prices.index)
        
        for stock in selected_stocks:
            ticker = self.stock_code_mapping.get(stock.code)
            if ticker in prices.columns and not prices[ticker].dropna().empty:
                initial_price = prices[ticker].dropna().iloc[0]
                if initial_price > 0:
                    num_shares = (investment_amount * (stock.allocation / 100)) / initial_price
                    portfolio_df[stock.code] = prices[ticker] * num_shares
        
        portfolio_df = portfolio_df.fillna(method='ffill').dropna()
        portfolio_df['total'] = portfolio_df.sum(axis=1)

        if portfolio_df.empty:
            raise ValueError("포트폴리오 가치를 계산할 수 없습니다.")
        
        initial_value = portfolio_df['total'].iloc[0]
        final_value = portfolio_df['total'].iloc[-1]
        total_return_pct = (final_value / initial_value - 1) * 100
        
        market_ticker = self.market_indices["KOSPI"]
        market_prices = prices[market_ticker].dropna()
        market_return_pct = 0.0
        if not market_prices.empty:
            market_initial = market_prices.iloc[0]
            market_final = market_prices.iloc[-1]
            if market_initial > 0:
                market_return_pct = (market_final / market_initial - 1) * 100
        
        stock_analysis = []
        for stock in selected_stocks:
            ticker = self.stock_code_mapping.get(stock.code)
            if ticker in prices.columns and not prices[ticker].dropna().empty:
                s_prices = prices[ticker].dropna()
                s_initial = s_prices.iloc[0]
                s_final = s_prices.iloc[-1]
                s_return = (s_final / s_initial - 1) * 100 if s_initial > 0 else 0
                stock_analysis.append({"name": stock.name, "code": stock.code, "return_pct": round(s_return, 2)})

        # [수정] .dict() 대신 수동으로 딕셔너리 생성
        scenario_info_dict = {
            "id": scenario.id,
            "name": scenario.name,
            "description": scenario.description
        }
        
        return {
            "scenario_info": scenario_info_dict,
            "simulation_results": {
                "initial_amount": int(initial_value),
                "final_amount": int(final_value),
                "total_return_pct": round(total_return_pct, 2)
            },
            "market_comparison": {"KOSPI_return_pct": round(market_return_pct, 2)},
            "stock_analysis": stock_analysis,
            "learning_points": ["시나리오에 맞는 종목 선택이 중요합니다.", f"시장 대비 {round(total_return_pct - market_return_pct, 2)}%p {'초과' if total_return_pct > market_return_pct else '하회'} 수익을 기록했습니다."]
        }

    async def validate_simulation_inputs(self, scenario_id: str, investment_amount: int, investment_period: int, selected_stocks: List[Dict]) -> Dict:
        errors, warnings = [], []
        if scenario_id not in self.scenarios:
            errors.append("유효하지 않은 시나리오 ID입니다.")
        if not (10000 <= investment_amount <= 100000000):
            errors.append("투자 금액은 1만원 이상 1억원 이하여야 합니다.")
        if not (1 <= investment_period <= 24):
            errors.append("투자 기간은 1개월에서 24개월 사이여야 합니다.")
        if not selected_stocks:
            errors.append("최소 1개 이상의 종목을 선택해야 합니다.")
        
        total_alloc = sum(s['allocation'] for s in selected_stocks)
        if abs(total_alloc - 100.0) > 0.1:
            warnings.append(f"총 투자 비중이 100%가 아닙니다 (현재: {total_alloc:.1f}%).")
            
        return {"valid": not errors, "errors": errors, "warnings": warnings}

# --- Service Singleton & Initialization ---
engine: Optional[SimulationEngine] = None

def initialize():
    global engine
    if engine is None:
        engine = SimulationEngine()
    print("✅ Simulation Service initialized.")

def is_initialized():
    return engine is not None

def get_health() -> dict:
    return {"name": "simulation_service", "status": "ok" if is_initialized() else "error"}