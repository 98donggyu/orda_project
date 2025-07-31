# services/pipeline_service.py
import json
from typing import List, Dict, Optional
from datetime import datetime
import time
import asyncio

from services import crawling_service, rag_service, database_service
from config import FAST_LLM_MODEL, OPENAI_API_KEY
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

# --- 캐시를 위한 전역 변수 ---
_latest_analyzed_issues_cache: List[Dict] = []
_last_update_time: Optional[datetime] = None

class StockMarketFilter:
    """LLM을 이용해 주식 시장 관련성이 높은 뉴스를 필터링하는 클래스"""
    def __init__(self):
        self.llm = ChatOpenAI(model=FAST_LLM_MODEL, temperature=0.1, api_key=OPENAI_API_KEY)
        self.prompt = self._create_prompt()
        self.parser = JsonOutputParser()
        self.chain = self.prompt | self.llm | self.parser

    def _create_prompt(self) -> ChatPromptTemplate:
        return ChatPromptTemplate.from_template(
            """
            당신은 한국 주식시장 전문 애널리스트입니다.
            다음 뉴스 이슈 목록에서 주식시장에 가장 큰 영향을 미칠 것으로 예상되는 상위 5개를 선별해주세요.
            평가 기준은 '직접적 기업 영향', '정책적 영향', '시장 심리', '산업 트렌드'입니다.

            ## 뉴스 목록:
            {issues_list}

            ## 출력 형식 (JSON):
            {{
              "selected_issues": [
                {{
                  "이슈번호": <원본 이슈번호>,
                  "제목": "<원본 제목>",
                  "선별이유": "<주식시장 관점에서의 선별 이유>"
                }}
              ]
            }}
            """
        )
    
    def filter(self, issues: List[Dict], target_count: int = 5) -> List[Dict]:
        """주어진 이슈 리스트에서 관련성 높은 이슈를 필터링"""
        if not issues:
            return []
        
        issues_text = "\n".join([f"- 이슈번호 {i.get('이슈번호', 'N/A')}: {i.get('제목', 'N/A')}" for i in issues])
        
        try:
            result = self.chain.invoke({"issues_list": issues_text})
            selected_info = result.get("selected_issues", [])
            
            # 원본 이슈 데이터와 병합
            selected_ids = {s['이슈번호'] for s in selected_info}
            final_issues = [issue for issue in issues if issue.get('이슈번호') in selected_ids]
            
            # 선별 이유 추가
            info_map = {s['이슈번호']: s for s in selected_info}
            for issue in final_issues:
                issue['선별이유'] = info_map.get(issue['이슈번호'], {}).get('선별이유', 'N/A')

            return final_issues[:target_count]

        except Exception as e:
            print(f"❌ LLM 필터링 실패: {e}. 상위 {target_count}개 이슈를 임의로 반환합니다.")
            return issues[:target_count]

_filter_instance = StockMarketFilter()

async def run_full_pipeline():
    """전체 데이터 파이프라인을 순차적으로 실행"""
    global _latest_analyzed_issues_cache, _last_update_time
    
    start_time = time.time()
    print("🚀 전체 뉴스 파이프라인을 시작합니다.")

    # 1. 뉴스 크롤링
    crawled_issues = crawling_service.crawl_news()
    if not crawled_issues:
        print("⚠️ 크롤링된 뉴스가 없어 파이프라인을 중단합니다.")
        return

    # 2. LLM 필터링
    print("🔍 주식 시장 관련성 높은 뉴스 5개를 필터링합니다.")
    filtered_issues = _filter_instance.filter(crawled_issues)
    if not filtered_issues:
        print("⚠️ 필터링된 뉴스가 없어 파이프라인을 중단합니다.")
        return

    # 3. RAG 종합 분석 (각 이슈에 대해)
    print(f"🧠 필터링된 {len(filtered_issues)}개 이슈에 대해 RAG 종합 분석을 수행합니다.")
    analyzed_issues = []
    
    async def analyze(issue):
        content = f"{issue['제목']}\n{issue['내용']}"
        rag_result = await rag_service.comprehensive_analysis(content)
        issue.update(rag_result) # 분석 결과를 원본 이슈에 병합
        return issue

    tasks = [analyze(issue) for issue in filtered_issues]
    analyzed_issues = await asyncio.gather(*tasks)

    # 4. 결과 캐싱
    _latest_analyzed_issues_cache = analyzed_issues
    _last_update_time = datetime.now()
    
    end_time = time.time()
    print(f"✅ 파이프라인 완료! (소요 시간: {end_time - start_time:.2f}초)")

def get_latest_analyzed_issues() -> List[Dict]:
    """캐시된 최신 분석 완료 이슈를 반환"""
    if not _latest_analyzed_issues_cache:
        print("ℹ️ 캐시가 비어있어 파이프라인을 즉시 실행합니다.")
        asyncio.run(run_full_pipeline())
    return _latest_analyzed_issues_cache