# services/crawling_service.py (수정된 버전 - 원본 코드 그대로 사용)
"""
크롤링 및 필터링 통합 서비스
원본 BigKindsCrawler를 그대로 사용하고 필터링만 추가
"""

import os
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

# 원본 BigKindsCrawler 그대로 import (같은 폴더에서)
from .crawling_bigkinds import BigKindsCrawler

class CrawlingService:
    """크롤링 및 필터링 통합 서비스 - 원본 BigKindsCrawler 사용"""
    
    def __init__(self, data_dir: str = "data2", headless: bool = True):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.headless = headless
        
        load_dotenv(override=True)
        
        # AI 필터링용 LLM 초기화
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        
        print("✅ 크롤링 서비스 초기화 완료")
    
    def crawl_and_filter_news(self, 
                             issues_per_category: int = 10,
                             target_filtered_count: int = 5) -> Dict:
        """원본 BigKindsCrawler 사용 + 필터링"""
        
        print(f"🕷️ BigKinds 크롤링 시작: 카테고리별 {issues_per_category}개씩")
        
        # Step 1: 원본 BigKindsCrawler로 크롤링
        crawler = BigKindsCrawler(
            data_dir=str(self.data_dir),
            headless=self.headless,
            issues_per_category=issues_per_category
        )
        
        # 원본 메서드 그대로 호출
        crawling_result = crawler.crawl_all_categories()
        
        print(f"✅ 크롤링 완료: {crawling_result.get('total_issues', 0)}개 이슈")
        
        # Step 2: 필터링
        all_issues = crawling_result.get("all_issues", [])
        if all_issues:
            filtering_result = self._filter_by_stock_relevance(all_issues, target_filtered_count)
        else:
            filtering_result = {
                "selected_issues": [],
                "filter_metadata": {
                    "filtering_method": "no_issues_to_filter",
                    "original_count": 0,
                    "selected_count": 0,
                    "filtered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        
        return {
            **crawling_result,
            "filtered_issues": filtering_result["selected_issues"],
            "filter_metadata": filtering_result["filter_metadata"]
        }
    
    def _filter_by_stock_relevance(self, all_issues: List[Dict], target_count: int) -> Dict:
        """주식시장 관련성 기반 필터링"""
        
        print(f"🤖 AI 필터링 시작: {len(all_issues)}개 → {target_count}개 선별")
        
        # 각 이슈별로 주식시장 관련성 점수 계산
        scored_issues = []
        
        for i, issue in enumerate(all_issues, 1):
            print(f"🔄 이슈 {i}/{len(all_issues)} 분석 중: {issue.get('제목', 'N/A')[:30]}...")
            
            # AI로 주식시장 관련성 분석
            relevance_score = self._analyze_stock_market_relevance(issue)
            
            scored_issue = issue.copy()
            scored_issue.update({
                "주식시장_관련성_점수": relevance_score["종합점수"],
                "관련성_분석": relevance_score
            })
            
            scored_issues.append(scored_issue)
        
        # 점수순 정렬 및 상위 선별
        scored_issues.sort(key=lambda x: x["주식시장_관련성_점수"], reverse=True)
        selected_issues = scored_issues[:target_count]
        
        # 순위 부여
        for rank, issue in enumerate(selected_issues, 1):
            issue["rank"] = rank
        
        result = {
            "selected_issues": selected_issues,
            "filter_metadata": {
                "filtering_method": "gpt-4o-mini_stock_relevance",
                "original_count": len(all_issues),
                "selected_count": len(selected_issues),
                "average_score": sum(issue["주식시장_관련성_점수"] for issue in selected_issues) / len(selected_issues) if selected_issues else 0,
                "filtered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        # 필터링 결과 저장
        self._save_filtering_result(result)
        
        print(f"✅ AI 필터링 완료: 상위 {len(selected_issues)}개 선별")
        return result
    
    def _analyze_stock_market_relevance(self, issue: Dict) -> Dict:
        """AI를 사용한 주식시장 관련성 분석"""
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", """너는 뉴스가 주식시장에 미칠 영향을 분석하는 전문가야.
다음 기준으로 뉴스의 주식시장 관련성을 1-10점으로 평가해줘:

1. 직접적 기업 영향 (기업실적, 경영진 변화 등)
2. 산업 전반 영향 (정책변화, 기술혁신 등) 
3. 거시경제 영향 (금리, 환율, 정책 등)
4. 투자심리 영향 (시장 트렌드, 이슈 확산성 등)

각 기준별 점수와 종합점수를 제시해줘."""),
            ("human", """
[뉴스 제목]
{title}

[뉴스 내용]  
{content}

위 뉴스의 주식시장 관련성을 분석해주세요.

출력 형식 (JSON):
{{
  "직접적_기업영향": 점수,
  "산업_전반영향": 점수, 
  "거시경제_영향": 점수,
  "투자심리_영향": 점수,
  "종합점수": 점수,
  "분석근거": "상세 분석 내용"
}}""")
        ])
        
        parser = JsonOutputParser()
        chain = prompt | self.llm | parser
        
        try:
            result = chain.invoke({
                "title": issue.get("제목", ""),
                "content": issue.get("내용", "")  # 원본에서는 "내용" 필드 사용
            })
            
            return {
                "직접적_기업영향": result.get("직접적_기업영향", 5),
                "산업_전반영향": result.get("산업_전반영향", 5),
                "거시경제_영향": result.get("거시경제_영향", 5), 
                "투자심리_영향": result.get("투자심리_영향", 5),
                "종합점수": result.get("종합점수", 5),
                "분석근거": result.get("분석근거", "AI 분석 완료")
            }
            
        except Exception as e:
            print(f"❌ AI 분석 실패: {e}")
            return {
                "직접적_기업영향": 5,
                "산업_전반영향": 5,
                "거시경제_영향": 5,
                "투자심리_영향": 5,
                "종합점수": 5,
                "분석근거": f"AI 분석 실패: {e}"
            }
    
    def _save_filtering_result(self, result: Dict):
        """필터링 결과 저장"""
        timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
        filename = f"{timestamp}_StockFiltered_{len(result['selected_issues'])}issues.json"
        filepath = self.data_dir / filename
        
        save_data = {
            **result,
            "file_info": {
                "filename": filename,
                "created_at": datetime.now().isoformat(),
                "filter_version": "StockRelevanceFilter_v1.0"
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 필터링 결과 저장: {filepath}")