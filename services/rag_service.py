# services/rag_service.py (완전 수정된 버전)
"""
RAG 분석 서비스 - 벡터 검색 + AI Agent 하이브리드 분석
integrated_pipeline.py의 RealRAGAnalysisExecutor 로직 이관
"""

import os
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_pinecone import PineconeVectorStore
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

class RAGService:
    """RAG 분석 서비스"""
    
    def __init__(self):
        load_dotenv(override=True)
        
        # 환경 설정
        self.EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
        self.INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "ordaproject")
        
        # LLM 초기화
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        self.embedding = OpenAIEmbeddings(model=self.EMBEDDING_MODEL)
        
        # 벡터 스토어 초기화
        self.industry_store = PineconeVectorStore(
            index_name=self.INDEX_NAME,
            embedding=self.embedding,
            namespace="industry"
        )
        
        self.past_issue_store = PineconeVectorStore(
            index_name=self.INDEX_NAME,
            embedding=self.embedding,
            namespace="past_issue"
        )
        
        # 데이터베이스 로딩
        self._load_databases()
        
        print("✅ RAG 분석 서비스 초기화 완료")
    
    def _load_databases(self):
        """산업 DB 및 과거 이슈 DB 로딩"""
        try:
            # 산업 DB 로딩
            self.industry_df = pd.read_csv("data/산업DB.v.0.3.csv")
            self.industry_dict = dict(zip(self.industry_df["KRX 업종명"], self.industry_df["상세내용"]))
            self.valid_krx_names = list(self.industry_df["KRX 업종명"].unique())
            print(f"✅ 산업 DB 로드: {len(self.valid_krx_names)}개 업종")
            
            # 과거 이슈 DB 로딩
            self.past_df = pd.read_csv("data/Past_news.csv")
            self.issue_dict = dict(zip(
                self.past_df["Issue_name"], 
                self.past_df["Contents"] + "\n\n상세: " + self.past_df["Contentes(Spec)"]
            ))
            self.valid_issue_names = list(self.past_df["Issue_name"].unique())
            print(f"✅ 과거 이슈 DB 로드: {len(self.valid_issue_names)}개 이슈")
            
        except Exception as e:
            print(f"⚠️ DB 로드 실패: {e}")
            self.industry_dict = {}
            self.valid_krx_names = []
            self.issue_dict = {}
            self.valid_issue_names = []
    
    def analyze_issues_with_rag(self, filtered_issues: List[Dict]) -> List[Dict]:
        """필터링된 이슈들에 대해 RAG 분석 수행"""
        
        print(f"🔍 RAG 분석 시작: {len(filtered_issues)}개 이슈")
        
        enriched_issues = []
        
        for i, issue in enumerate(filtered_issues, 1):
            print(f"🔄 이슈 {i}/{len(filtered_issues)} RAG 분석 중: {issue.get('제목', 'N/A')[:50]}...")
            
            # 관련 산업 분석
            related_industries = self._analyze_industry_for_issue(issue)
            
            # 관련 과거 이슈 분석
            related_past_issues = self._analyze_past_issues_for_issue(issue)
            
            # RAG 신뢰도 계산
            rag_confidence = self._calculate_rag_confidence(related_industries, related_past_issues)
            
            # 기본 이슈에 RAG 결과 추가
            enriched_issue = issue.copy()
            enriched_issue.update({
                "관련산업": related_industries,
                "관련과거이슈": related_past_issues,
                "RAG분석신뢰도": rag_confidence
            })
            
            enriched_issues.append(enriched_issue)
            
            print(f"   ✅ 이슈 {i} RAG 완료: 산업 {len(related_industries)}개, 과거이슈 {len(related_past_issues)}개, 신뢰도 {rag_confidence}")
        
        print(f"✅ RAG 분석 완료: 평균 신뢰도 {self._calculate_average_confidence(enriched_issues)}")
        return enriched_issues
    
    def _analyze_industry_for_issue(self, issue: Dict) -> List[Dict]:
        """특정 이슈에 대한 관련 산업 분석"""
        try:
            query = f"{issue.get('제목', '')}\n{issue.get('원본내용', issue.get('내용', ''))}"
            
            # Step 1: 벡터 검색
            vector_candidates = self._vector_search_industries(query)
            
            # Step 2: AI Agent 분석
            ai_candidates = self._ai_extract_candidate_industries(query)
            
            # Step 3: 결과 통합
            final_candidates = self._combine_industry_results(query, vector_candidates, ai_candidates)
            
            return final_candidates[:3]  # 상위 3개 반환
            
        except Exception as e:
            print(f"❌ 산업 분석 실패: {e}")
            return []
    
    def _analyze_past_issues_for_issue(self, issue: Dict) -> List[Dict]:
        """특정 이슈에 대한 관련 과거 이슈 분석"""
        try:
            query = f"{issue.get('제목', '')}\n{issue.get('원본내용', issue.get('내용', ''))}"
            
            # Step 1: 벡터 검색
            vector_candidates = self._vector_search_past_issues(query)
            
            # Step 2: AI Agent 분석  
            ai_candidates = self._ai_extract_candidate_past_issues(query)
            
            # Step 3: 결과 통합
            final_candidates = self._combine_past_issue_results(query, vector_candidates, ai_candidates)
            
            return final_candidates[:3]  # 상위 3개 반환
            
        except Exception as e:
            print(f"❌ 과거 이슈 분석 실패: {e}")
            return []
    
    def _vector_search_industries(self, query: str) -> List[Dict]:
        """벡터 검색으로 관련 산업 후보 추출"""
        try:
            results = self.industry_store.similarity_search_with_score(query, k=10)
            
            candidates = []
            for doc, score in results:
                content = doc.page_content.replace('\ufeff', '').replace('﻿', '')
                
                if "KRX 업종명:" in content:
                    lines = content.split("\n")
                    for line in lines:
                        if "KRX 업종명:" in line:
                            industry_name = line.replace("KRX 업종명:", "").strip()
                            if industry_name in self.industry_dict:
                                if not any(c["name"] == industry_name for c in candidates):
                                    similarity_percentage = round((1 - score) * 100, 1)
                                    
                                    content_parts = content.split("상세내용:")
                                    industry_detail = content_parts[1].strip() if len(content_parts) > 1 else self.industry_dict[industry_name]
                                    
                                    candidates.append({
                                        "name": industry_name,
                                        "similarity": similarity_percentage,
                                        "description": industry_detail
                                    })
                            break
            
            return candidates
            
        except Exception as e:
            print(f"❌ 산업 벡터 검색 실패: {e}")
            return []
    
    def _vector_search_past_issues(self, query: str) -> List[Dict]:
        """벡터 검색으로 관련 과거 이슈 후보 추출"""
        try:
            results = self.past_issue_store.similarity_search_with_score(query, k=10)
            
            candidates = []
            for doc, score in results:
                content = doc.page_content.replace('\ufeff', '').replace('﻿', '')
                
                if "Issue_name:" in content:
                    lines = content.split("\n")
                    for line in lines:
                        if "Issue_name:" in line:
                            issue_name = line.replace("Issue_name:", "").strip()
                            if issue_name in self.issue_dict:
                                if not any(c["name"] == issue_name for c in candidates):
                                    similarity_percentage = round((1 - score) * 100, 1)
                                    
                                    content_parts = content.split("Contents:")
                                    issue_detail = content_parts[1].strip() if len(content_parts) > 1 else self.issue_dict[issue_name]
                                    
                                    # 기간 정보 추출
                                    period = "N/A"
                                    for line in lines:
                                        if "Start_date:" in line and "Fin_date:" in line:
                                            start = line.split("Start_date:")[1].split("Fin_date:")[0].strip()
                                            end = line.split("Fin_date:")[1].strip()
                                            period = f"{start} ~ {end}"
                                            break
                                    
                                    candidates.append({
                                        "name": issue_name,
                                        "similarity": similarity_percentage,
                                        "description": issue_detail,
                                        "period": period
                                    })
                            break
            
            return candidates
            
        except Exception as e:
            print(f"❌ 과거 이슈 벡터 검색 실패: {e}")
            return []
    
    def _ai_extract_candidate_industries(self, news_content: str) -> List[Dict]:
        """AI Agent가 관련 산업 후보 추출"""
        if not self.valid_krx_names:
            return []
            
        prompt = ChatPromptTemplate.from_messages([
            ("system", """너는 뉴스와 산업의 관련성을 판단하는 전문 애널리스트야.
주어진 뉴스 내용을 분석하고, 제공된 KRX 업종 리스트에서 관련 가능성이 높은 산업들을 선별해야 해.

관련성 판단 기준:
1. 직접적 영향: 뉴스가 해당 산업에 직접적인 영향을 미치는가?
2. 공급망 관계: 뉴스 관련 기업/산업과 공급망 관계가 있는가?
3. 시장 동향: 뉴스가 해당 산업의 시장 동향에 영향을 미치는가?
4. 정책/규제: 뉴스가 해당 산업 관련 정책이나 규제와 연관되는가?"""),
            ("human", """
[뉴스 내용]
{news}

[KRX 업종 리스트]  
{industries}

위 뉴스와 관련 가능성이 높은 산업을 10개 선별해주세요.
각 산업에 대해 관련성 점수(1-10점)와 간단한 이유를 제시해주세요.

출력 형식 (JSON):
{{
  "candidates": [
    {{"industry": "산업명", "score": 점수, "reason": "관련성 이유"}},
    ...
  ]
}}""")
        ])
        
        parser = JsonOutputParser()
        chain = prompt | self.llm | parser
        
        try:
            result = chain.invoke({
                "news": news_content,
                "industries": ", ".join(self.valid_krx_names[:50])  # 너무 많으면 제한
            })
            return result.get("candidates", [])
        except Exception as e:
            print(f"❌ AI 산업 후보 추출 실패: {e}")  # 🔧 수정: 에러 메시지 올바르게 변경
            return []
    
    def _ai_extract_candidate_past_issues(self, news_content: str) -> List[Dict]:
        """AI Agent가 관련 과거 이슈 후보 추출"""  # 🔧 추가: 빠져있던 함수 추가
        if not self.valid_issue_names:
            return []
            
        prompt = ChatPromptTemplate.from_messages([
            ("system", """너는 현재 뉴스와 과거 이슈의 관련성을 판단하는 전문 애널리스트야.
주어진 현재 뉴스 내용을 분석하고, 제공된 과거 이슈 리스트에서 관련 가능성이 높은 이슈들을 선별해야 해.

관련성 판단 기준:
1. 유사한 시장 상황: 과거 이슈와 현재 상황이 유사한 시장 환경인가?
2. 동일한 산업/기업 영향: 같은 산업이나 유사한 기업들에 영향을 미치는가?
3. 정책/경제적 유사성: 정책 변화나 경제적 요인이 유사한가?
4. 투자자 심리: 투자자들의 반응이나 시장 심리가 비슷한가?"""),
            ("human", """
[현재 뉴스 내용]
{news}

[과거 이슈 리스트]
{issues}

위 현재 뉴스와 관련 가능성이 높은 과거 이슈를 10개 선별해주세요.
각 과거 이슈에 대해 관련성 점수(1-10점)와 간단한 이유를 제시해주세요.

출력 형식 (JSON):
{{
  "candidates": [
    {{"issue": "이슈명", "score": 점수, "reason": "관련성 이유"}},
    ...
  ]
}}""")
        ])
        
        parser = JsonOutputParser()
        chain = prompt | self.llm | parser
        
        try:
            result = chain.invoke({
                "news": news_content,
                "issues": ", ".join(self.valid_issue_names[:50])  # 너무 많으면 제한
            })
            return result.get("candidates", [])
        except Exception as e:
            print(f"❌ AI 과거 이슈 후보 추출 실패: {e}")
            return []
    
    def _combine_industry_results(self, query: str, vector_candidates: List[Dict], ai_candidates: List[Dict]) -> List[Dict]:
        """벡터 검색 결과와 AI 후보를 결합하여 최종 관련 산업 도출"""
        all_candidates = {}
        
        # 벡터 검색 결과 추가
        for candidate in vector_candidates:
            name = candidate["name"]
            all_candidates[name] = {
                "name": name,
                "vector_similarity": candidate["similarity"],
                "ai_score": 0,
                "ai_reason": "",
                "description": candidate["description"]
            }
        
        # AI 후보 추가/업데이트
        for candidate in ai_candidates:
            name = candidate["industry"]
            if name in all_candidates:
                all_candidates[name]["ai_score"] = candidate["score"]
                all_candidates[name]["ai_reason"] = candidate["reason"]
            elif name in self.industry_dict:  # 유효한 산업명인 경우만
                all_candidates[name] = {
                    "name": name,
                    "vector_similarity": 0,
                    "ai_score": candidate["score"],
                    "ai_reason": candidate["reason"],
                    "description": self.industry_dict[name]
                }
        
        # 종합 점수 계산 (벡터 유사도 + AI 점수)
        for candidate in all_candidates.values():
            # 벡터 유사도를 10점 만점으로 정규화
            normalized_vector = candidate["vector_similarity"] / 10
            ai_score = candidate["ai_score"]
            
            # 가중평균 (AI 점수에 더 높은 가중치)
            candidate["final_score"] = round((normalized_vector * 0.3 + ai_score * 0.7), 1)
        
        # 최종 점수로 정렬
        sorted_candidates = sorted(all_candidates.values(), 
                                  key=lambda x: x["final_score"], 
                                  reverse=True)
        
        return sorted_candidates
    
    def _combine_past_issue_results(self, query: str, vector_candidates: List[Dict], ai_candidates: List[Dict]) -> List[Dict]:
        """벡터 검색 결과와 AI 후보를 결합하여 최종 관련 과거 이슈 도출"""
        all_candidates = {}
        
        # 벡터 검색 결과 추가
        for candidate in vector_candidates:
            name = candidate["name"]
            all_candidates[name] = {
                "name": name,
                "vector_similarity": candidate["similarity"],
                "ai_score": 0,
                "ai_reason": "",
                "description": candidate["description"],
                "period": candidate.get("period", "N/A")
            }
        
        # AI 후보 추가/업데이트
        for candidate in ai_candidates:
            name = candidate["issue"]
            if name in all_candidates:
                all_candidates[name]["ai_score"] = candidate["score"]
                all_candidates[name]["ai_reason"] = candidate["reason"]
            elif name in self.issue_dict:  # 유효한 이슈명인 경우만
                all_candidates[name] = {
                    "name": name,
                    "vector_similarity": 0,
                    "ai_score": candidate["score"],
                    "ai_reason": candidate["reason"],
                    "description": self.issue_dict[name],
                    "period": "N/A"
                }
        
        # 종합 점수 계산
        for candidate in all_candidates.values():
            normalized_vector = candidate["vector_similarity"] / 10
            ai_score = candidate["ai_score"]
            candidate["final_score"] = round((normalized_vector * 0.3 + ai_score * 0.7), 1)
        
        # 최종 점수로 정렬
        sorted_candidates = sorted(all_candidates.values(), 
                                    key=lambda x: x["final_score"], 
                                    reverse=True)
        
        return sorted_candidates
    
    def _calculate_rag_confidence(self, industries: List[Dict], past_issues: List[Dict]) -> float:
        """RAG 분석 신뢰도 계산"""
        if not industries or not past_issues:
            return 0.0
        
        # 실제 final_score 기반 신뢰도 계산
        industry_avg = sum(ind.get("final_score", 0) for ind in industries) / len(industries)
        past_avg = sum(issue.get("final_score", 0) for issue in past_issues) / len(past_issues)
        
        return round((industry_avg + past_avg) / 2, 1)
    
    def _calculate_average_confidence(self, enriched_issues: List[Dict]) -> float:
        """전체 이슈들의 평균 RAG 신뢰도 계산"""
        if not enriched_issues:
            return 0.0
        
        confidences = [issue.get("RAG분석신뢰도", 0.0) for issue in enriched_issues]
        return round(sum(confidences) / len(confidences), 2)