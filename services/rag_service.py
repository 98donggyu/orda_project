# services/rag_service.py
import os
import json
from typing import List, Dict, Any, Optional
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pinecone import Pinecone, ServerlessSpec

from config import (
    OPENAI_API_KEY, PINECONE_API_KEY, PINECONE_INDEX_NAME,
    MAIN_LLM_MODEL, EMBEDDING_MODEL,
    PINECONE_NAMESPACE_INDUSTRY, PINECONE_NAMESPACE_PAST_ISSUE
)
from models.schemas import PastIssueInfo, IndustryInfo

class VectorSearchEngine:
    """Pinecone 벡터 검색 및 관리를 담당하는 클래스"""
    def __init__(self):
        self.pc = Pinecone(api_key=PINECONE_API_KEY)
        self.embedding = OpenAIEmbeddings(model=EMBEDDING_MODEL, api_key=OPENAI_API_KEY)
        self.index_name = PINECONE_INDEX_NAME
        self._connect_or_create_index()
        self.industry_store = PineconeVectorStore(index_name=self.index_name, embedding=self.embedding, namespace=PINECONE_NAMESPACE_INDUSTRY)
        self.past_issue_store = PineconeVectorStore(index_name=self.index_name, embedding=self.embedding, namespace=PINECONE_NAMESPACE_PAST_ISSUE)

    def _connect_or_create_index(self):
        if self.index_name not in self.pc.list_indexes().names():
            print(f"⚠️ 인덱스 '{self.index_name}'가 존재하지 않아 새로 생성합니다.")
            self.pc.create_index(
                name=self.index_name,
                dimension=1536,  # text-embedding-3-small 차원
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1")
            )
        self.index = self.pc.Index(self.index_name)
        print(f"✅ Pinecone 인덱스 '{self.index_name}'에 연결되었습니다.")

    async def upsert_documents(self, documents: List[Document], namespace: str):
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
        chunks = text_splitter.split_documents(documents)
        await PineconeVectorStore.from_documents(chunks, self.embedding, index_name=self.index_name, namespace=namespace)

    async def search_similar_past_issues(self, query: str, top_k: int) -> List[PastIssueInfo]:
        docs_with_scores = await self.past_issue_store.asimilarity_search_with_score(query, k=top_k)
        results = []
        for doc, score in docs_with_scores:
            results.append(PastIssueInfo(
                issue_name=doc.metadata.get('title', 'N/A'),
                contents=doc.page_content,
                similarity_score=score
            ))
        return results

    async def search_related_industries(self, query: str, top_k: int) -> List[IndustryInfo]:
        docs_with_scores = await self.industry_store.asimilarity_search_with_score(query, k=top_k)
        results = []
        for doc, score in docs_with_scores:
            results.append(IndustryInfo(
                industry_name=doc.metadata.get('name', 'N/A'),
                description=doc.page_content,
                similarity_score=score
            ))
        return results
    
    def get_index_stats(self):
        return self.index.describe_index_stats()

    async def health_check(self) -> dict:
        try:
            stats = self.get_index_stats()
            return {"overall_status": "healthy", "details": stats.to_dict()}
        except Exception as e:
            return {"overall_status": "unhealthy", "error": str(e)}

class RAGAnalyzer:
    """RAG 기반 뉴스 분석 시스템"""
    def __init__(self, vector_engine: VectorSearchEngine):
        self.llm = ChatOpenAI(model=MAIN_LLM_MODEL, temperature=0.3, api_key=OPENAI_API_KEY)
        self.vector_engine = vector_engine
        self._init_prompts()

    def _init_prompts(self):
        self.analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", "당신은 전문 금융 애널리스트입니다. 주어진 현재 뉴스, 과거 유사 이슈, 관련 산업 정보를 바탕으로 투자자 입장에서 이해하기 쉽게 종합적인 분석을 제공해주세요. 투자 추천은 절대 하지 마세요."),
            ("human", """
            ## 현재 뉴스:
            {current_news}

            ## 과거 유사 이슈 (유사도 순):
            {past_issues}

            ## 관련 산업 정보 (관련성 순):
            {industry_context}

            위 정보를 바탕으로, 다음 항목에 대해 2~3문단으로 명확하고 쉽게 종합 분석을 작성해주세요.
            """)
        ])
        self.confidence_prompt = ChatPromptTemplate.from_template(
            "현재 뉴스: {current_news}\n과거 이슈: {past_issues}\n관련 산업: {industry_context}\n위 정보들의 연관성을 고려하여, 분석의 신뢰도를 0.0에서 1.0 사이의 점수로 평가하고 그 이유를 간략히 설명해주세요. JSON 형식으로 응답하세요: {{\"confidence\": 점수, \"reason\": \"평가 이유\"}}"
        )

    async def comprehensive_analysis(self, current_news: str, max_past_issues: int = 3, max_industries: int = 3) -> Dict:
        past_issues = await self.vector_engine.search_similar_past_issues(current_news, top_k=max_past_issues)
        industries = await self.vector_engine.search_related_industries(current_news, top_k=max_industries)

        past_issues_str = "\n".join([f"- {p.issue_name}" for p in past_issues]) if past_issues else "없음"
        industries_str = "\n".join([f"- {i.industry_name}" for i in industries]) if industries else "없음"

        analysis_chain = self.analysis_prompt | self.llm | StrOutputParser()
        confidence_chain = self.confidence_prompt | self.llm | JsonOutputParser()

        explanation = await analysis_chain.ainvoke({
            "current_news": current_news,
            "past_issues": past_issues_str,
            "industry_context": industries_str
        })
        
        confidence_result = await confidence_chain.ainvoke({
            "current_news": current_news,
            "past_issues": past_issues_str,
            "industry_context": industries_str
        })

        return {
            "explanation": explanation,
            "confidence": confidence_result.get("confidence", 0.0),
            "past_issues": [p.dict() for p in past_issues],
            "industries": [i.dict() for i in industries],
        }

# --- Service Singleton & Initialization ---
vector_engine: Optional[VectorSearchEngine] = None
rag_analyzer: Optional[RAGAnalyzer] = None

def initialize():
    global vector_engine, rag_analyzer
    if not OPENAI_API_KEY or not PINECONE_API_KEY:
        print("⚠️ 경고: OpenAI 또는 Pinecone API 키가 설정되지 않았습니다. RAG 서비스가 비활성화됩니다.")
        return
    if vector_engine is None:
        vector_engine = VectorSearchEngine()
    if rag_analyzer is None:
        rag_analyzer = RAGAnalyzer(vector_engine)
    print("✅ RAG Service initialized.")

def is_initialized():
    return rag_analyzer is not None

async def get_health() -> dict:
    if not is_initialized():
        return {"name": "rag_service", "status": "disabled", "detail": "API keys not set"}
    
    health = await vector_engine.health_check()
    return {
        "name": "rag_service", 
        "status": "ok" if health.get("overall_status") == "healthy" else "degraded",
        "detail": health
    }

async def comprehensive_analysis(current_news: str, max_past_issues: int = 3, max_industries: int = 3) -> Dict:
    if not is_initialized():
        raise ConnectionError("RAG service is not initialized.")
    return await rag_analyzer.comprehensive_analysis(current_news, max_past_issues, max_industries)