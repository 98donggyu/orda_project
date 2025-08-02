"""
파이프라인 통합 서비스 - 크롤링, 필터링, RAG 분석을 연결
integrated_pipeline.py의 IntegratedNewsPipeline 로직 이관
"""

import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from .crawling_service import CrawlingService
from .rag_service import RAGService

class PipelineService:
    """전체 파이프라인 통합 서비스"""
    
    def __init__(self, data_dir: str = "data2", headless: bool = True):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # 서비스 초기화
        self.crawling_service = CrawlingService(str(self.data_dir), headless)
        self.rag_service = RAGService()
        
        print("✅ 파이프라인 서비스 초기화 완료")
    
    def execute_full_pipeline(self, 
                            issues_per_category: int = 10,
                            target_filtered_count: int = 5) -> Dict:
        """전체 파이프라인 실행: 크롤링 → 필터링 → RAG 분석"""
        
        pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        started_at = datetime.now()
        
        print(f"🚀 파이프라인 실행 시작 (ID: {pipeline_id})")
        print(f"📋 설정: 카테고리별 {issues_per_category}개, 최종 선별 {target_filtered_count}개")
        
        result = {
            "pipeline_id": pipeline_id,
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": None,
            "execution_time": None,
            "final_status": "running",
            "steps_completed": [],
            "errors": []
        }
        
        try:
            # Step 1: 크롤링 + 필터링
            print(f"\n{'='*60}")
            print(f"📡 Step 1: 크롤링 + 주식시장 필터링")
            print(f"{'='*60}")
            
            crawling_result = self.crawling_service.crawl_and_filter_news(
                issues_per_category, target_filtered_count
            )
            
            result["crawling_result"] = {
                "total_crawled": len(crawling_result.get("all_issues", [])),
                "filtered_count": len(crawling_result.get("filtered_issues", []))
            }
            result["steps_completed"].append("crawling_and_filtering")
            
            # Step 2: RAG 분석
            print(f"\n{'='*60}")
            print(f"🔍 Step 2: RAG 분석 (산업 + 과거 이슈)")
            print(f"{'='*60}")
            
            filtered_issues = crawling_result.get("filtered_issues", [])
            if not filtered_issues:
                raise ValueError("필터링된 이슈가 없습니다.")
            
            enriched_issues = self.rag_service.analyze_issues_with_rag(filtered_issues)
            
            result["rag_result"] = {
                "analyzed_count": len(enriched_issues),
                "average_confidence": self._calculate_average_confidence(enriched_issues)
            }
            result["steps_completed"].append("rag_analysis")
            
            # Step 3: API용 데이터 준비
            print(f"\n{'='*60}")
            print(f"🌐 Step 3: API 응답 데이터 준비")
            print(f"{'='*60}")
            
            api_data = self._prepare_api_data(crawling_result, enriched_issues)
            result["api_ready_data"] = api_data
            result["steps_completed"].append("api_preparation")
            
            # 파이프라인 완료
            completed_at = datetime.now()
            execution_time = completed_at - started_at
            
            result.update({
                "completed_at": completed_at.strftime("%Y-%m-%d %H:%M:%S"),
                "execution_time": str(execution_time),
                "final_status": "success"
            })
            
            # 결과 저장
            saved_file = self._save_pipeline_result(result)
            result["saved_file"] = saved_file
            
            print(f"\n🎉 파이프라인 실행 완료!")
            print(f"⏰ 실행 시간: {execution_time}")
            print(f"📊 최종 결과: {len(enriched_issues)}개 이슈 분석 완료")
            
            return result
            
        except Exception as e:
            error_msg = f"파이프라인 실행 실패: {e}"
            print(f"❌ {error_msg}")
            
            result.update({
                "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "final_status": "failed",
                "errors": [error_msg]
            })
            
            raise Exception(error_msg)
    
    def _prepare_api_data(self, crawling_result: Dict, enriched_issues: List[Dict]) -> Dict:
        """API 응답용 데이터 구성"""
        
        api_data = {
            "success": True,
            "data": {
                "total_crawled": len(crawling_result.get("all_issues", [])),
                "selected_count": len(enriched_issues),
                "selection_criteria": "주식시장 영향도 + RAG 분석",
                "selected_issues": []
            },
            "metadata": {
                "crawled_at": crawling_result.get("crawling_metadata", {}).get("timestamp", ""),
                "categories_processed": crawling_result.get("crawling_metadata", {}).get("categories_processed", []),
                "ai_filter_applied": True,
                "rag_analysis_applied": True,
                "filter_model": "gpt-4o-mini",
                "rag_model": "gpt-4o-mini",
                "rag_confidence": self._calculate_average_confidence(enriched_issues)
            }
        }
        
        # 이슈 데이터 변환
        for issue in enriched_issues:
            api_issue = {
                "이슈번호": issue.get("이슈번호", 0),
                "제목": issue.get("제목", ""),
                "내용": issue.get("원본내용", issue.get("내용", "")),
                "카테고리": issue.get("카테고리", ""),
                "추출시간": issue.get("추출시간", ""),
                "주식시장_관련성_점수": issue.get("주식시장_관련성_점수", 0),
                "순위": issue.get("rank", 0),
                
                # RAG 분석 결과
                "관련산업": issue.get("관련산업", []),
                "관련과거이슈": issue.get("관련과거이슈", []),
                "RAG분석신뢰도": issue.get("RAG분석신뢰도", 0.0),
            }
            api_data["data"]["selected_issues"].append(api_issue)
        
        # 순위별 정렬
        api_data["data"]["selected_issues"].sort(key=lambda x: x.get("순위", 999))
        
        return api_data
    
    def _calculate_average_confidence(self, enriched_issues: List[Dict]) -> float:
        """평균 RAG 신뢰도 계산"""
        if not enriched_issues:
            return 0.0
        
        confidences = [issue.get("RAG분석신뢰도", 0.0) for issue in enriched_issues]
        return round(sum(confidences) / len(confidences), 2)
    
    def _save_pipeline_result(self, result: Dict) -> str:
        """파이프라인 실행 결과 저장"""
        try:
            timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
            filename = f"{timestamp}_Pipeline_Results.json"
            filepath = self.data_dir / filename
            
            save_data = {
                **result,
                "file_info": {
                    "filename": filename,
                    "created_at": datetime.now().isoformat(),
                    "pipeline_version": "PipelineService_v1.0"
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 파이프라인 결과 저장: {filepath}")
            return str(filepath)
            
        except Exception as e:
            print(f"⚠️ 결과 저장 실패: {e}")
            return ""
    
    def get_latest_analyzed_issues(self) -> List[Dict]:
        """최신 분석된 이슈들 조회 (API용)"""
        try:
            # 1. MySQL에서 먼저 조회 시도
            from .database_service import DatabaseService
            db_service = DatabaseService()
            
            if db_service.is_initialized():
                mysql_data = db_service.get_latest_news_issues()
                if mysql_data:
                    print(f"📊 MySQL에서 {len(mysql_data)}개 이슈 조회")
                    return mysql_data
            
            # 2. MySQL에 데이터가 없으면 최신 파일에서 조회
            pipeline_files = list(self.data_dir.glob("*_Pipeline_Results.json"))
            if pipeline_files:
                latest_file = max(pipeline_files, key=lambda f: f.stat().st_mtime)
                
                with open(latest_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                api_data = data.get("api_ready_data", {})
                issues = api_data.get("data", {}).get("selected_issues", [])
                
                print(f"📂 파일에서 {len(issues)}개 이슈 조회: {latest_file.name}")
                return issues
            
            print("⚠️ 분석된 이슈 데이터가 없습니다.")
            return []
            
        except Exception as e:
            print(f"❌ 최신 이슈 조회 실패: {e}")
            return []