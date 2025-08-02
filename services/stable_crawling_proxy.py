"""
안정적인 크롤링 프록시 - BigKindsCrawler의 안정성 향상
원본 코드 수정 없이 프록시 패턴으로 안정성 보강
"""

import os
import sys
import json
import time
import random
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging

# 프로젝트 루트 설정
current_dir = Path(__file__).parent
project_root = current_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from services.crawling_bigkinds import BigKindsCrawler

logger = logging.getLogger(__name__)

class StableBigKindsCrawler:
    """BigKindsCrawler의 안정성 향상 프록시"""
    
    def __init__(self, data_dir: str = "data2", headless: bool = True, issues_per_category: int = 10):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.headless = headless
        self.issues_per_category = issues_per_category
        
        # 재시도 설정
        self.max_retries = 3
        self.retry_delay = 5  # 초
        
        logger.info("✅ 안정적인 크롤링 프록시 초기화")
    
    def crawl_all_categories_with_retry(self) -> Dict:
        """재시도 로직이 포함된 안정적인 크롤링"""
        
        for attempt in range(1, self.max_retries + 1):
            logger.info(f"🔄 크롤링 시도 {attempt}/{self.max_retries}")
            
            try:
                # 각 시도마다 새로운 크롤러 인스턴스 생성
                crawler = self._create_enhanced_crawler()
                
                # 크롤링 실행
                result = crawler.crawl_all_categories()
                
                # 성공적으로 완료되면 결과 반환
                if result.get('total_issues', 0) > 0:
                    logger.info(f"✅ 크롤링 성공 (시도 {attempt}): {result['total_issues']}개 이슈")
                    return result
                else:
                    logger.warning(f"⚠️ 크롤링 완료되었지만 이슈가 없음 (시도 {attempt})")
                    
            except Exception as e:
                logger.error(f"❌ 크롤링 실패 (시도 {attempt}): {e}")
                
                # 마지막 시도가 아니면 대기 후 재시도
                if attempt < self.max_retries:
                    delay = self.retry_delay * attempt  # 지수적 백오프
                    logger.info(f"⏳ {delay}초 대기 후 재시도...")
                    time.sleep(delay)
                else:
                    logger.error(f"❌ 모든 재시도 실패. 최종 실패 처리")
                    return self._create_failed_result(str(e))
        
        return self._create_failed_result("모든 재시도 실패")
    
    def _create_enhanced_crawler(self) -> BigKindsCrawler:
        """향상된 설정의 크롤러 생성"""
        
        # 기존 BigKindsCrawler 클래스를 상속받아 설정 강화
        class EnhancedBigKindsCrawler(BigKindsCrawler):
            def _setup_driver(self):
                """Chrome 드라이버 설정 - 안정성 강화 버전"""
                from selenium import webdriver
                from selenium.webdriver.support.ui import WebDriverWait
                
                options = webdriver.ChromeOptions()
                
                # 기본 헤드리스 설정
                if self.headless:
                    options.add_argument("--headless")
                    options.add_argument("--no-sandbox")
                    options.add_argument("--disable-dev-shm-usage")
                else:
                    options.add_argument("--start-maximized")
                
                # 향상된 안정성 설정
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                
                # 추가 안정성 설정
                options.add_argument("--disable-web-security")
                options.add_argument("--allow-running-insecure-content")
                options.add_argument("--disable-features=VizDisplayCompositor")
                
                # 리소스 최적화
                options.add_argument("--disable-extensions")
                options.add_argument("--disable-plugins")
                options.add_argument("--disable-images")  # 이미지 로딩 비활성화로 속도 향상
                options.add_argument("--disable-javascript")  # JS 비활성화 (필요시)
                
                # 메모리 최적화
                options.add_argument("--memory-pressure-off")
                options.add_argument("--max_old_space_size=4096")
                
                # 로그 레벨 조정 (에러 메시지 줄이기)
                options.add_argument("--log-level=3")
                options.add_experimental_option('excludeSwitches', ['enable-logging'])
                options.add_experimental_option('useAutomationExtension', False)
                
                # User-Agent 설정 (탐지 방지)
                options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                
                self.driver = webdriver.Chrome(options=options)
                
                # webdriver 속성 숨기기
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                
                # 대기 시간 증가 (기존 15초 → 30초)
                self.wait = WebDriverWait(self.driver, 30)
                
                print("✅ 향상된 Chrome 드라이버 설정 완료")
            
            def _navigate_to_bigkinds(self):
                """BigKinds 사이트 접속 - 안정성 강화"""
                print("🌐 BigKinds 사이트 접속 중... (안정성 강화)")
                
                try:
                    self.driver.get("https://www.bigkinds.or.kr/")
                    
                    # 페이지 로딩 완료까지 충분히 대기
                    time.sleep(5)  # 기존 3초 → 5초
                    
                    # 페이지 로딩 확인
                    self.driver.execute_script("return document.readyState") == "complete"
                    
                    # 팝업 닫기 시도 (여러 선택자로)
                    popup_selectors = [
                        ".popup-close-btn",
                        ".modal-close",
                        ".close-btn",
                        "[data-dismiss='modal']"
                    ]
                    
                    for selector in popup_selectors:
                        try:
                            popup = self.driver.find_element(By.CSS_SELECTOR, selector)
                            popup.click()
                            time.sleep(1)
                        except:
                            continue
                    
                    print("✅ BigKinds 사이트 접속 완료 (안정성 강화)")
                    return True
                    
                except Exception as e:
                    print(f"❌ 사이트 접속 실패: {str(e)}")
                    return False
            
            def _crawl_issues_in_category(self, category: str, max_issues: int) -> List[Dict]:
                """카테고리 내 이슈들 크롤링 - 안정성 강화"""
                issues = []
                failed_count = 0
                max_failures = 3  # 연속 실패 허용 횟수
                
                for i in range(1, max_issues + 1):
                    print(f"  📰 [{i}/{max_issues}] 이슈 처리 중... (실패: {failed_count})")
                    
                    try:
                        # 3번째 이슈부터는 슬라이드 넘기기 필요
                        if i >= 3:
                            self._navigate_slides(i)
                        
                        # 이슈 데이터 추출 (재시도 로직 포함)
                        issue_data = self._extract_issue_data_with_retry(i, category)
                        
                        if issue_data:
                            issues.append(issue_data)
                            print(f"    ✅ 이슈 {i} 추출 완료: {issue_data['제목'][:30]}...")
                            failed_count = 0  # 성공 시 실패 카운트 리셋
                        else:
                            failed_count += 1
                            print(f"    ⚠️ 이슈 {i} 데이터 없음")
                        
                        # 팝업 닫기 및 위치 복원
                        self._close_popup_and_restore()
                        
                        # 이슈 간 랜덤 대기 (탐지 방지)
                        time.sleep(random.uniform(1, 2))
                        
                        # 연속 실패가 많으면 조기 종료
                        if failed_count >= max_failures:
                            print(f"    ⚠️ 연속 {max_failures}회 실패로 조기 종료")
                            break
                        
                    except Exception as e:
                        failed_count += 1
                        print(f"    ❌ 이슈 {i} 처리 실패: {e}")
                        
                        # 연속 실패가 많으면 조기 종료
                        if failed_count >= max_failures:
                            print(f"    ⚠️ 연속 {max_failures}회 실패로 조기 종료")
                            break
                        
                        # 실패 시 복구 시도
                        try:
                            self._recover_from_error()
                        except:
                            pass
                        
                        continue
                
                return issues
            
            def _extract_issue_data_with_retry(self, issue_num: int, category: str, max_retries: int = 3) -> Optional[Dict]:
                """이슈 데이터 추출 - 재시도 로직 포함"""
                for attempt in range(1, max_retries + 1):
                    try:
                        return self._extract_issue_data(issue_num, category)
                    except Exception as e:
                        print(f"    🔄 이슈 {issue_num} 추출 재시도 {attempt}/{max_retries}: {e}")
                        if attempt < max_retries:
                            time.sleep(2)  # 재시도 전 대기
                        else:
                            print(f"    ❌ 이슈 {issue_num} 최종 실패")
                            return None
            
            def _recover_from_error(self):
                """에러 발생 시 복구 시도"""
                try:
                    # ESC 키 누르기
                    from selenium.webdriver.common.action_chains import ActionChains
                    from selenium.webdriver.common.keys import Keys
                    ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                    time.sleep(1)
                    
                    # 이슈 섹션으로 다시 스크롤
                    self.driver.execute_script("window.scrollTo(0, 880);")
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"    ⚠️ 에러 복구 실패: {e}")
        
        # 향상된 크롤러 인스턴스 생성
        return EnhancedBigKindsCrawler(
            data_dir=str(self.data_dir),
            headless=self.headless,
            issues_per_category=self.issues_per_category
        )
    
    def _create_failed_result(self, error_msg: str) -> Dict:
        """실패 결과 생성"""
        return {
            "total_issues": 0,
            "categories": {},
            "crawling_log": [{
                "status": "failed",
                "error": error_msg,
                "timestamp": datetime.now().strftime("%H:%M:%S")
            }],
            "all_issues": [],
            "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "execution_method": "stable_proxy_with_retry",
            "error": error_msg
        }


class StableCrawlingService:
    """안정적인 크롤링 서비스 (기존 CrawlingService와 동일한 인터페이스)"""
    
    def __init__(self, data_dir: str = "data2", headless: bool = True):
        self.data_dir = data_dir
        self.headless = headless
        self.stable_crawler = StableBigKindsCrawler(
            data_dir=data_dir,
            headless=headless
        )
        
        # AI 필터링 초기화
        self._init_ai_filtering()
        
        logger.info("✅ 안정적인 크롤링 서비스 초기화 완료")
    
    def _init_ai_filtering(self):
        """AI 필터링 초기화"""
        try:
            from dotenv import load_dotenv
            from langchain_openai import ChatOpenAI
            
            load_dotenv(override=True)
            self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
            logger.info("✅ OpenAI LLM 초기화 완료")
        except Exception as e:
            logger.warning(f"⚠️ OpenAI LLM 초기화 실패: {e}")
            self.llm = None
    
    def crawl_and_filter_news(self, 
                             issues_per_category: int = 10,
                             target_filtered_count: int = 5) -> Dict:
        """안정적인 크롤링 + AI 필터링"""
        
        logger.info(f"🕷️ 안정적인 크롤링 시작: 카테고리별 {issues_per_category}개씩")
        
        # 크롤러 설정 업데이트
        self.stable_crawler.issues_per_category = issues_per_category
        
        # Step 1: 안정적인 크롤링
        crawling_result = self.stable_crawler.crawl_all_categories_with_retry()
        
        # Step 2: AI 필터링
        all_issues = crawling_result.get("all_issues", [])
        if all_issues and self.llm:
            filtering_result = self._filter_by_stock_relevance(all_issues, target_filtered_count)
        else:
            filtering_reason = "no_llm" if not self.llm else "no_issues_to_filter"
            filtering_result = {
                "selected_issues": all_issues[:target_filtered_count] if all_issues else [],
                "filter_metadata": {
                    "filtering_method": filtering_reason,
                    "original_count": len(all_issues),
                    "selected_count": min(len(all_issues), target_filtered_count),
                    "filtered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        
        result = {
            **crawling_result,
            "filtered_issues": filtering_result["selected_issues"],
            "filter_metadata": filtering_result["filter_metadata"],
            "execution_method": "stable_proxy_with_ai_filtering"
        }
        
        logger.info(f"✅ 안정적인 크롤링 및 필터링 완료")
        return result
    
    def _filter_by_stock_relevance(self, all_issues: List[Dict], target_count: int) -> Dict:
        """AI 필터링 (기존과 동일)"""
        logger.info(f"🤖 AI 필터링 시작: {len(all_issues)}개 → {target_count}개 선별")
        
        if not self.llm:
            return {
                "selected_issues": all_issues[:target_count],
                "filter_metadata": {
                    "filtering_method": "no_ai_simple_slice",
                    "original_count": len(all_issues),
                    "selected_count": min(len(all_issues), target_count),
                    "filtered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        
        scored_issues = []
        
        for i, issue in enumerate(all_issues, 1):
            try:
                relevance_score = self._analyze_stock_market_relevance(issue)
                scored_issue = issue.copy()
                scored_issue.update({
                    "주식시장_관련성_점수": relevance_score["종합점수"],
                    "관련성_분석": relevance_score
                })
                scored_issues.append(scored_issue)
            except Exception as e:
                logger.warning(f"⚠️ 이슈 {i} AI 분석 실패: {e}")
                scored_issue = issue.copy()
                scored_issue.update({
                    "주식시장_관련성_점수": 5,
                    "관련성_분석": {"종합점수": 5, "분석근거": f"분석 실패: {e}"}
                })
                scored_issues.append(scored_issue)
        
        scored_issues.sort(key=lambda x: x["주식시장_관련성_점수"], reverse=True)
        selected_issues = scored_issues[:target_count]
        
        for rank, issue in enumerate(selected_issues, 1):
            issue["rank"] = rank
        
        return {
            "selected_issues": selected_issues,
            "filter_metadata": {
                "filtering_method": "stable_gpt-4o-mini_stock_relevance",
                "original_count": len(all_issues),
                "selected_count": len(selected_issues),
                "average_score": sum(issue["주식시장_관련성_점수"] for issue in selected_issues) / len(selected_issues) if selected_issues else 0,
                "filtered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    
    def _analyze_stock_market_relevance(self, issue: Dict) -> Dict:
        """AI 주식시장 관련성 분석"""
        from langchain_core.prompts import ChatPromptTemplate
        from langchain_core.output_parsers import JsonOutputParser
        
        if not self.llm:
            raise Exception("LLM이 초기화되지 않았습니다")
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", "뉴스의 주식시장 관련성을 1-10점으로 평가해주세요."),
            ("human", "제목: {title}\n내용: {content}")
        ])
        
        try:
            parser = JsonOutputParser()
            chain = prompt | self.llm | parser
            
            title = issue.get("제목", issue.get("title", "제목 없음"))
            content = issue.get("내용", issue.get("content", "내용 없음"))
            
            result = chain.invoke({"title": title, "content": content})
            
            return {
                "직접적_기업영향": result.get("직접적_기업영향", 5),
                "산업_전반영향": result.get("산업_전반영향", 5),
                "거시경제_영향": result.get("거시경제_영향", 5),
                "투자심리_영향": result.get("투자심리_영향", 5),
                "종합점수": result.get("종합점수", 5),
                "분석근거": result.get("분석근거", "AI 분석 완료")
            }
        except Exception as e:
            raise


# 편의 함수
def crawl_with_stable_proxy(headless: bool = True, 
                           issues_per_category: int = 10, 
                           target_filtered_count: int = 5) -> Dict:
    """안정적인 프록시로 크롤링"""
    service = StableCrawlingService(headless=headless)
    return service.crawl_and_filter_news(issues_per_category, target_filtered_count)