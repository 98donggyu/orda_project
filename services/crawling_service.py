# services/crawling_service.py
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from datetime import datetime
from typing import List, Dict, Optional

# config.py가 프로젝트 루트에 있다고 가정
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CRAWLING_TARGET_CATEGORIES, CRAWLING_ISSUES_PER_CATEGORY

class BigKindsCrawler:
    """BigKinds 웹사이트에서 최신 뉴스를 크롤링하는 서비스"""

    def __init__(self, headless: bool = True):
        self.target_categories = CRAWLING_TARGET_CATEGORIES
        self.issues_per_category = CRAWLING_ISSUES_PER_CATEGORY
        self.headless = headless
        self.driver = None
        self.wait = None

    def _setup_driver(self):
        """Selenium WebDriver 설정"""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
        
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 15)
            print("✅ WebDriver가 성공적으로 설정되었습니다.")
        except Exception as e:
            print(f"❌ WebDriver 설정 중 오류 발생: {e}")
            raise

    def _cleanup_driver(self):
        """WebDriver 종료"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.wait = None
            print("✅ WebDriver가 종료되었습니다.")

    def crawl_all_categories(self) -> List[Dict]:
        """모든 대상 카테고리의 뉴스를 크롤링"""
        print(f"🚀 BigKinds 뉴스 크롤링을 시작합니다. (대상: {', '.join(self.target_categories)})")
        self._setup_driver()
        all_issues = []
        
        try:
            self.driver.get("https://www.bigkinds.or.kr/")
            print("🌐 BigKinds 웹사이트에 접속했습니다.")
            time.sleep(3)
            
            total_issue_id = 1
            for category in self.target_categories:
                print(f"📂 '{category}' 카테고리 크롤링 중...")
                try:
                    self.driver.execute_script("window.scrollTo(0, 880);")
                    time.sleep(1)

                    cat_button_selector = f'a.issue-category[data-category="{category}"]'
                    cat_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, cat_button_selector)))
                    self.driver.execute_script("arguments[0].click();", cat_button)
                    time.sleep(4)

                    count = 0
                    for i in range(1, self.issues_per_category + 1):
                        try:
                            issue_selector = f'div.swiper-slide:nth-child({i}) .issue-item-link'
                            issue_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, issue_selector)))
                            
                            self.driver.execute_script("arguments[0].click();", issue_element)
                            time.sleep(2)

                            title_elem = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'p.issuPopTitle')))
                            content_elem = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'p.pT20.issuPopContent')))
                            
                            title = title_elem.text.strip()
                            content = content_elem.text.strip()

                            all_issues.append({
                                "이슈번호": total_issue_id,
                                "카테고리": category,
                                "제목": title,
                                "내용": content,
                                "추출시간": datetime.now().isoformat(),
                                "고유ID": f"{category}_{total_issue_id}"
                            })
                            total_issue_id += 1
                            count += 1
                            
                            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                            time.sleep(1)

                            if count >= self.issues_per_category:
                                break
                        
                        except (TimeoutException, NoSuchElementException) as e:
                            print(f"    - 이슈 {i} 처리 실패: {e}")
                            try:
                                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                            except:
                                pass
                            continue
                    print(f"  -> '{category}' 카테고리에서 {count}개 이슈 수집 완료.")

                except (TimeoutException, NoSuchElementException) as e:
                    print(f"❌ '{category}' 카테고리 처리 중 심각한 오류 발생: {e}")
                    continue
        
        finally:
            self._cleanup_driver()
            print(f"✅ 크롤링 완료. 총 {len(all_issues)}개의 이슈를 수집했습니다.")
        
        return all_issues

# --- Service Singleton ---
_crawler_instance = BigKindsCrawler()

def crawl_news() -> List[Dict]:
    """뉴스 크롤링 실행 함수"""
    return _crawler_instance.crawl_all_categories()

def get_health() -> dict:
    return {"name": "crawling_service", "status": "ok"}


# --- 테스트 실행 코드 ---
if __name__ == '__main__':
    print("... crawling_service.py 직접 실행 테스트 ...")
    
    # headless=False로 설정하여 브라우저 창을 직접 보면서 테스트합니다.
    test_crawler = BigKindsCrawler(headless=False) 
    
    try:
        crawled_data = test_crawler.crawl_all_categories()
        
        if crawled_data:
            print("\n--- 📊 크롤링 결과 (상위 3개) ---")
            for i, issue in enumerate(crawled_data[:3]):
                print(f"  {i+1}. [{issue['카테고리']}] {issue['제목'][:50]}...")
            print(f"\n✅ 총 {len(crawled_data)}개의 이슈를 성공적으로 크롤링했습니다.")
        else:
            print("\n⚠️ 크롤링된 데이터가 없습니다. 사이트 구조 변경이나 네트워크 문제를 확인하세요.")

    except Exception as e:
        import traceback
        print(f"\n❌ 테스트 중 심각한 오류 발생: {e}")
        traceback.print_exc()