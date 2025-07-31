# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# --- 기본 경로 설정 ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

# --- 데이터 파일 경로 ---
INDUSTRY_CSV_PATH = DATA_DIR / "산업DB.v.0.3.csv"
PAST_NEWS_CSV_PATH = DATA_DIR / "Past_news.csv"
DATABASE_PATH = DATA_DIR / "orda.db"

# --- API 및 모델 설정 ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# LLM 모델 이름
MAIN_LLM_MODEL = "gpt-4o"
FAST_LLM_MODEL = "gpt-4o-mini"
EMBEDDING_MODEL = "text-embedding-3-small"

# --- Pinecone 설정 ---
PINECONE_INDEX_NAME = "ordaproject"
PINECONE_NAMESPACE_INDUSTRY = "industry"
PINECONE_NAMESPACE_PAST_ISSUE = "past_issue"

# --- 크롤링 설정 ---
CRAWLING_TARGET_CATEGORIES = ["정치", "경제", "사회", "문화", "국제", "지역", "IT과학"]
CRAWLING_ISSUES_PER_CATEGORY = 10

# --- 시뮬레이션 설정 ---
YFINANCE_TICKER_SUFFIX_KOSPI = ".KS"
YFINANCE_TICKER_SUFFIX_KOSDAQ = ".KQ"

# --- FastAPI 서버 설정 ---
API_TITLE = "오르다(Orda) API"
API_VERSION = "1.0.0"
API_DESCRIPTION = "투자 학습 플랫폼 백엔드 API"
CORS_ALLOW_ORIGINS = ["*"]