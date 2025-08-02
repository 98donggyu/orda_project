import os
from pathlib import Path
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# --- 기본 경로 ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA2_DIR = BASE_DIR / "data2"

# 디렉토리 생성
DATA2_DIR.mkdir(exist_ok=True)

# --- API 키 ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "ordaproject")

# --- 모델 설정 ---
MAIN_LLM_MODEL = "gpt-4o"
FAST_LLM_MODEL = "gpt-4o-mini"
EMBEDDING_MODEL = "text-embedding-3-small"

# --- MySQL 설정 ---
DATABASE_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3308)),
    'user': os.getenv('MYSQL_USER', 'orda_user'),
    'password': os.getenv('MYSQL_PASSWORD', 'orda_password'),
    'database': os.getenv('MYSQL_DATABASE', 'orda_news'),
    'charset': 'utf8mb4',
    'autocommit': True
}

# --- FastAPI 설정 ---
API_TITLE = "오르다(Orda) API"
API_VERSION = "2.0.0"
API_DESCRIPTION = "투자 학습 플랫폼 백엔드 API"
CORS_ALLOW_ORIGINS = ["*"]

print(f"✅ Config 로드 완료 - MySQL 포트: {DATABASE_CONFIG['port']}")