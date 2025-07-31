# services/database_service.py
import sqlite3
import pandas as pd
import json
import aiosqlite
from typing import List, Dict, Any, Optional

from config import DATABASE_PATH, INDUSTRY_CSV_PATH, PAST_NEWS_CSV_PATH
from models.schemas import SimulationRequest, SimulationResponse

class OrdaDatabase:
    """DB 파일 생성 및 CSV 데이터 로딩을 담당하는 동기 클래스"""
    def __init__(self, db_path=DATABASE_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(exist_ok=True, parents=True)

    def setup_database(self):
        """DB 테이블 생성 및 초기 데이터 로딩"""
        if self.db_path.exists():
            print("✅ 데이터베이스 파일이 이미 존재합니다. 초기화를 건너뜁니다.")
            return

        print("🚀 데이터베이스 초기 설정을 시작합니다.")
        self._create_tables()
        self._import_csv_data()
        print("🎉 데이터베이스 초기 설정 완료!")

    def _create_tables(self):
        """데이터베이스 테이블 생성"""
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS industries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            krx_name TEXT NOT NULL UNIQUE,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS past_issues (
            id TEXT PRIMARY KEY,
            issue_name TEXT NOT NULL,
            contents TEXT,
            related_industries TEXT,
            industry_reason TEXT,
            start_date TEXT,
            end_date TEXT,
            evidence_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS current_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_number INTEGER,
            title TEXT NOT NULL,
            content TEXT,
            crawled_at TIMESTAMP,
            source TEXT DEFAULT 'bigkinds',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS simulation_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scenario_id TEXT NOT NULL,
            investment_amount INTEGER,
            investment_period INTEGER,
            selected_stocks TEXT,
            total_return_pct REAL,
            final_amount INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executescript(create_tables_sql)
            print("✅ 데이터베이스 테이블이 성공적으로 생성되었습니다.")
        except Exception as e:
            print(f"❌ 테이블 생성 실패: {e}")
            raise

    def _import_csv_data(self):
        """CSV 파일들을 SQLite로 가져오기"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 산업 분류 데이터
                if INDUSTRY_CSV_PATH.exists():
                    df_ind = pd.read_csv(INDUSTRY_CSV_PATH).dropna(subset=['KRX 업종명'])
                    df_ind = df_ind.drop_duplicates(subset=['KRX 업종명'])
                    df_ind[['KRX 업종명', '상세내용']].rename(columns={'KRX 업종명': 'krx_name', '상세내용': 'description'}).to_sql('industries', conn, if_exists='append', index=False)
                    print(f"✅ 산업 분류 데이터 {len(df_ind)}건을 가져왔습니다.")
                else:
                    print(f"⚠️ 경고: 산업 DB 파일({INDUSTRY_CSV_PATH})을 찾을 수 없습니다.")

                # 과거 이슈 데이터
                if PAST_NEWS_CSV_PATH.exists():
                    df_past = pd.read_csv(PAST_NEWS_CSV_PATH).dropna(subset=['ID'])
                    df_past = df_past.fillna('')
                    df_past_renamed = df_past.rename(columns={
                        'ID': 'id', 'Issue_name': 'issue_name', 'Contents': 'contents',
                        '관련 산업': 'related_industries', '산업 이유': 'industry_reason',
                        'Start_date': 'start_date', 'Fin_date': 'end_date', '근거자료': 'evidence_source'
                    })
                    df_past_renamed[['id', 'issue_name', 'contents', 'related_industries', 'industry_reason', 'start_date', 'end_date', 'evidence_source']].to_sql('past_issues', conn, if_exists='append', index=False)
                    print(f"✅ 과거 이슈 데이터 {len(df_past)}건을 가져왔습니다.")
                else:
                    print(f"⚠️ 경고: 과거 뉴스 파일({PAST_NEWS_CSV_PATH})을 찾을 수 없습니다.")
        except Exception as e:
            print(f"❌ CSV 데이터 가져오기 실패: {e}")
            raise

    def get_database_stats(self) -> Dict[str, Any]:
        """데이터베이스 통계 정보 반환"""
        stats = {}
        try:
            with sqlite3.connect(self.db_path) as conn:
                tables = ['industries', 'past_issues', 'current_issues', 'simulation_results']
                for table in tables:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    stats[table] = count
            stats['db_size_mb'] = round(self.db_path.stat().st_size / (1024 * 1024), 2)
            return stats
        except Exception as e:
            print(f"❌ DB 통계 조회 실패: {e}")
            return {}

class OrdaDatabaseAPI:
    """FastAPI에서 사용할 비동기 DB 쿼리 클래스"""
    def __init__(self, db_path=DATABASE_PATH):
        self.db_path = str(db_path)

    async def get_past_news(self, limit: int, search: Optional[str], industry: Optional[str]) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = "SELECT * FROM past_issues WHERE 1=1"
            params = []
            if search:
                query += " AND (issue_name LIKE ? OR contents LIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])
            if industry:
                query += " AND related_industries LIKE ?"
                params.append(f"%{industry}%")
            query += " ORDER BY start_date DESC LIMIT ?"
            params.append(limit)
            
            cursor = await db.execute(query, tuple(params))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_industries(self, search: Optional[str], limit: int) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = "SELECT * FROM industries WHERE 1=1"
            params = []
            if search:
                query += " AND (krx_name LIKE ? OR description LIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])
            query += " ORDER BY krx_name LIMIT ?"
            params.append(limit)

            cursor = await db.execute(query, tuple(params))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def save_simulation_result(self, req: SimulationRequest, res: SimulationResponse):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO simulation_results 
                (scenario_id, investment_amount, investment_period, selected_stocks, total_return_pct, final_amount)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    req.scenario_id,
                    req.investment_amount,
                    req.investment_period,
                    json.dumps([s.dict() for s in req.selected_stocks], ensure_ascii=False),
                    res.simulation_results.total_return_pct,
                    res.simulation_results.final_amount,
                ),
            )
            await db.commit()

# --- Service Singleton ---
orda_db: Optional[OrdaDatabase] = None
db_api: Optional[OrdaDatabaseAPI] = None

def initialize():
    """서비스 초기화 함수"""
    global orda_db, db_api
    if orda_db is None:
        orda_db = OrdaDatabase()
        orda_db.setup_database()
    if db_api is None:
        db_api = OrdaDatabaseAPI()
    print("✅ Database Service initialized.")

def is_initialized() -> bool:
    return orda_db is not None and db_api is not None

def get_health() -> dict:
    status = "ok" if is_initialized() and DATABASE_PATH.exists() else "error"
    return {"name": "database_service", "status": status}