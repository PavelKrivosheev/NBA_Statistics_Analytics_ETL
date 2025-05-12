import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
import pandas as pd
import logging
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/load_incremental_line_score_game.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Пул соединений PostgreSQL
pg_pool = SimpleConnectionPool(
    minconn=1, maxconn=4,
    dbname="nba_stats", user="admin", password="admin", host="postgres_db1", port="5432"
)


def log_to_postgres(process_name, object_name, layer, source_type, start_time, end_time, status, records_processed,
                    comment, operation_type):
    """Записывает информацию о загрузке в tech.load_logs."""
    conn = pg_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tech.load_logs (
                process_name, object_name, layer, source_type, start_time, end_time, 
                status, records_processed, comment, operation_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            process_name, object_name, layer, source_type,
            datetime.fromtimestamp(start_time),
            datetime.fromtimestamp(end_time) if end_time else None,
            status, records_processed, comment, operation_type
        ))
        conn.commit()
        logger.info(f"Лог записан в tech.load_logs: {object_name}, {status}, {operation_type}")
    except psycopg2.Error as e:
        logger.error(f"Ошибка записи лога в PostgreSQL: {e}")
        raise
    finally:
        cur.close()
        pg_pool.putconn(conn)


def check_quality_line_score_game(rows):
    """Проверяет качество данных для line_score_game."""
    if not rows:
        return "No data returned for the given year", 0

    df = pd.DataFrame([dict(row) for row in rows])

    # Проверка наличия ключевых столбцов
    required_columns = ['game_id', 'team_id_home', 'team_id_away']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        comment = f"Missing columns: {', '.join(missing_columns)}"
        logger.error(comment)
        return comment, len(df)

    missing_counts = {
        'game_id': df['game_id'].isna().sum(),
        'team_id_home': df['team_id_home'].isna().sum(),
        'team_id_away': df['team_id_away'].isna().sum()
    }
    duplicate_count = len(df) - df['game_id'].drop_duplicates().shape[0]
    format_issues = 0
    business_rule_issues = 0

    # Проверка числовых полей
    numeric_fields = [
        'pts_qtr1_home', 'pts_qtr2_home', 'pts_qtr3_home', 'pts_qtr4_home',
        'pts_ot1_home', 'pts_ot2_home', 'pts_ot3_home', 'pts_ot4_home', 'pts_home',
        'pts_qtr1_away', 'pts_qtr2_away', 'pts_qtr3_away', 'pts_qtr4_away',
        'pts_ot1_away', 'pts_ot2_away', 'pts_ot3_away', 'pts_ot4_away', 'pts_away'
    ]
    for field in numeric_fields:
        if field in df.columns:
            invalid = pd.to_numeric(df[field], errors='coerce').isna() & df[field].notna()
            format_issues += invalid.sum()
            negative = df[field].astype(float, errors='ignore').lt(0).sum()
            business_rule_issues += negative

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (game_id): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)


def load_incremental_line_score_game(source_path, year, dag_id, task_id):
    """Загружает инкрементальные данные для line_score_game в stage."""
    start_time = time.time()
    total_records = 0
    quality_comment = ""
    sqlite_table_name = "line_score"
    stage_table_name = "line_score_game"
    source_type = "SQLITE"

    try:
        # Подключение к PostgreSQL
        conn = pg_pool.getconn()
        cur = conn.cursor()

        # Подключение к SQLite
        sqlite_conn = sqlite3.connect(source_path)
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cur = sqlite_conn.cursor()

        # Список столбцов
        columns = [
            'game_date_est', 'game_id', 'team_id_home', 'team_abbreviation_home',
            'team_city_name_home', 'team_nickname_home', 'pts_qtr1_home', 'pts_qtr2_home',
            'pts_qtr3_home', 'pts_qtr4_home', 'pts_ot1_home', 'pts_ot2_home', 'pts_ot3_home',
            'pts_ot4_home', 'pts_home', 'team_id_away', 'team_abbreviation_away',
            'team_city_name_away', 'team_nickname_away', 'pts_qtr1_away', 'pts_qtr2_away',
            'pts_qtr3_away', 'pts_qtr4_away', 'pts_ot1_away', 'pts_ot2_away', 'pts_ot3_away',
            'pts_ot4_away', 'pts_away'
        ]

        # Проверка максимальной длины строк
        sqlite_cur.execute(f"PRAGMA table_info({sqlite_table_name})")
        valid_columns = [row['name'] for row in sqlite_cur.fetchall()]
        logger.info(f"Available columns in SQLite: {valid_columns}")
        query_columns = [col for col in columns if col in valid_columns]
        if not query_columns:
            raise ValueError(f"Нет валидных столбцов для таблицы {sqlite_table_name}")
        max_length_query = f"SELECT {', '.join([f'MAX(LENGTH(CAST({col} AS TEXT))) AS max_{col}' for col in query_columns])} FROM {sqlite_table_name} WHERE strftime('%Y', game_date_est) = '{year}'"
        sqlite_cur.execute(max_length_query)
        max_lengths = sqlite_cur.fetchone()
        for field, length in dict(max_lengths).items():
            if length and length > 100 and field in [
                'game_id', 'team_id_home', 'team_abbreviation_home', 'team_city_name_home',
                'team_nickname_home', 'pts_qtr1_home', 'pts_qtr2_home', 'pts_qtr3_home',
                'pts_qtr4_home', 'team_id_away', 'team_abbreviation_away', 'team_city_name_away',
                'team_nickname_away', 'pts_qtr1_away', 'pts_qtr2_away', 'pts_qtr3_away',
                'pts_qtr4_away'
            ]:
                logger.warning(f"Длина {field} ({length}) превышает VARCHAR(100)")

        # Извлечение и проверка данных
        select_query = f"""
            SELECT {', '.join(columns)}
            FROM {sqlite_table_name}
            WHERE strftime('%Y', game_date_est) = '{year}'
        """
        sqlite_cur.execute(select_query)
        rows = sqlite_cur.fetchall()
        quality_comment, total_rows = check_quality_line_score_game(rows)

        if rows:
            batch_size = 1000
            batch = []
            for row in rows:
                batch.append(tuple(row))
                if len(batch) >= batch_size:
                    query = f"INSERT INTO stage.{stage_table_name} ({', '.join(columns)}) VALUES %s"
                    execute_values(cur, query, batch)
                    total_records += len(batch)
                    batch = []
            if batch:
                query = f"INSERT INTO stage.{stage_table_name} ({', '.join(columns)}) VALUES %s"
                execute_values(cur, query, batch)
                total_records += len(batch)

        conn.commit()
        end_time = time.time()
        logger.info(f"Загрузка завершена для {stage_table_name}, год {year}. Всего загружено {total_records} записей")

        log_to_postgres(
            process_name=dag_id,
            object_name=stage_table_name,
            layer="STAGE",
            source_type=source_type,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            records_processed=total_records,
            comment=quality_comment,
            operation_type="INCREMENTAL_LOAD"
        )

    except Exception as e:
        end_time = time.time()
        error_message = str(e)
        logger.error(f"Ошибка загрузки для {stage_table_name}, год {year}: {error_message}")
        log_to_postgres(
            process_name=dag_id,
            object_name=stage_table_name,
            layer="STAGE",
            source_type=source_type,
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            records_processed=total_records,
            comment=f"Error: {error_message}; {quality_comment}",
            operation_type="INCREMENTAL_LOAD"
        )
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            pg_pool.putconn(conn)
        if 'sqlite_conn' in locals():
            sqlite_conn.close()