import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/load_incremental_shot_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Пул соединений PostgreSQL
pg_pool = SimpleConnectionPool(
    minconn=1, maxconn=4,
    dbname="nba_stats", user="admin", password="admin", host="postgres_db1", port="5432"
)

def log_to_postgres(process_name, object_name, layer, source_type, start_time, end_time, status, records_processed, comment, operation_type):
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

def check_quality_shot_data(df, year):
    """Проверяет качество данных для shot_data."""
    if df.empty:
        return f"No data returned for year {year}", 0

    logger.info(f"Columns in DataFrame: {list(df.columns)}")

    # Проверка наличия ключевых столбцов
    required_columns = ['player_id', 'game_id', 'team_id']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        comment = f"Missing columns: {', '.join(missing_columns)}"
        logger.error(comment)
        return comment, len(df)

    # Проверка пропущенных значений
    missing_counts = {
        'player_id': df['player_id'].isna().sum(),
        'game_id': df['game_id'].isna().sum(),
        'team_id': df['team_id'].isna().sum()
    }

    # Проверка дубликатов
    duplicate_count = len(df) - df[['player_id', 'game_id', 'loc_x', 'loc_y', 'quarter']].drop_duplicates().shape[0]

    # Проверка форматов
    format_issues = 0
    business_rule_issues = 0

    # Проверка числовых полей
    numeric_fields = ['loc_x', 'loc_y', 'shot_distance', 'quarter', 'mins_left', 'secs_left']
    for field in numeric_fields:
        if field in df.columns:
            invalid = df[field].isna() & df[field].notnull()  # Проверка некорректных значений
            format_issues += invalid.sum()
            if field in ['shot_distance', 'quarter', 'mins_left', 'secs_left']:
                negative = (df[field] < 0).sum()
                business_rule_issues += negative

    # Проверка shot_made
    if 'shot_made' in df.columns:
        invalid = df['shot_made'].apply(lambda x: x not in [True, False]).sum()
        format_issues += invalid

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (player_id, game_id, loc_x, loc_y, quarter): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def load_incremental_shot_data(source_path, year, dag_id, task_id):
    """Загружает инкрементальные данные для shot_data в stage."""
    start_time = time.time()
    total_records = 0
    skipped_rows = 0
    quality_comment = ""
    table_name = "shot_data"
    source_type = "CSV"

    try:
        logger.info(f"Attempting to read CSV file: {source_path}")

        # Проверка существования файла
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"CSV file not found: {source_path}")

        # Определение типов данных для ускорения чтения
        dtypes = {
            'SEASON_1': 'str',
            'SEASON_2': 'str',
            'TEAM_ID': 'str',
            'TEAM_NAME': 'str',
            'PLAYER_ID': 'str',
            'PLAYER_NAME': 'str',
            'POSITION_GROUP': 'str',
            'POSITION': 'str',
            'GAME_DATE': 'str',
            'GAME_ID': 'str',
            'HOME_TEAM': 'str',
            'AWAY_TEAM': 'str',
            'EVENT_TYPE': 'str',
            'SHOT_MADE': 'bool',
            'ACTION_TYPE': 'str',
            'SHOT_TYPE': 'str',
            'BASIC_ZONE': 'str',
            'ZONE_NAME': 'str',
            'ZONE_ABB': 'str',
            'ZONE_RANGE': 'str',
            'LOC_X': 'float32',
            'LOC_Y': 'float32',
            'SHOT_DISTANCE': 'int32',
            'QUARTER': 'int32',
            'MINS_LEFT': 'int32',
            'SECS_LEFT': 'int32'
        }

        # Чтение CSV с указанием типов
        df = pd.read_csv(source_path, dtype=dtypes, low_memory=False)
        logger.info(f"Total rows in CSV: {len(df)}")
        df.columns = df.columns.str.lower()  # Приведение к нижнему регистру
        logger.info(f"Processed columns: {list(df.columns)}")

        # Проверка наличия столбца season_1
        if 'season_1' not in df.columns:
            logger.error("Column 'season_1' not found in CSV")
            raise ValueError("Column 'season_1' not found in CSV")

        logger.info(f"Unique values in season_1: {df['season_1'].unique()}")

        # Фильтрация данных по году
        df = df[df['season_1'] == str(year)]
        logger.info(f"Rows after filtering by year {year}: {len(df)}")
        quality_comment, total_rows = check_quality_shot_data(df, year)

        if not df.empty:
            # Векторизованное преобразование данных
            df['game_date'] = pd.to_datetime(df['game_date'], format='%m-%d-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            df['shot_made'] = df['shot_made'].apply(lambda x: x in [True, 'TRUE'])

            # Проверка и обработка пропущенных значений
            required_columns = ['player_id', 'game_id', 'event_type']
            invalid_rows = df[required_columns].isna().any(axis=1)
            skipped_rows = invalid_rows.sum()
            if skipped_rows > 0:
                logger.warning(f"Пропущено {skipped_rows} строк с пустыми player_id, game_id или event_type")
                df = df[~invalid_rows]

            # Проверка максимальной длины строк
            string_columns = [
                'season_1', 'season_2', 'team_id', 'team_name', 'player_id', 'player_name',
                'position_group', 'position', 'game_id', 'home_team', 'away_team', 'event_type',
                'action_type', 'shot_type', 'basic_zone', 'zone_name', 'zone_abb', 'zone_range'
            ]
            max_lengths = {'season_1': 10, 'season_2': 10, 'player_id': 50, 'game_id': 50, 'zone_abb': 50, 'position_group': 50, 'position': 50}
            for col in string_columns:
                if col in df.columns:
                    max_length = df[col].astype(str).str.len().max()
                    if pd.notna(max_length) and max_length > max_lengths.get(col, 100):
                        logger.warning(f"Длина {col} ({max_length}) превышает допустимую")

            # Подготовка данных для вставки
            columns = [
                'season_1', 'season_2', 'team_id', 'team_name', 'player_id', 'player_name',
                'position_group', 'position', 'game_date', 'game_id', 'home_team', 'away_team',
                'event_type', 'shot_made', 'action_type', 'shot_type', 'basic_zone', 'zone_name',
                'zone_abb', 'zone_range', 'loc_x', 'loc_y', 'shot_distance', 'quarter',
                'mins_left', 'secs_left'
            ]

            # Преобразование данных в кортежи для execute_values
            data_tuples = [
                tuple(
                    None if pd.isna(row[col]) else
                    row[col].strftime('%Y-%m-%d') if col == 'game_date' and isinstance(row[col], (pd.Timestamp, datetime)) else
                    row[col]
                    for col in columns
                )
                for _, row in df[columns].iterrows()
            ]

            # Подключение к PostgreSQL
            conn = pg_pool.getconn()
            cur = conn.cursor()

            # Вставка данных чанками
            batch_size = 5000  # Увеличен размер чанка
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                query = f"INSERT INTO stage.{table_name} ({', '.join(columns)}) VALUES %s"
                execute_values(cur, query, batch)
                total_records += len(batch)
                if i % 25000 == 0:  # Логирование каждые 25,000 строк
                    logger.info(f"Inserted {len(batch)} records, total: {total_records}")

            conn.commit()
        else:
            quality_comment = f"No data returned for year {year} after filtering"
            logger.warning(quality_comment)

        end_time = time.time()
        status = "SUCCESS" if total_records > 0 else "WARNING"
        if skipped_rows > 0:
            quality_comment += f"; Skipped rows: {skipped_rows}"
        logger.info(f"Загрузка завершена для {table_name}, год {year}. Всего загружено {total_records} записей, пропущено {skipped_rows}")

        log_to_postgres(
            process_name=dag_id,
            object_name=table_name,
            layer="STAGE",
            source_type=source_type,
            start_time=start_time,
            end_time=end_time,
            status=status,
            records_processed=total_records,
            comment=quality_comment,
            operation_type="INCREMENTAL_LOAD"
        )

    except Exception as e:
        end_time = time.time()
        error_message = str(e)
        logger.error(f"Ошибка загрузки для {table_name}, год {year}: {error_message}")
        log_to_postgres(
            process_name=dag_id,
            object_name=table_name,
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