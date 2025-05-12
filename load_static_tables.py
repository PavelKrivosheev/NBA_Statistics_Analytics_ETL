import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
import pandas as pd
import logging
import time
from datetime import datetime
import hashlib

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/load_static_tables.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Пул соединений PostgreSQL
pg_pool = SimpleConnectionPool(
    minconn=1, maxconn=4,
    dbname="nba_stats", user="admin", password="admin", host="postgres_db1", port="5432"
)

# Конфигурация таблиц
TABLE_CONFIG = {
    "common_player_info": {
        "source_type": "SQLITE",
        "select_query": """
            SELECT person_id, first_name, last_name, birthdate, school, country, last_affiliation,
                   height, weight, season_exp, jersey, position, rosterstatus,
                   games_played_current_season_flag, team_id, team_name, team_abbreviation,
                   team_code, team_city, from_year, to_year, dleague_flag, nba_flag,
                   games_played_flag, draft_year, draft_round, draft_number
            FROM common_player_info
        """,
        "check_quality": lambda rows, cols: check_quality_common_player_info(rows, cols),
        "unique_key": ["person_id"]
    },
    "other_stats": {
        "source_type": "SQLITE",
        "select_query": """
            SELECT game_id, team_id_home, team_abbreviation_home, team_city_home, pts_paint_home,
                   pts_2nd_chance_home, pts_fb_home, largest_lead_home, lead_changes, times_tied,
                   team_turnovers_home, total_turnovers_home, team_rebounds_home, pts_off_to_home,
                   team_id_away, team_abbreviation_away, team_city_away, pts_paint_away,
                   pts_2nd_chance_away, pts_fb_away, largest_lead_away, team_turnovers_away,
                   total_turnovers_away, team_rebounds_away, pts_off_to_away
            FROM other_stats
        """,
        "check_quality": lambda rows, cols: check_quality_other_stats(rows, cols),
        "unique_key": ["game_id", "team_id_home", "team_id_away"]
    },
    "player": {
        "source_type": "SQLITE",
        "select_query": """
            SELECT id, full_name, first_name, last_name
            FROM player
        """,
        "check_quality": lambda rows, cols: check_quality_player(rows, cols),
        "unique_key": ["id"]
    },
    "team": {
        "source_type": "SQLITE",
        "select_query": """
            SELECT id, full_name, abbreviation, nickname, city, state, year_founded
            FROM team
        """,
        "check_quality": lambda rows, cols: check_quality_team(rows, cols),
        "unique_key": ["id"]
    },
    "team_details": {
        "source_type": "SQLITE",
        "select_query": """
            SELECT team_id, abbreviation, nickname, yearfounded, city, arena, arenacapacity,
                   owner, generalmanager, headcoach, instagram
            FROM team_details
        """,
        "check_quality": lambda rows, cols: check_quality_team_details(rows, cols),
        "unique_key": ["team_id"]
    },
    "draft_history": {
        "source_type": "SQLITE",
        "select_query": """
            SELECT person_id, player_name, season, round_number, round_pick, overall_pick,
                   draft_type, team_id, team_city, team_name, team_abbreviation,
                   organization, organization_type, player_profile_flag
            FROM draft_history
        """,
        "check_quality": lambda rows, cols: check_quality_draft_history(rows, cols),
        "unique_key": ["person_id", "season"]
    },
    "draft_combine_stats": {
        "source_type": "SQLITE",
        "select_query": """
            SELECT season, player_id, first_name, last_name, position,
                   height_wo_shoes_ft_in, weight, wingspan_ft_in, on_move_college
            FROM draft_combine_stats
        """,
        "check_quality": lambda rows, cols: check_quality_draft_combine_stats(rows, cols),
        "unique_key": ["player_id", "season"]
    }
}

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

def compute_row_hash(row, columns):
    """Вычисляет хэш строки для сравнения изменений."""
    row_parts = []
    for col in columns:
        value = row.get(col, "")
        # Обрабатываем пустую строку как None
        if value == "":
            value = ""
        else:
            # Нормализация числовых значений
            try:
                float_val = float(value)
                if float_val.is_integer():
                    value = str(int(float_val))  # Приводим 1949.0 к 1949
                else:
                    value = str(float_val)  # Оставляем дробные как есть, например, 180.9
            except (ValueError, TypeError):
                value = str(value) if value is not None else ""
        row_parts.append(value.strip())
    row_str = "".join(row_parts)
    logger.debug(f"Python concat string for {row.get(columns[0])}: {row_str}")
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def check_quality_common_player_info(rows, columns):
    if not rows:
        return "No data returned", 0
    logger.info(f"check_quality_common_player_info: Columns expected: {columns}")
    df = pd.DataFrame(rows, columns=columns)
    missing_counts = {
        'person_id': df['person_id'].isna().sum(),
        'team_id': df['team_id'].isna().sum()
    }
    duplicate_count = len(df) - df['person_id'].nunique()
    format_issues = 0
    business_rule_issues = 0

    for field in ['from_year', 'to_year', 'season_exp']:
        if field in df.columns:
            invalid = pd.to_numeric(df[field], errors='coerce').isna() & df[field].notna()
            format_issues += invalid.sum()

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (person_id): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def check_quality_other_stats(rows, columns):
    if not rows:
        return "No data returned", 0
    logger.info(f"check_quality_other_stats: Columns expected: {columns}")
    df = pd.DataFrame(rows, columns=columns)
    missing_counts = {
        'game_id': df['game_id'].isna().sum(),
        'team_id_home': df['team_id_home'].isna().sum(),
        'team_id_away': df['team_id_away'].isna().sum()
    }
    duplicate_count = len(df.groupby(['game_id', 'team_id_home', 'team_id_away'])) - df[['game_id', 'team_id_home', 'team_id_away']].drop_duplicates().shape[0]
    format_issues = 0
    business_rule_issues = 0

    for field in ['pts_paint_home', 'pts_2nd_chance_home', 'pts_fb_home', 'largest_lead_home',
                  'lead_changes', 'times_tied', 'team_turnovers_home', 'total_turnovers_home',
                  'team_rebounds_home', 'pts_off_to_home', 'pts_paint_away', 'pts_2nd_chance_away',
                  'pts_fb_away', 'largest_lead_away', 'team_turnovers_away', 'total_turnovers_away',
                  'team_rebounds_away', 'pts_off_to_away']:
        if field in df.columns:
            invalid = pd.to_numeric(df[field], errors='coerce').isna() & df[field].notna()
            format_issues += invalid.sum()
            negative = df[field].astype(float, errors='ignore').lt(0).sum()
            business_rule_issues += negative

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (game_id, team_id_home, team_id_away): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def check_quality_player(rows, columns):
    if not rows:
        return "No data returned", 0
    logger.info(f"check_quality_player: Columns expected: {columns}")
    df = pd.DataFrame(rows, columns=columns)
    missing_counts = {'id': df['id'].isna().sum()}
    duplicate_count = len(df) - df['id'].nunique()
    format_issues = pd.to_numeric(df['id'], errors='coerce').isna().sum()
    business_rule_issues = 0

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (id): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def check_quality_team(rows, columns):
    if not rows:
        return "No data returned", 0
    logger.info(f"check_quality_team: Columns expected: {columns}")
    df = pd.DataFrame(rows, columns=columns)
    missing_counts = {'id': df['id'].isna().sum()}
    duplicate_count = len(df) - df['id'].nunique()
    format_issues = pd.to_numeric(df['id'], errors='coerce').isna().sum()
    business_rule_issues = df['year_founded'].astype(float).apply(lambda x: x < 1800 or x > 2023).sum()

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (id): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def check_quality_team_details(rows, columns):
    if not rows:
        return "No data returned", 0
    logger.info(f"check_quality_team_details: Columns expected: {columns}")
    df = pd.DataFrame(rows, columns=columns)
    missing_counts = {'team_id': df['team_id'].isna().sum()}
    duplicate_count = len(df) - df['team_id'].nunique()
    format_issues = pd.to_numeric(df['team_id'], errors='coerce').isna().sum()
    business_rule_issues = df['yearfounded'].astype(float).apply(lambda x: x < 1800 or x > 2023).sum()
    if 'arenacapacity' in df.columns:
        business_rule_issues += df['arenacapacity'].astype(float, errors='ignore').lt(0).sum()

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (team_id): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def check_quality_draft_history(rows, columns):
    if not rows:
        return "No data returned", 0
    logger.info(f"check_quality_draft_history: Columns expected: {columns}")
    df = pd.DataFrame(rows, columns=columns)
    missing_counts = {
        'person_id': df['person_id'].isna().sum(),
        'season': df['season'].isna().sum(),
        'team_id': df['team_id'].isna().sum()
    }
    duplicate_count = len(df) - df[['person_id', 'season']].drop_duplicates().shape[0]
    format_issues = 0
    business_rule_issues = 0

    for field in ['round_number', 'round_pick', 'overall_pick']:
        if field in df.columns:
            invalid = pd.to_numeric(df[field], errors='coerce').isna() & df[field].notna()
            format_issues += invalid.sum()
            negative = df[field].astype(float, errors='ignore').lt(0).sum()
            business_rule_issues += negative

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (person_id, season): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def check_quality_draft_combine_stats(rows, columns):
    if not rows:
        return "No data returned", 0
    logger.info(f"check_quality_draft_combine_stats: Columns expected: {columns}")
    df = pd.DataFrame(rows, columns=columns)
    if 'player_id' not in df.columns:
        comment = "Column 'player_id' is missing in the data"
        logger.error(comment)
        return comment, len(df)

    missing_counts = {
        'player_id': df['player_id'].isna().sum(),
        'season': df['season'].isna().sum()
    }
    duplicate_count = len(df) - df[['player_id', 'season']].drop_duplicates().shape[0]
    format_issues = 0
    business_rule_issues = 0

    if 'weight' in df.columns:
        invalid = pd.to_numeric(df['weight'], errors='coerce').isna() & df['weight'].notna()
        format_issues += invalid.sum()
        if invalid.sum() > 0:
            invalid_values = df['weight'][invalid].unique()
            logger.warning(f"Некорректные значения в weight: {invalid_values}")

        numeric_values = pd.to_numeric(df['weight'], errors='coerce')
        negative = numeric_values[numeric_values < 0].count()
        business_rule_issues += negative

    comment = (
        f"Missing values: {', '.join([f'{k}: {v}' for k, v in missing_counts.items() if v > 0])}; "
        f"Duplicates (player_id, season): {duplicate_count}; "
        f"Format issues: {format_issues}; "
        f"Business rule issues: {business_rule_issues}"
    )
    return comment, len(df)

def load_static_table(table_name, source_path, dag_id, task_id):
    """Синхронизирует данные статических таблиц между SQLite и stage с учетом load_timestamp."""
    start_time = time.time()
    inserted_records = 0
    updated_records = 0
    unchanged_records = 0
    deleted_keys = []
    quality_comment = ""
    config = TABLE_CONFIG[table_name]
    source_type = config['source_type']
    unique_key = config['unique_key']

    try:
        # Подключение к PostgreSQL
        conn = pg_pool.getconn()
        cur = conn.cursor()

        # Подключение к SQLite
        sqlite_conn = sqlite3.connect(source_path)
        sqlite_cur = sqlite_conn.cursor()

        # Извлечение данных из SQLite
        sqlite_cur.execute(config['select_query'])
        rows = sqlite_cur.fetchall()
        if not rows:
            logger.warning(f"Нет данных для таблицы {table_name} в источнике")
            log_to_postgres(
                process_name=dag_id,
                object_name=table_name,
                layer="STAGE",
                source_type=source_type,
                start_time=start_time,
                end_time=time.time(),
                status="SUCCESS",
                records_processed=0,
                comment=quality_comment,
                operation_type="SYNC"
            )
            return

        # Получение имен столбцов из результата запроса
        columns = [desc[0] for desc in sqlite_cur.description]
        logger.info(f"Table {table_name}: Columns from SQLite query: {columns}")

        # Добавление load_timestamp к столбцам
        all_columns = columns + ['load_timestamp']

        # Проверка максимальной длины строк
        sqlite_cur.execute(f"PRAGMA table_info({table_name})")
        valid_columns = [row[1] for row in sqlite_cur.fetchall()]
        query_columns = [col for col in columns if col in valid_columns]
        if not query_columns:
            raise ValueError(f"Нет валидных столбцов для таблицы {table_name}")
        max_length_query = f"SELECT {', '.join([f'MAX(LENGTH(CAST({col} AS TEXT))) AS max_{col}' for col in query_columns])} FROM {table_name}"
        sqlite_cur.execute(max_length_query)
        max_lengths = sqlite_cur.fetchone()
        for field, length in dict(zip([f'max_{col}' for col in query_columns], max_lengths)).items():
            if length and length > 100:
                logger.warning(f"Длина {field} ({length}) превышает VARCHAR(100)")

        # Проверка наличия данных в stage
        cur.execute(f"SELECT COUNT(*) FROM stage.{table_name}")
        stage_count = cur.fetchone()[0]

        # Проверка качества данных
        quality_comment, total_rows = config['check_quality'](rows, columns)

        # Для draft_combine_stats, common_player_info, other_stats: пропустить синхронизацию, если данные уже есть
        if table_name in ['draft_combine_stats', 'common_player_info', 'other_stats'] and stage_count > 0:
            logger.info(f"Таблица {table_name} уже содержит данные ({stage_count} записей), загрузка пропущена")
            log_to_postgres(
                process_name=dag_id,
                object_name=table_name,
                layer="STAGE",
                source_type=source_type,
                start_time=start_time,
                end_time=time.time(),
                status="SUCCESS",
                records_processed=0,
                comment=f"Data already exists ({stage_count} records), loading skipped; {quality_comment}",
                operation_type="SKIPPED"
            )
            return

        # Полная загрузка, если таблица пуста
        if stage_count == 0:
            logger.info(f"Таблица {table_name} пуста, выполняется полная загрузка")
            batch_size = 1000
            batch = []
            current_timestamp = datetime.now()
            for row in rows:
                batch.append(row + (current_timestamp,))
                if len(batch) >= batch_size:
                    query = f"INSERT INTO stage.{table_name} ({', '.join(all_columns)}) VALUES %s"
                    execute_values(cur, query, batch)
                    inserted_records += len(batch)
                    batch = []
            if batch:
                query = f"INSERT INTO stage.{table_name} ({', '.join(all_columns)}) VALUES %s"
                execute_values(cur, query, batch)
                inserted_records += len(batch)

            conn.commit()
            end_time = time.time()
            logger.info(f"Полная загрузка завершена для {table_name}. Всего загружено {inserted_records} записей")

            log_to_postgres(
                process_name=dag_id,
                object_name=table_name,
                layer="STAGE",
                source_type=source_type,
                start_time=start_time,
                end_time=end_time,
                status="SUCCESS",
                records_processed=inserted_records,
                comment=quality_comment,
                operation_type="FULL_LOAD"
            )
            return

        # Синхронизация для остальных таблиц (player, team, team_details, draft_history)
        # Получение столбцов и их типов из stage
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'stage' AND table_name = '{table_name}' AND column_name != 'load_timestamp'
        """)
        stage_columns_info = {row[0]: row[1] for row in cur.fetchall()}
        stage_columns = list(stage_columns_info.keys())

        # Создание временной таблицы с типами столбцов из stage
        temp_table = f"temp_{table_name}_{int(time.time())}"
        column_definitions = []
        for col in stage_columns:
            pg_type = stage_columns_info.get(col, 'TEXT')
            if pg_type.startswith('character') or pg_type == 'text':
                col_type = 'TEXT'
            elif pg_type in ('integer', 'bigint', 'smallint'):
                col_type = 'INTEGER'
            elif pg_type == 'numeric' or pg_type.startswith('decimal'):
                col_type = 'NUMERIC'
            elif pg_type == 'timestamp' or pg_type.startswith('timestamp'):
                col_type = 'TIMESTAMP'
            else:
                col_type = 'TEXT'
            column_definitions.append(f"{col} {col_type}")
        cur.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} (
                {', '.join(column_definitions)},
                load_timestamp TIMESTAMP,
                row_hash TEXT
            )
        """)
        logger.info(f"Создана временная таблица {temp_table} с типами: {column_definitions}")

        # Загрузка данных из SQLite во временную таблицу
        batch_size = 1000
        batch = []
        current_timestamp = datetime.now()
        for row in rows:
            row_dict = dict(zip(columns, row))
            row_hash = compute_row_hash(row_dict, columns)
            batch.append(row + (current_timestamp, row_hash))
            if len(batch) == 1:  # Логируем первую запись для отладки
                row_str = "".join(str(row_dict.get(col, "")).strip() for col in columns)
                logger.info(f"First row in temp_table: {row_dict}, concat_string: {row_str}, row_hash: {row_hash}")
            if len(batch) >= batch_size:
                query = f"INSERT INTO {temp_table} ({', '.join(columns)}, load_timestamp, row_hash) VALUES %s"
                execute_values(cur, query, batch)
                batch = []
        if batch:
            query = f"INSERT INTO {temp_table} ({', '.join(columns)}, load_timestamp, row_hash) VALUES %s"
            execute_values(cur, query, batch)

        # Вычисление хэшей для stage с нормализацией
        hash_columns = []
        for col in columns:
            pg_type = stage_columns_info.get(col, 'TEXT')
            if pg_type in ('integer', 'bigint', 'smallint', 'numeric', 'decimal'):
                # Нормализация чисел: удаляем .0 для целых чисел
                hash_columns.append(
                    f"""
                    CASE
                        WHEN {col} IS NULL THEN ''
                        WHEN FLOOR({col}) = {col} THEN TRIM(CAST(FLOOR({col}) AS TEXT))
                        ELSE TRIM(CAST({col} AS TEXT))
                    END"""
                )
            elif pg_type.startswith('timestamp'):
                # Нормализация дат: формат YYYY-MM-DD
                hash_columns.append(f"COALESCE(TO_CHAR({col}, 'YYYY-MM-DD'), '')")
            else:
                # Текстовые и другие типы: COALESCE для NULL
                hash_columns.append(f"COALESCE(TRIM(CAST({col} AS TEXT)), '')")
        cur.execute(f"""
            ALTER TABLE stage.{table_name} ADD COLUMN IF NOT EXISTS row_hash TEXT;
            UPDATE stage.{table_name}
            SET row_hash = MD5(CONCAT({', '.join(hash_columns)}))
            WHERE row_hash IS NULL;
        """)

        # Логирование первой записи в stage для отладки
        key_conditions = " AND ".join([f"s.{key} = t.{key}" for key in unique_key])
        cur.execute(f"""
            SELECT {', '.join(columns)}, 
                   CONCAT({', '.join(hash_columns)}) AS concat_string,
                   row_hash
            FROM stage.{table_name} s
            WHERE EXISTS (
                SELECT 1 FROM {temp_table} t WHERE {key_conditions}
            )
            LIMIT 1
        """)
        stage_row = cur.fetchone()
        if stage_row:
            stage_dict = dict(zip(columns + ['concat_string', 'row_hash'], stage_row))
            logger.info(f"First row in stage.{table_name}: {stage_dict}")

        # Дополнительное логирование для draft_combine_stats
        if table_name == 'draft_combine_stats':
            cur.execute(f"""
                SELECT t.season, t.player_id, t.weight, s.weight
                FROM {temp_table} t
                JOIN stage.{table_name} s ON {key_conditions}
                WHERE t.row_hash != s.row_hash
                LIMIT 5
            """)
            diff_rows = cur.fetchall()
            if diff_rows:
                logger.info(f"Differences in draft_combine_stats (weight): {diff_rows}")

        # Определение новых записей
        cur.execute(f"""
            INSERT INTO stage.{table_name} ({', '.join(all_columns)})
            SELECT {', '.join(columns)}, clock_timestamp()
            FROM {temp_table} t
            WHERE NOT EXISTS (
                SELECT 1
                FROM stage.{table_name} s
                WHERE {key_conditions}
            )
        """)
        inserted_records = cur.rowcount
        logger.info(f"Добавлено {inserted_records} новых записей в {table_name}")

        # Проверка неизменённых записей
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {temp_table} t
            JOIN stage.{table_name} s ON {key_conditions}
            WHERE t.row_hash = s.row_hash
        """)
        unchanged_records = cur.fetchone()[0]
        logger.info(f"Обнаружено {unchanged_records} записей без изменений в {table_name}")

        # Вставка обновлённых записей (где row_hash отличается)
        cur.execute(f"""
            INSERT INTO stage.{table_name} ({', '.join(all_columns)})
            SELECT {', '.join(columns)}, clock_timestamp()
            FROM {temp_table} t
            WHERE EXISTS (
                SELECT 1
                FROM stage.{table_name} s
                WHERE {key_conditions}
                AND s.row_hash != t.row_hash
            )
        """)
        updated_records = cur.rowcount
        logger.info(f"Добавлено {updated_records} обновлённых записей в {table_name}")

        # Определение удалённых записей (логирование)
        cur.execute(f"""
            SELECT {', '.join(unique_key)}
            FROM stage.{table_name} s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {temp_table} t
                WHERE {key_conditions}
            )
        """)
        deleted_keys = [dict(zip(unique_key, row)) for row in cur.fetchall()]
        deleted_keys_str = "; ".join([", ".join([f"{k}:{v}" for k, v in key.items()]) for key in deleted_keys])
        if len(deleted_keys_str) > 1000:
            deleted_keys_str = deleted_keys_str[:1000] + "... truncated"
        logger.info(f"Обнаружено {len(deleted_keys)} удалённых записей в {table_name}")

        # Удаление временной таблицы и столбца row_hash
        cur.execute(f"DROP TABLE {temp_table}")
        cur.execute(f"ALTER TABLE stage.{table_name} DROP COLUMN IF EXISTS row_hash")

        conn.commit()
        end_time = time.time()
        total_records = inserted_records + updated_records
        status = "SUCCESS"
        quality_comment += f"; Inserted: {inserted_records}, Updated: {updated_records}, Unchanged: {unchanged_records}, Deleted keys: [{deleted_keys_str}]"

        logger.info(f"Синхронизация завершена для {table_name}. Всего обработано {total_records} записей")

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
            operation_type="SYNC"
        )

    except Exception as e:
        end_time = time.time()
        error_message = str(e)
        logger.error(f"Ошибка синхронизации для {table_name}: {error_message}")
        log_to_postgres(
            process_name=dag_id,
            object_name=table_name,
            layer="STAGE",
            source_type=source_type,
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            records_processed=inserted_records + updated_records,
            comment=f"Error: {error_message}; {quality_comment}",
            operation_type="SYNC"
        )
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            pg_pool.putconn(conn)
        if 'sqlite_conn' in locals():
            sqlite_conn.close()