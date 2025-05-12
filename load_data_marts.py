import psycopg2
import logging
import time
from datetime import datetime
import io
from psycopg2.extras import execute_values

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/load_data_marts.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация подключения к БД
DB_CONFIG = {
    "dbname": "nba_stats",
    "user": "admin",
    "password": "admin",
    "host": "postgres_db1",
    "port": "5432"
}

# Конфигурация витрин
DATA_MART_CONFIG = {
    "player_shot_efficiency": {
        "schema": """
            player_id BIGINT NOT NULL,
            full_name VARCHAR(100), 
            season_year INTEGER NOT NULL,
            team_id BIGINT NOT NULL,
            team_name VARCHAR(100),
            position VARCHAR(100),
            basic_zone VARCHAR(100) NOT NULL,
            zone_name VARCHAR(100),
            zone_range VARCHAR(50),
            games_played INTEGER NOT NULL,
            shot_attempts INTEGER NOT NULL,
            shots_made INTEGER NOT NULL,
            zone_fg_percentage DECIMAL(5,2),
            shots_per_game DECIMAL(10,2),
            points_per_zone DECIMAL(10,2),
            load_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT player_shot_efficiency_pkey PRIMARY KEY (player_id, full_name, season_year, basic_zone, zone_name, zone_range)
        """,
        "select_query": """
            SELECT
                player_id,
                full_name,
                season_year,
                team_id,
                team_name,
                position,
                basic_zone,
                zone_name,
                zone_range,
                games_played,
                shot_attempts,
                shots_made,
                zone_fg_percentage,
                shots_per_game,
                points_per_zone,
                CURRENT_TIMESTAMP as load_timestamp
            FROM (
                SELECT
                    fs.player_id,
                    dp.full_name,
                    dt.season_year,
                    fs.team_id,
                    dm.full_name as team_name,
                    dp.position,
                    dz.basic_zone,
                    dz.zone_name,
                    dz.zone_range,
                    COUNT(DISTINCT fs.game_id) AS games_played,
                    COUNT(*) AS shot_attempts,
                    SUM(CASE WHEN fs.shot_made THEN 1 ELSE 0 END) AS shots_made,
                    ROUND(100.0 * SUM(CASE WHEN fs.shot_made THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS zone_fg_percentage,
                    ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT fs.game_id), 0), 2) AS shots_per_game,
                    ROUND(1.0 * SUM(CASE WHEN fs.shot_made THEN CASE WHEN fs.shot_type = '3PT Field Goal' THEN 3 ELSE 2 END ELSE 0 END) / NULLIF(COUNT(DISTINCT fs.game_id), 0), 2) AS points_per_zone,
                    ROW_NUMBER() OVER (PARTITION BY fs.player_id, dt.season_year, dz.basic_zone, dz.zone_name, dz.zone_range ORDER BY dp.full_name) as rn
                FROM dds.fact_shots fs
                JOIN dds.dim_zone dz ON fs.zone_id = dz.zone_id
                JOIN dds.dim_time dt ON fs.time_id = dt.time_id
                JOIN dds.dim_team dm ON fs.team_id = int8(dm.business_key)
                JOIN dds.dim_player dp ON fs.player_id = int8(dp.business_key) AND dp.is_active = TRUE
                GROUP BY fs.player_id, dp.full_name, dt.season_year, fs.team_id, dm.full_name, dp.position, dz.basic_zone, dz.zone_name, dz.zone_range
            ) subquery
            WHERE rn = 1
        """
    },
    "team_attack_efficiency": {
        "schema": """
            team_id BIGINT NOT NULL,
            season_year INTEGER NOT NULL,
            team_name VARCHAR(100) NOT NULL,
            games_played INTEGER NOT NULL,
            total_points INTEGER NOT NULL,
            team_ppg DECIMAL(10,2),
            fgm INTEGER NOT NULL,
            fga INTEGER NOT NULL,
            team_fg_percentage DECIMAL(5,2),
            fg3m INTEGER NOT NULL,
            fg3a INTEGER NOT NULL,
            team_3p_percentage DECIMAL(5,2),
            paint_points INTEGER NOT NULL,
            paint_points_per_game DECIMAL(10,2),
            load_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT team_attack_efficiency_pkey PRIMARY KEY (team_id, season_year)
        """,
        "select_query": """
            SELECT
                fg.team_id,
                dt.season_year,
                dteam.full_name AS team_name,
                COUNT(DISTINCT fg.game_id) AS games_played,
                SUM(fg.pts) AS total_points,
                ROUND(1.0 * SUM(fg.pts) / NULLIF(COUNT(DISTINCT fg.game_id), 0), 2) AS team_ppg,
                SUM(fg.fgm) AS fgm,
                SUM(fg.fga) AS fga,
                ROUND(100.0 * SUM(fg.fgm) / NULLIF(SUM(fg.fga), 0), 2) AS team_fg_percentage,
                SUM(fg.fg3m) AS fg3m,
                SUM(fg.fg3a) AS fg3a,
                ROUND(100.0 * SUM(fg.fg3m) / NULLIF(SUM(fg.fg3a), 0), 2) AS team_3p_percentage,
                SUM(fg.pts_paint) AS paint_points,
                ROUND(1.0 * SUM(fg.pts_paint) / NULLIF(COUNT(DISTINCT fg.game_id), 0), 2) AS paint_points_per_game,
                CURRENT_TIMESTAMP AS load_timestamp
            FROM dds.fact_games fg
            JOIN dds.dim_time dt ON fg.time_id = dt.time_id
            JOIN dds.dim_team dteam ON fg.team_id = int8(dteam.business_key) AND dteam.is_active = TRUE
            GROUP BY fg.team_id, dt.season_year, dteam.full_name
        """
    },
    "three_point_trends": {
        "schema": """
            team_id BIGINT NOT NULL,
            season_year INTEGER NOT NULL,
            team_name VARCHAR(100) NOT NULL,
            games_played INTEGER NOT NULL,
            fg3a INTEGER NOT NULL,
            team_3pa_per_game DECIMAL(10,2),
            fg3m INTEGER NOT NULL,
            team_3p_percentage DECIMAL(5,2),
            three_point_points INTEGER NOT NULL,
            total_points INTEGER NOT NULL,
            three_point_contribution DECIMAL(5,2),
            load_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT three_point_trends_pkey PRIMARY KEY (team_id, season_year)
        """,
        "select_query": """
            SELECT
                fg.team_id,
                dt.season_year,
                dteam.full_name AS team_name,
                COUNT(DISTINCT fg.game_id) AS games_played,
                SUM(fg.fg3a) AS fg3a,
                ROUND(1.0 * SUM(fg.fg3a) / NULLIF(COUNT(DISTINCT fg.game_id), 0), 2) AS team_3pa_per_game,
                SUM(fg.fg3m) AS fg3m,
                ROUND(100.0 * SUM(fg.fg3m) / NULLIF(SUM(fg.fg3a), 0), 2) AS team_3p_percentage,
                SUM(fg.fg3m) * 3 AS three_point_points,
                SUM(fg.pts) AS total_points,
                ROUND(100.0 * (SUM(fg.fg3m) * 3) / NULLIF(SUM(fg.pts), 0), 2) AS three_point_contribution,
                CURRENT_TIMESTAMP AS load_timestamp
            FROM dds.fact_games fg
            JOIN dds.dim_time dt ON fg.time_id = dt.time_id
            JOIN dds.dim_team dteam ON fg.team_id = int8(dteam.business_key) AND dteam.is_active = TRUE
            GROUP BY fg.team_id, dt.season_year, dteam.full_name
        """
    }
}

def ensure_table_schema(table_name, schema):
    """Проверяет и создаёт таблицу витрины с заданной схемой."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Проверяем существование таблицы
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'dm' AND table_name = %s
            )
        """, (table_name,))
        table_exists = cur.fetchone()[0]

        if not table_exists:
            logger.info(f"Создаём таблицу dm.{table_name}")
            cur.execute(f"""
                CREATE TABLE dm.{table_name} ({schema})
            """)
        else:
            logger.info(f"Таблица dm.{table_name} уже существует, пропускаем создание")

        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при проверке/создании таблицы {table_name}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def log_to_postgres(process_name, object_name, layer, source_type, start_time, end_time, status, records_processed, comment):
    """Записывает информацию о загрузке в tech.load_logs."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tech.load_logs (
                process_name, object_name, layer, source_type, start_time, end_time,
                status, records_processed, comment
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            process_name, object_name, layer, source_type,
            datetime.fromtimestamp(start_time),
            datetime.fromtimestamp(end_time) if end_time else None,
            status, records_processed, comment
        ))
        conn.commit()
        logger.info(f"Лог записан в tech.load_logs: {object_name}, {status}")
    except psycopg2.Error as e:
        logger.error(f"Ошибка записи лога: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def load_data_mart(table_name, dag_id, task_id):
    """Загружает данные в витрину."""
    start_time = time.time()
    inserted_records = 0
    config = DATA_MART_CONFIG[table_name]
    select_query = config['select_query']
    schema = config['schema']

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Проверяем и создаём таблицу
        ensure_table_schema(table_name, schema)

        # Проверяем данные
        logger.info(f"Executing select_query for {table_name}: {select_query}")
        cur.execute(f"SELECT COUNT(*) FROM ({select_query}) AS subquery")
        data_count = cur.fetchone()[0]
        logger.info(f"Found {data_count} records for {table_name} before loading")

        if data_count == 0:
            logger.info(f"Нет данных для {table_name}, пропускаем загрузку")
            log_to_postgres(
                process_name=dag_id,
                object_name=table_name,
                layer="DATA_MART",
                source_type="DDS",
                start_time=start_time,
                end_time=time.time(),
                status="SUCCESS",
                records_processed=0,
                comment="No data"
            )
            return

        # Очищаем таблицу
        logger.info(f"Очищаем таблицу dm.{table_name}")
        cur.execute(f"TRUNCATE TABLE dm.{table_name}")

        # Выполняем вставку данных
        cur.execute(select_query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        # Логируем первые 10 строк для диагностики
        logger.info(f"Data to be inserted into dm.{table_name}: {rows[:10]}")

        # Вставка с помощью execute_values
        insert_query = f"""
            INSERT INTO dm.{table_name} ({', '.join(columns)})
            VALUES %s
        """
        execute_values(cur, insert_query, rows)
        inserted_records = cur.rowcount
        logger.info(f"Вставлены {inserted_records} записи в dm.{table_name}")

        conn.commit()
        status = "SUCCESS" if inserted_records > 0 else "FAILED"
        comment = f"Inserted: {inserted_records}"

        log_to_postgres(
            process_name=dag_id,
            object_name=table_name,
            layer="DATA_MART",
            source_type="DDS",
            start_time=start_time,
            end_time=time.time(),
            status=status,
            records_processed=inserted_records,
            comment=comment
        )

    except Exception as e:
        logger.error(f"Ошибка загрузки для {table_name}: {e}")
        log_to_postgres(
            process_name=dag_id,
            object_name=table_name,
            layer="DATA_MART",
            source_type="DDS",
            start_time=start_time,
            end_time=time.time(),
            status="FAILED",
            records_processed=inserted_records,
            comment=f"Error: {str(e)}"
        )
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()






# import psycopg2
# import logging
# import time
# from datetime import datetime
# import io
# from psycopg2.extras import execute_values
#
# # Настройка логирования
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('/opt/airflow/logs/load_data_marts.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)
#
# # Конфигурация подключения к БД
# DB_CONFIG = {
#     "dbname": "nba_stats",
#     "user": "admin",
#     "password": "admin",
#     "host": "postgres_db1",
#     "port": "5432"
# }
#
# # Конфигурация витрин
# DATA_MART_CONFIG = {
#     "player_shot_efficiency": {
#         "schema": """
#             player_id BIGINT NOT NULL,
#             season_year INTEGER NOT NULL,
#             team_id BIGINT NOT NULL,
#             position VARCHAR(100),
#             basic_zone VARCHAR(100) NOT NULL,
#             zone_name VARCHAR(100),
#             zone_range VARCHAR(50),
#             games_played INTEGER NOT NULL,
#             shot_attempts INTEGER NOT NULL,
#             shots_made INTEGER NOT NULL,
#             zone_fg_percentage DECIMAL(5,2),
#             shots_per_game DECIMAL(10,2),
#             points_per_zone DECIMAL(10,2),
#             load_timestamp TIMESTAMP NOT NULL,
#             CONSTRAINT player_shot_efficiency_pkey PRIMARY KEY (player_id, season_year, basic_zone, zone_name, zone_range)
#         """,
#         "select_query": """
#             SELECT
#                 fs.player_id,
#                 dt.season_year,
#                 fs.team_id,
#                 dp.position,
#                 dz.basic_zone,
#                 dz.zone_name,
#                 dz.zone_range,
#                 COUNT(DISTINCT fs.game_id) AS games_played,
#                 COUNT(*) AS shot_attempts,
#                 SUM(CASE WHEN fs.shot_made THEN 1 ELSE 0 END) AS shots_made,
#                 ROUND(100.0 * SUM(CASE WHEN fs.shot_made THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS zone_fg_percentage,
#                 ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT fs.game_id), 0), 2) AS shots_per_game,
#                 ROUND(1.0 * SUM(CASE WHEN fs.shot_made THEN CASE WHEN fs.shot_type = '3PT Field Goal' THEN 3 ELSE 2 END ELSE 0 END) / NULLIF(COUNT(DISTINCT fs.game_id), 0), 2) AS points_per_zone,
#                 CURRENT_TIMESTAMP AS load_timestamp
#             FROM dds.fact_shots fs
#             JOIN dds.dim_zone dz ON fs.zone_id = dz.zone_id
#             JOIN dds.dim_time dt ON fs.time_id = dt.time_id
#             JOIN dds.dim_player dp ON fs.player_id = dp.player_id AND dp.is_active = TRUE
#             GROUP BY fs.player_id, dt.season_year, fs.team_id, dp.position, dz.basic_zone, dz.zone_name, dz.zone_range
#         """
#     },
#     "team_attack_efficiency": {
#         "schema": """
#             team_id BIGINT NOT NULL,
#             season_year INTEGER NOT NULL,
#             team_name VARCHAR(100) NOT NULL,
#             games_played INTEGER NOT NULL,
#             total_points INTEGER NOT NULL,
#             team_ppg DECIMAL(10,2),
#             fgm INTEGER NOT NULL,
#             fga INTEGER NOT NULL,
#             team_fg_percentage DECIMAL(5,2),
#             fg3m INTEGER NOT NULL,
#             fg3a INTEGER NOT NULL,
#             team_3p_percentage DECIMAL(5,2),
#             paint_points INTEGER NOT NULL,
#             paint_points_per_game DECIMAL(10,2),
#             load_timestamp TIMESTAMP NOT NULL,
#             CONSTRAINT team_attack_efficiency_pkey PRIMARY KEY (team_id, season_year)
#         """,
#         "select_query": """
#             SELECT
#                 fg.team_id,
#                 dt.season_year,
#                 dt.full_name AS team_name,
#                 COUNT(DISTINCT fg.game_id) AS games_played,
#                 SUM(fg.pts) AS total_points,
#                 ROUND(1.0 * SUM(fg.pts) / NULLIF(COUNT(DISTINCT fg.game_id), 0), 2) AS team_ppg,
#                 SUM(fg.fgm) AS fgm,
#                 SUM(fg.fga) AS fga,
#                 ROUND(100.0 * SUM(fg.fgm) / NULLIF(SUM(fg.fga), 0), 2) AS team_fg_percentage,
#                 SUM(fg.fg3m) AS fg3m,
#                 SUM(fg.fg3a) AS fg3a,
#                 ROUND(100.0 * SUM(fg.fg3m) / NULLIF(SUM(fg.fg3a), 0), 2) AS team_3p_percentage,
#                 SUM(fg.pts_paint) AS paint_points,
#                 ROUND(1.0 * SUM(fg.pts_paint) / NULLIF(COUNT(DISTINCT fg.game_id), 0), 2) AS paint_points_per_game,
#                 CURRENT_TIMESTAMP AS load_timestamp
#             FROM dds.fact_games fg
#             JOIN dds.dim_time dt ON fg.time_id = dt.time_id
#             JOIN dds.dim_team dt ON fg.team_id = dt.team_id AND dt.is_active = TRUE
#             GROUP BY fg.team_id, dt.season_year, dt.full_name
#         """
#     },
#     "three_point_trends": {
#         "schema": """
#             team_id BIGINT NOT NULL,
#             season_year INTEGER NOT NULL,
#             team_name VARCHAR(100) NOT NULL,
#             games_played INTEGER NOT NULL,
#             fg3a INTEGER NOT NULL,
#             team_3pa_per_game DECIMAL(10,2),
#             fg3m INTEGER NOT NULL,
#             team_3p_percentage DECIMAL(5,2),
#             three_point_points INTEGER NOT NULL,
#             total_points INTEGER NOT NULL,
#             three_point_contribution DECIMAL(5,2),
#             load_timestamp TIMESTAMP NOT NULL,
#             CONSTRAINT three_point_trends_pkey PRIMARY KEY (team_id, season_year)
#         """,
#         "select_query": """
#             SELECT
#                 fg.team_id,
#                 dt.season_year,
#                 dt.full_name AS team_name,
#                 COUNT(DISTINCT fg.game_id) AS games_played,
#                 SUM(fg.fg3a) AS fg3a,
#                 ROUND(1.0 * SUM(fg.fg3a) / NULLIF(COUNT(DISTINCT fg.game_id), 0), 2) AS team_3pa_per_game,
#                 SUM(fg.fg3m) AS fg3m,
#                 ROUND(100.0 * SUM(fg.fg3m) / NULLIF(SUM(fg.fg3a), 0), 2) AS team_3p_percentage,
#                 SUM(fg.fg3m) * 3 AS three_point_points,
#                 SUM(fg.pts) AS total_points,
#                 ROUND(100.0 * (SUM(fg.fg3m) * 3) / NULLIF(SUM(fg.pts), 0), 2) AS three_point_contribution,
#                 CURRENT_TIMESTAMP AS load_timestamp
#             FROM dds.fact_games fg
#             JOIN dds.dim_time dt ON fg.time_id = dt.time_id
#             JOIN dds.dim_team dt ON fg.team_id = dt.team_id AND dt.is_active = TRUE
#             GROUP BY fg.team_id, dt.season_year, dt.full_name
#         """
#     }
# }
#
# def ensure_table_schema(table_name, schema):
#     """Проверяет и создаёт таблицу витрины с заданной схемой."""
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()
#
#         # Проверяем существование таблицы
#         cur.execute("""
#             SELECT EXISTS (
#                 SELECT FROM information_schema.tables
#                 WHERE table_schema = 'dm' AND table_name = %s
#             )
#         """, (table_name,))
#         table_exists = cur.fetchone()[0]
#
#         if not table_exists:
#             logger.info(f"Создаём таблицу dm.{table_name}")
#             cur.execute(f"""
#                 CREATE TABLE dm.{table_name} ({schema})
#             """)
#         else:
#             logger.info(f"Таблица dm.{table_name} уже существует, пропускаем создание")
#
#         conn.commit()
#     except psycopg2.Error as e:
#         logger.error(f"Ошибка при проверке/создании таблицы {table_name}: {e}")
#         raise
#     finally:
#         cur.close()
#         conn.close()
#
# def log_to_postgres(process_name, object_name, layer, source_type, start_time, end_time, status, records_processed, comment):
#     """Записывает информацию о загрузке в tech.load_logs."""
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()
#         cur.execute("""
#             INSERT INTO tech.load_logs (
#                 process_name, object_name, layer, source_type, start_time, end_time,
#                 status, records_processed, comment
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (
#             process_name, object_name, layer, source_type,
#             datetime.fromtimestamp(start_time),
#             datetime.fromtimestamp(end_time) if end_time else None,
#             status, records_processed, comment
#         ))
#         conn.commit()
#         logger.info(f"Лог записан в tech.load_logs: {object_name}, {status}")
#     except psycopg2.Error as e:
#         logger.error(f"Ошибка записи лога: {e}")
#         raise
#     finally:
#         cur.close()
#         conn.close()
#
# def load_data_mart(table_name, dag_id, task_id):
#     """Загружает данные в витрину."""
#     start_time = time.time()
#     inserted_records = 0
#     config = DATA_MART_CONFIG[table_name]
#     select_query = config['select_query']
#     schema = config['schema']
#
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         cur = conn.cursor()
#
#         # Проверяем и создаём таблицу
#         ensure_table_schema(table_name, schema)
#
#         # Проверяем данные
#         logger.info(f"Executing select_query for {table_name}: {select_query}")
#         cur.execute(f"SELECT COUNT(*) FROM ({select_query}) AS subquery")
#         data_count = cur.fetchone()[0]
#         logger.info(f"Found {data_count} records for {table_name} before loading")
#
#         if data_count == 0:
#             logger.info(f"Нет данных для {table_name}, пропускаем загрузку")
#             log_to_postgres(
#                 process_name=dag_id,
#                 object_name=table_name,
#                 layer="DATA_MART",
#                 source_type="DDS",
#                 start_time=start_time,
#                 end_time=time.time(),
#                 status="SUCCESS",
#                 records_processed=0,
#                 comment="No data"
#             )
#             return
#
#         # Очищаем таблицу
#         logger.info(f"Очищаем таблицу dm.{table_name}")
#         cur.execute(f"TRUNCATE TABLE dm.{table_name}")
#
#         # Выполняем вставку данных
#         cur.execute(select_query)
#         rows = cur.fetchall()
#         columns = [desc[0] for desc in cur.description]
#
#         # Логируем первые 10 строк для диагностики
#         logger.info(f"Data to be inserted into dm.{table_name}: {rows[:10]}")
#
#         # Вставка с помощью execute_values
#         insert_query = f"""
#             INSERT INTO dm.{table_name} ({', '.join(columns)})
#             VALUES %s
#         """
#         execute_values(cur, insert_query, rows)
#         inserted_records = cur.rowcount
#         logger.info(f"Вставлены {inserted_records} записи в dm.{table_name}")
#
#         conn.commit()
#         status = "SUCCESS" if inserted_records > 0 else "FAILED"
#         comment = f"Inserted: {inserted_records}"
#
#         log_to_postgres(
#             process_name=dag_id,
#             object_name=table_name,
#             layer="DATA_MART",
#             source_type="DDS",
#             start_time=start_time,
#             end_time=time.time(),
#             status=status,
#             records_processed=inserted_records,
#             comment=comment
#         )
#
#     except Exception as e:
#         logger.error(f"Ошибка загрузки для {table_name}: {e}")
#         log_to_postgres(
#             process_name=dag_id,
#             object_name=table_name,
#             layer="DATA_MART",
#             source_type="DDS",
#             start_time=start_time,
#             end_time=time.time(),
#             status="FAILED",
#             records_processed=inserted_records,
#             comment=f"Error: {str(e)}"
#         )
#         raise
#     finally:
#         if 'cur' in locals():
#             cur.close()
#         if 'conn' in locals():
#             conn.close()