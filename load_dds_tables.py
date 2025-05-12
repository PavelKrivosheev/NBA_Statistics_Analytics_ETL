import psycopg2
import logging
import time
from datetime import datetime
from psycopg2.extras import execute_values

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/load_dds_tables.log'),
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

# Конфигурация таблиц
TABLE_CONFIG = {
    "dim_time": {
        "stage_table": "shot_data",
        "primary_keys": ["season_year"],
        "attributes": ["season_name"],
        "select_query": """
            WITH max_timestamp AS (
                SELECT season_year, COALESCE(MAX(load_timestamp), '1970-01-01'::TIMESTAMP) AS max_ts
                FROM dds.dim_time
                GROUP BY season_year
            ),
            combined AS (
                SELECT CAST(season_1 AS INTEGER) AS season_year,
                       CAST(season_1 AS TEXT) || '-' || CAST(CAST(season_1 AS INTEGER) + 1 AS TEXT) AS season_name,
                       load_timestamp
                FROM stage.shot_data
                WHERE season_1 IS NOT NULL
                UNION ALL
                SELECT CAST(SUBSTR(season_id, 2) AS INTEGER) AS season_year,
                       SUBSTR(season_id, 2) || '-' || CAST(CAST(SUBSTR(season_id, 2) AS INTEGER) + 1 AS TEXT) AS season_name,
                       load_timestamp
                FROM stage.game
                WHERE season_id IS NOT NULL
            ),
            ranked AS (
                SELECT season_year, season_name, load_timestamp,
                       ROW_NUMBER() OVER (PARTITION BY season_year ORDER BY load_timestamp DESC) AS rn
                FROM combined
                WHERE season_year NOT IN (SELECT season_year FROM max_timestamp)
                   OR load_timestamp > (SELECT max_ts FROM max_timestamp WHERE season_year = combined.season_year)
            )
            SELECT season_year, season_name, load_timestamp
            FROM ranked
            WHERE rn = 1
        """,
        "operation_type": "UPSERT",
        "schema": """
            time_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            season_year INTEGER NOT NULL,
            season_name VARCHAR(10) NOT NULL,
            load_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT dim_time_season_year_key UNIQUE (season_year)
        """,
        "unique_keys": ["season_year"],
        "column_types": {
            "season_year": "INTEGER",
            "season_name": "VARCHAR(10)",
            "load_timestamp": "TIMESTAMP"
        }
    },
    "dim_zone": {
        "stage_table": "shot_data",
        "primary_keys": ["basic_zone", "zone_name", "zone_range"],
        "attributes": ["zone_abb"],
        "select_query": """
            WITH max_timestamp AS (
                SELECT basic_zone, zone_name, zone_range, COALESCE(MAX(load_timestamp), '1970-01-01'::TIMESTAMP) AS max_ts
                FROM dds.dim_zone
                GROUP BY basic_zone, zone_name, zone_range
            ),
            ranked AS (
                SELECT basic_zone, zone_name, zone_range, zone_abb, load_timestamp,
                       ROW_NUMBER() OVER (PARTITION BY basic_zone, zone_name, zone_range ORDER BY load_timestamp DESC) AS rn
                FROM stage.shot_data
                WHERE basic_zone IS NOT NULL
                AND ((basic_zone, zone_name, zone_range) NOT IN (SELECT basic_zone, zone_name, zone_range FROM max_timestamp)
                     OR load_timestamp > (SELECT max_ts FROM max_timestamp 
                                          WHERE basic_zone = stage.shot_data.basic_zone 
                                          AND zone_name = stage.shot_data.zone_name 
                                          AND zone_range = stage.shot_data.zone_range))
            )
            SELECT basic_zone, zone_name, zone_range, zone_abb, load_timestamp
            FROM ranked
            WHERE rn = 1
        """,
        "operation_type": "UPSERT",
        "schema": """
            zone_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            basic_zone VARCHAR(100) NOT NULL,
            zone_name VARCHAR(100),
            zone_range VARCHAR(50),
            zone_abb VARCHAR(50),
            load_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT dim_zone_basic_zone_zone_name_zone_range_key UNIQUE (basic_zone, zone_name, zone_range)
        """,
        "unique_keys": ["basic_zone", "zone_name", "zone_range"],
        "column_types": {
            "basic_zone": "VARCHAR(100)",
            "zone_name": "VARCHAR(100)",
            "zone_range": "VARCHAR(50)",
            "zone_abb": "VARCHAR(50)",
            "load_timestamp": "TIMESTAMP"
        }
    },
    "dim_team": {
        "stage_table": "team",
        "primary_keys": ["business_key"],
        "attributes": ["full_name", "abbreviation", "city", "arena", "conference"],
        "select_query": """
            WITH max_timestamp AS (
                SELECT business_key, COALESCE(MAX(load_timestamp), '1970-01-01'::TIMESTAMP) AS max_ts
                FROM dds.dim_team
                GROUP BY business_key
            ),
            ranked AS (
                SELECT t.id AS business_key, t.full_name, t.abbreviation, t.city,
                       td.arena, 'Unknown' AS conference, t.load_timestamp,
                       ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY t.load_timestamp DESC) AS rn
                FROM stage.team t
                LEFT JOIN stage.team_details td ON t.id = td.team_id
                WHERE t.id IS NOT NULL
                AND (CAST(t.id AS VARCHAR) NOT IN (SELECT business_key FROM max_timestamp)
                     OR t.load_timestamp > (SELECT max_ts FROM max_timestamp WHERE business_key = CAST(t.id AS VARCHAR)))
            )
            SELECT business_key, full_name, abbreviation, city, arena, conference, load_timestamp
            FROM ranked
            WHERE rn = 1
        """,
        "operation_type": "SCD2",
        "schema": """
            team_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            business_key VARCHAR(100) NOT NULL,
            full_name VARCHAR(100),
            abbreviation VARCHAR(100),
            city VARCHAR(100),
            arena VARCHAR(100),
            conference VARCHAR(50),
            load_timestamp TIMESTAMP NOT NULL,
            valid_from TIMESTAMP NOT NULL,
            valid_to TIMESTAMP,
            is_active BOOLEAN NOT NULL
        """,
        "unique_keys": ["business_key"],
        "column_types": {
            "business_key": "VARCHAR(100)",
            "full_name": "VARCHAR(100)",
            "abbreviation": "VARCHAR(100)",
            "city": "VARCHAR(100)",
            "arena": "VARCHAR(100)",
            "conference": "VARCHAR(50)",
            "load_timestamp": "TIMESTAMP"
        }
    },
    "dim_player": {
        "stage_table": "common_player_info",
        "primary_keys": ["business_key"],
        "attributes": ["full_name", "first_name", "last_name", "position", "team_id"],
        "select_query": """
            WITH max_timestamp AS (
                SELECT business_key, COALESCE(MAX(load_timestamp), '1970-01-01'::TIMESTAMP) AS max_ts
                FROM dds.dim_player
                GROUP BY business_key
            ),
            ranked AS (
                SELECT person_id AS business_key, first_name || ' ' || last_name AS full_name,
                       first_name, last_name, position,
                       CASE WHEN team_id = '0' THEN NULL ELSE team_id END AS team_id,
                       load_timestamp,
                       ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY load_timestamp DESC) AS rn
                FROM stage.common_player_info
                WHERE person_id IS NOT NULL
                AND (CAST(person_id AS VARCHAR) NOT IN (SELECT business_key FROM max_timestamp)
                     OR load_timestamp > (SELECT max_ts FROM max_timestamp WHERE business_key = CAST(person_id AS VARCHAR)))
            )
            SELECT business_key, full_name, first_name, last_name, position, team_id, load_timestamp
            FROM ranked
            WHERE rn = 1
        """,
        "operation_type": "SCD2",
        "schema": """
            player_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            business_key VARCHAR(50) NOT NULL,
            full_name VARCHAR(100),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            position VARCHAR(100),
            team_id BIGINT,
            load_timestamp TIMESTAMP NOT NULL,
            valid_from TIMESTAMP NOT NULL,
            valid_to TIMESTAMP,
            is_active BOOLEAN NOT NULL
        """,
        "unique_keys": ["business_key"],
        "column_types": {
            "business_key": "VARCHAR(50)",
            "full_name": "VARCHAR(100)",
            "first_name": "VARCHAR(100)",
            "last_name": "VARCHAR(100)",
            "position": "VARCHAR(100)",
            "team_id": "BIGINT",
            "load_timestamp": "TIMESTAMP"
        }
    },
    "dim_game": {
        "stage_table": "game",
        "primary_keys": ["game_id"],
        "attributes": ["game_date", "season_id", "season_year", "home_team_id", "away_team_id", "matchup"],
        "select_query": """
            WITH max_timestamp AS (
                SELECT game_id, COALESCE(MAX(load_timestamp), '1970-01-01'::TIMESTAMP) AS max_ts
                FROM dds.dim_game
                GROUP BY game_id
            ),
            ranked AS (
                SELECT CAST(game_id AS BIGINT) AS game_id,
                       game_date::DATE AS game_date,
                       season_id,
                       CAST(SUBSTR(season_id, 2) AS INTEGER) AS season_year,
                       CAST(team_id_home AS BIGINT) AS home_team_id,
                       CAST(team_id_away AS BIGINT) AS away_team_id,
                       matchup_home AS matchup,
                       load_timestamp,
                       ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY load_timestamp DESC) AS rn
                FROM stage.game
                WHERE game_id IS NOT NULL
                AND team_id_home IS NOT NULL
                AND team_id_away IS NOT NULL
                AND game_id ~ '^[0-9]+$'
                AND team_id_home ~ '^[0-9]+$'
                AND team_id_away ~ '^[0-9]+$'
                AND (CAST(game_id AS BIGINT) NOT IN (SELECT game_id FROM max_timestamp)
                     OR load_timestamp > (SELECT max_ts FROM max_timestamp WHERE game_id = CAST(stage.game.game_id AS BIGINT)))
            )
            SELECT game_id, game_date, season_id, season_year, home_team_id, away_team_id, matchup, load_timestamp
            FROM ranked
            WHERE rn = 1
        """,
        "operation_type": "UPSERT",
        "schema": """
            game_id BIGINT PRIMARY KEY,
            game_date DATE NOT NULL,
            season_id VARCHAR(100) NOT NULL,
            season_year INTEGER NOT NULL,
            home_team_id BIGINT NOT NULL,
            away_team_id BIGINT NOT NULL,
            matchup VARCHAR(100),
            load_timestamp TIMESTAMP NOT NULL
        """,
        "unique_keys": ["game_id"],
        "column_types": {
            "game_id": "BIGINT",
            "game_date": "DATE",
            "season_id": "VARCHAR(100)",
            "season_year": "INTEGER",
            "home_team_id": "BIGINT",
            "away_team_id": "BIGINT",
            "matchup": "VARCHAR(100)",
            "load_timestamp": "TIMESTAMP"
        }
    },
    "fact_shots": {
        "stage_table": "shot_data",
        "primary_keys": ["game_id", "player_id", "quarter", "mins_left", "secs_left"],
        "attributes": ["team_id", "zone_id", "time_id", "shot_made", "shot_type", "loc_x", "loc_y", "shot_distance"],
        "select_query": """
            WITH new_data AS (
                SELECT 
                    CAST(s.game_id AS BIGINT) AS game_id,
                    CAST(s.player_id AS BIGINT) AS player_id,
                    s.quarter,
                    s.mins_left,
                    s.secs_left,
                    CAST(CASE WHEN s.team_id = '0' THEN NULL ELSE s.team_id END AS BIGINT) AS team_id,
                    dz.zone_id,
                    dt.time_id,
                    s.shot_made,
                    s.shot_type,
                    s.loc_x,
                    s.loc_y,
                    s.shot_distance,
                    s.load_timestamp,
                    ROW_NUMBER() OVER (PARTITION BY s.game_id, s.player_id, s.quarter, s.mins_left, s.secs_left 
                                       ORDER BY s.load_timestamp DESC) AS rn
                FROM stage.shot_data s
                JOIN dds.dim_zone dz ON s.basic_zone = dz.basic_zone 
                                    AND s.zone_name = dz.zone_name 
                                    AND s.zone_range = dz.zone_range
                JOIN dds.dim_time dt ON CAST(s.season_1 AS INTEGER) = dt.season_year
                WHERE s.game_id IS NOT NULL
                  AND s.player_id IS NOT NULL
                  AND s.team_id IS NOT NULL
                  AND s.game_id ~ '^[0-9]+$'
                  AND s.player_id ~ '^[0-9]+$'
                  AND s.team_id ~ '^[0-9]+$'
            ),
            max_timestamp AS (
                SELECT game_id, player_id, quarter, mins_left, secs_left, 
                       MAX(load_timestamp) AS max_ts
                FROM dds.fact_shots
                GROUP BY game_id, player_id, quarter, mins_left, secs_left
            )
            SELECT 
                n.game_id, 
                n.player_id, 
                n.quarter, 
                n.mins_left, 
                n.secs_left, 
                n.team_id, 
                n.zone_id, 
                n.time_id, 
                n.shot_made, 
                n.shot_type, 
                n.loc_x, 
                n.loc_y, 
                n.shot_distance, 
                n.load_timestamp
            FROM new_data n
            LEFT JOIN max_timestamp mt 
                ON n.game_id = mt.game_id 
                AND n.player_id = mt.player_id 
                AND n.quarter = mt.quarter 
                AND n.mins_left = mt.mins_left 
                AND n.secs_left = mt.secs_left
            WHERE n.rn = 1
              AND (mt.max_ts IS NULL OR n.load_timestamp > mt.max_ts)
        """,
        "operation_type": "UPSERT",
        "schema": """
            shot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            player_id BIGINT NOT NULL,
            team_id BIGINT NOT NULL,
            game_id BIGINT NOT NULL,
            zone_id BIGINT NOT NULL,
            time_id BIGINT NOT NULL,
            shot_made BOOLEAN NOT NULL,
            shot_type VARCHAR(50) NOT NULL,
            loc_x DOUBLE PRECISION,
            loc_y DOUBLE PRECISION,
            shot_distance INTEGER,
            quarter INTEGER,
            mins_left INTEGER,
            secs_left INTEGER,
            load_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT fact_shots_unique_key UNIQUE (game_id, player_id, quarter, mins_left, secs_left)
        """,
        "unique_keys": ["game_id", "player_id", "quarter", "mins_left", "secs_left"],
        "column_types": {
            "game_id": "BIGINT",
            "player_id": "BIGINT",
            "quarter": "INTEGER",
            "mins_left": "INTEGER",
            "secs_left": "INTEGER",
            "team_id": "BIGINT",
            "zone_id": "BIGINT",
            "time_id": "BIGINT",
            "shot_made": "BOOLEAN",
            "shot_type": "VARCHAR(50)",
            "loc_x": "DOUBLE PRECISION",
            "loc_y": "DOUBLE PRECISION",
            "shot_distance": "INTEGER",
            "load_timestamp": "TIMESTAMP"
        }
    },
    "fact_games": {
        "stage_table": "game",
        "primary_keys": ["game_id", "team_id"],
        "attributes": ["time_id", "is_home", "pts", "fgm", "fga", "fg3m", "fg3a", "pts_paint"],
        "select_query": """
            WITH max_timestamp AS (
                SELECT game_id, team_id, COALESCE(MAX(load_timestamp), '1970-01-01'::TIMESTAMP) AS max_ts
                FROM dds.fact_games
                GROUP BY game_id, team_id
            ),
            ranked AS (
                SELECT CAST(g.game_id AS BIGINT) AS game_id,
                       CAST(t.team_id AS BIGINT) AS team_id,
                       dt.time_id,
                       t.is_home,
                       CASE WHEN t.is_home THEN g.pts_home ELSE g.pts_away END AS pts,
                       CASE WHEN t.is_home THEN g.fgm_home ELSE g.fgm_away END AS fgm,
                       CASE WHEN t.is_home THEN g.fga_home ELSE g.fga_away END AS fga,
                       CASE WHEN t.is_home THEN g.fg3m_home ELSE g.fg3m_away END AS fg3m,
                       CASE WHEN t.is_home THEN g.fg3a_home ELSE g.fg3a_away END AS fg3a,
                       CASE WHEN t.is_home THEN os.pts_paint_home ELSE os.pts_paint_away END AS pts_paint,
                       g.load_timestamp,
                       ROW_NUMBER() OVER (PARTITION BY g.game_id, t.team_id ORDER BY g.load_timestamp DESC) AS rn
                FROM stage.game g
                LEFT JOIN stage.other_stats os ON g.game_id = os.game_id
                JOIN dds.dim_time dt ON CAST(SUBSTR(g.season_id, 2) AS INTEGER) = dt.season_year
                CROSS JOIN LATERAL (
                    VALUES
                        (g.team_id_home, TRUE),
                        (g.team_id_away, FALSE)
                ) AS t(team_id, is_home)
                WHERE g.game_id IS NOT NULL
                AND t.team_id IS NOT NULL
                AND g.game_id ~ '^[0-9]+$'
                AND t.team_id ~ '^[0-9]+$'
                AND ((CAST(g.game_id AS BIGINT), CAST(t.team_id AS BIGINT)) NOT IN (
                    SELECT game_id, team_id FROM max_timestamp
                ) OR g.load_timestamp > (SELECT max_ts FROM max_timestamp 
                                         WHERE game_id = CAST(g.game_id AS BIGINT) 
                                         AND team_id = CAST(t.team_id AS BIGINT)))
            )
            SELECT game_id, team_id, time_id, is_home, pts, fgm, fga, fg3m, fg3a, pts_paint, load_timestamp
            FROM ranked
            WHERE rn = 1
        """,
        "operation_type": "UPSERT",
        "schema": """
            game_id BIGINT NOT NULL,
            team_id BIGINT NOT NULL,
            time_id BIGINT NOT NULL,
            is_home BOOLEAN NOT NULL,
            pts INTEGER,
            fgm INTEGER,
            fga INTEGER,
            fg3m INTEGER,
            fg3a INTEGER,
            pts_paint INTEGER,
            load_timestamp TIMESTAMP NOT NULL,
            PRIMARY KEY (game_id, team_id)
        """,
        "unique_keys": ["game_id", "team_id"],
        "column_types": {
            "game_id": "BIGINT",
            "team_id": "BIGINT",
            "time_id": "BIGINT",
            "is_home": "BOOLEAN",
            "pts": "INTEGER",
            "fgm": "INTEGER",
            "fga": "INTEGER",
            "fg3m": "INTEGER",
            "fg3a": "INTEGER",
            "pts_paint": "INTEGER",
            "load_timestamp": "TIMESTAMP"
        }
    }
}

def ensure_table_schema(table_name, schema):
    """Проверяет и создаёт таблицу с заданной схемой, включая уникальные ограничения."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Проверяем существование таблицы
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'dds' AND table_name = %s
            )
        """, (table_name,))
        table_exists = cur.fetchone()[0]

        if not table_exists:
            logger.info(f"Создаём таблицу dds.{table_name}")
            cur.execute(f"""
                CREATE TABLE dds.{table_name} ({schema})
            """)
        else:
            # Проверяем наличие load_timestamp
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'dds' AND table_name = %s
                AND column_name = 'load_timestamp'
            """, (table_name,))
            columns = [row[0] for row in cur.fetchall()]

            if 'load_timestamp' not in columns:
                logger.info(f"Добавляем load_timestamp в dds.{table_name}")
                cur.execute(f"""
                    ALTER TABLE dds.{table_name}
                    ADD COLUMN load_timestamp TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
                """)

            # Проверяем наличие valid_from, valid_to, is_active для SCD2
            if TABLE_CONFIG[table_name]["operation_type"] == "SCD2":
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'dds' AND table_name = %s
                    AND column_name IN ('valid_from', 'valid_to', 'is_active')
                """, (table_name,))
                columns = [row[0] for row in cur.fetchall()]

                if 'valid_from' not in columns:
                    logger.info(f"Добавляем valid_from в dds.{table_name}")
                    cur.execute(f"""
                        ALTER TABLE dds.{table_name}
                        ADD COLUMN valid_from TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
                    """)
                if 'valid_to' not in columns:
                    logger.info(f"Добавляем valid_to в dds.{table_name}")
                    cur.execute(f"""
                        ALTER TABLE dds.{table_name}
                        ADD COLUMN valid_to TIMESTAMP
                    """)
                if 'is_active' not in columns:
                    logger.info(f"Добавляем is_active в dds.{table_name}")
                    cur.execute(f"""
                        ALTER TABLE dds.{table_name}
                        ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE
                    """)

            # Проверяем наличие уникального ограничения для UPSERT
            if TABLE_CONFIG[table_name]["operation_type"] == "UPSERT" and "unique_keys" in TABLE_CONFIG[table_name]:
                unique_keys = TABLE_CONFIG[table_name]["unique_keys"]
                constraint_name = f"{table_name}_unique_key"
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_constraint
                        WHERE connamespace = 'dds'::regnamespace
                        AND conrelid = ('dds.' || %s)::regclass
                        AND contype = 'u'
                        AND conname = %s
                    )
                """, (table_name, constraint_name))
                constraint_exists = cur.fetchone()[0]

                if not constraint_exists:
                    logger.info(f"Добавляем уникальное ограничение {constraint_name} в dds.{table_name}")
                    cur.execute(f"""
                        ALTER TABLE dds.{table_name}
                        ADD CONSTRAINT {constraint_name} UNIQUE ({', '.join(unique_keys)})
                    """)

        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при проверке/создании таблицы {table_name}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def log_to_postgres(process_name, object_name, layer, source_type, start_time, end_time, status, records_processed,
                    comment):
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

def generate_scd2_queries(table_name, stage_table, primary_keys, attributes, select_query):
    """Генерирует SQL-запросы для SCD2."""
    columns = primary_keys + attributes
    columns_with_alias = ", ".join([f"s.{col}" for col in columns])
    pk_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
    attr_condition = " OR ".join([f"t.{attr} IS DISTINCT FROM s.{attr}" for attr in attributes])
    column_types = TABLE_CONFIG[table_name]["column_types"]

    # Создаём/очищаем временную таблицу
    temp_table = f"temp_{table_name}"
    column_definitions = [f"{col} {column_types[col]}" for col in columns]
    create_temp_table = f"""
    DROP TABLE IF EXISTS {temp_table};
    CREATE TEMPORARY TABLE {temp_table} (
        {', '.join(column_definitions)},
        load_timestamp TIMESTAMP
    );
    """

    # Закрываем старые версии
    update_closed_versions = f"""
    UPDATE dds.{table_name} t
    SET valid_to = s.load_timestamp,
        is_active = FALSE
    FROM {temp_table} s
    WHERE {pk_condition}
      AND t.is_active = TRUE
      AND ({attr_condition})
      AND s.load_timestamp > t.load_timestamp;
    """

    # Вставляем новые версии
    insert_new_versions = f"""
    INSERT INTO dds.{table_name} ({', '.join(columns)}, load_timestamp, valid_from, valid_to, is_active)
    SELECT {columns_with_alias}, s.load_timestamp, s.load_timestamp, NULL::TIMESTAMP, TRUE
    FROM {temp_table} s
    WHERE EXISTS (
        SELECT 1 FROM dds.{table_name} t
        WHERE {pk_condition}
          AND t.valid_to = s.load_timestamp
          AND ({attr_condition})
          AND s.load_timestamp > t.load_timestamp
    )
    UNION
    SELECT {columns_with_alias}, s.load_timestamp, s.load_timestamp, NULL::TIMESTAMP, TRUE
    FROM {temp_table} s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.{table_name} t
        WHERE {pk_condition}
    );
    """

    return create_temp_table, update_closed_versions, insert_new_versions, temp_table

def generate_upsert_query(table_name, primary_keys, attributes, unique_keys, select_query):
    """Генерирует SQL-запрос для UPSERT с фильтрацией перед ON CONFLICT."""
    columns = primary_keys + attributes
    columns_with_alias = ", ".join([f"s.{col}" for col in columns])
    update_columns = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in attributes] + ["load_timestamp = EXCLUDED.load_timestamp"])
    conflict_columns = ", ".join(unique_keys)
    column_types = TABLE_CONFIG[table_name]["column_types"]

    # Создаём/очищаем временную таблицу
    temp_table = f"temp_{table_name}"
    column_definitions = [f"{col} {column_types[col]}" for col in columns]
    create_temp_table = f"""
    DROP TABLE IF EXISTS {temp_table};
    CREATE TEMPORARY TABLE {temp_table} (
        {', '.join(column_definitions)},
        load_timestamp TIMESTAMP
    );
    """

    # UPSERT-запрос с фильтрацией
    upsert_query = f"""
    INSERT INTO dds.{table_name} ({', '.join(columns)}, load_timestamp)
    SELECT {columns_with_alias}, s.load_timestamp
    FROM {temp_table} s
    LEFT JOIN dds.{table_name} t
        ON {' AND '.join([f"t.{key} = s.{key}" for key in unique_keys])}
    WHERE t.load_timestamp IS NULL OR t.load_timestamp < s.load_timestamp
    ON CONFLICT ({conflict_columns}) DO UPDATE
    SET {update_columns}
    WHERE dds.{table_name}.load_timestamp < EXCLUDED.load_timestamp;
    """

    return create_temp_table, upsert_query, temp_table

def check_column_types(table_name, stage_table, primary_keys, attributes):
    """Логирует типы данных столбцов для диагностики."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Типы в stage для всех релевантных столбцов
        relevant_columns = primary_keys + attributes + ['id', 'team_id', 'load_timestamp', 'game_id', 'team_id_home',
                                                        'team_id_away', 'person_id']
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'stage' AND table_name = %s
            AND column_name = ANY(%s)
        """, (stage_table, relevant_columns))
        stage_types = {}
        for row in cur.fetchall():
            stage_types[row[0]] = row[1]

        # Типы в dds
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'dds' AND table_name = %s
            AND column_name = ANY(%s)
        """, (table_name, primary_keys + attributes + ['load_timestamp']))
        dds_types = {}
        for row in cur.fetchall():
            dds_types[row[0]] = row[1]

        logger.info(
            f"Column types for {table_name}: stage.{stage_table} = {stage_types}, dds.{table_name} = {dds_types}")

    except psycopg2.Error as e:
        logger.error(f"Ошибка при проверке типов столбцов для {table_name}: {e}")
    finally:
        cur.close()
        conn.close()

def check_null_values(table_name, stage_table, select_query):
    """Логирует количество записей с NULL или некорректными значениями."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Определяем, какие столбцы проверять
        if table_name in ['dim_game']:
            null_columns = ['home_team_id', 'away_team_id']
            zero_columns = ['team_id_home', 'team_id_away']
        elif table_name in ['fact_games']:
            null_columns = ['team_id']
            zero_columns = ['team_id_home', 'team_id_away']
        else:
            null_columns = ['team_id']
            zero_columns = ['team_id']

        logger.info(f"Checking NULL/zero for {table_name}: null_columns={null_columns}, zero_columns={zero_columns}")

        # Подсчёт записей с NULL
        null_conditions = ' OR '.join([f"{col} IS NULL" for col in null_columns])
        null_query = f"""
        SELECT COUNT(*) AS null_count
        FROM ({select_query}) AS subquery
        WHERE {null_conditions}
        LIMIT 1
        """
        cur.execute(null_query)
        null_count = cur.fetchone()[0]

        # Подсчёт записей с '0'
        zero_conditions = ' OR '.join([f"{col} = '0'" for col in zero_columns])
        zero_query = f"""
        SELECT COUNT(*) AS zero_count
        FROM stage.{stage_table}
        WHERE {zero_conditions}
        LIMIT 1
        """
        cur.execute(zero_query)
        zero_count = cur.fetchone()[0]

        logger.info(
            f"Null/Zero checks for {table_name}: {null_conditions} = {null_count}, {zero_conditions} = {zero_count}")

    except psycopg2.Error as e:
        logger.error(f"Ошибка при проверке NULL/zero для {table_name}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def load_dds_table(table_name, dag_id, task_id):
    """Загружает данные в таблицы DDS."""

    start_time = time.time()
    inserted_records = 0
    updated_records = 0
    config = TABLE_CONFIG[table_name]
    stage_table = config['stage_table']
    operation_type = config['operation_type']
    primary_keys = config['primary_keys']
    attributes = config['attributes']
    select_query = config['select_query']
    schema = config['schema']

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Проверяем и создаём таблицу
        ensure_table_schema(table_name, schema)

        # Логируем типы данных столбцов
        check_column_types(table_name, stage_table, primary_keys, attributes)

        # Проверяем NULL/zero значения
        if table_name in ['dim_player', 'fact_shots', 'fact_games', 'dim_game']:
            check_null_values(table_name, stage_table, select_query)

        # Проверяем данные
        logger.info(f"Executing select_query for {table_name}: {select_query}")
        cur.execute(f"SELECT COUNT(*) FROM ({select_query}) AS subquery")
        data_count = cur.fetchone()[0]
        logger.info(f"Found {data_count} records for {table_name} before loading")

        if data_count == 0:
            logger.info(f"Нет новых данных для {table_name}, пропускаем загрузку")
            log_to_postgres(
                process_name=dag_id,
                object_name=table_name,
                layer="DDS",
                source_type="SQLITE",
                start_time=start_time,
                end_time=time.time(),
                status="SUCCESS",
                records_processed=0,
                comment="No new data"
            )
            return

        # Получаем столбцы из select_query
        cur.execute(select_query)
        columns = [desc[0] for desc in cur.description]

        if operation_type == "SCD2":
            create_temp_table, update_closed_versions, insert_new_versions, temp_table = generate_scd2_queries(
                table_name, stage_table, primary_keys, attributes, select_query
            )

            # Создаём/очищаем временную таблицу
            cur.execute(create_temp_table)
            logger.info(f"Создана/очищена временная таблица {temp_table} для {table_name}")

            # Прямая вставка из select_query в temp_table
            insert_into_temp = f"""
            INSERT INTO {temp_table} ({', '.join(columns)})
            {select_query}
            """
            cur.execute(insert_into_temp)
            logger.info(f"Данные вставлены в {temp_table} напрямую из select_query")

            # 1. Закрытие старых версий
            cur.execute(update_closed_versions)
            updated_records = cur.rowcount
            logger.info(f"Обновлены valid_to/is_active у {updated_records} записей в {table_name}")

            # 2. Вставка новых версий
            cur.execute(insert_new_versions)
            inserted_records = cur.rowcount
            logger.info(f"Вставлены {inserted_records} новые версии в {table_name}")

        elif operation_type == "UPSERT":
            create_temp_table, upsert_query, temp_table = generate_upsert_query(
                table_name, primary_keys, attributes, config['unique_keys'], select_query
            )

            # Создаём/очищаем временную таблицу
            cur.execute(create_temp_table)
            logger.info(f"Создана/очищена временная таблица {temp_table} для {table_name}")

            # Прямая вставка из select_query в temp_table
            insert_into_temp = f"""
            INSERT INTO {temp_table} ({', '.join(columns)})
            {select_query}
            """
            cur.execute(insert_into_temp)
            logger.info(f"Данные вставлены в {temp_table} напрямую из select_query")

            # Выполняем UPSERT
            cur.execute(upsert_query)
            inserted_records = cur.rowcount
            logger.info(f"Вставлены/обновлены {inserted_records} записи в {table_name}")

        conn.commit()
        status = "SUCCESS" if inserted_records + updated_records > 0 else "FAILED"
        comment = f"Inserted: {inserted_records}, Updated: {updated_records}"

        log_to_postgres(
            process_name=dag_id,
            object_name=table_name,
            layer="DDS",
            source_type="SQLITE",
            start_time=start_time,
            end_time=time.time(),
            status=status,
            records_processed=inserted_records + updated_records,
            comment=comment
        )

    except Exception as e:
        logger.error(f"Ошибка загрузки для {table_name}: {e}")
        log_to_postgres(
            process_name=dag_id,
            object_name=table_name,
            layer="DDS",
            source_type="SQLITE",
            start_time=start_time,
            end_time=time.time(),
            status="FAILED",
            records_processed=inserted_records + updated_records,
            comment=f"Error: {str(e)}"
        )
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()








