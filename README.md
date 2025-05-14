# Аналитика статистики НБА: Автоматизация ETL и визуализация 🏀

Добро пожаловать в **Аналитика статистики НБА: Автоматизация ETL и визуализация**! Этот проект автоматизирует сбор, обработку и анализ статистики NBA, предоставляя аналитические дашборды в Metabase. От сырых данных до крутых инсайтов — давай начнём! 🚀

## 📝 О проекте
Проект создаёт ETL-пайплайн для обработки данных NBA из SQLite и CSV, их хранения в PostgreSQL DWH и визуализации в Metabase. Основные возможности:
- **Данные**: Статистика игр, бросков, игроков и команд (2004–2025).
- **Технологии**: Python, Airflow, PostgreSQL, Metabase, Docker.
- **Результат**: Дашборды для анализа эффективности бросков, командной результативности и трендов трёхочковых.

## 🛠 Требования
- Docker и Docker Compose
- Python 3.8+
- Зависимости (см. `requirements.txt`):
  ```
  psycopg2-binary
  SQLAlchemy-Utils<0.38.0
  tenacity>=8.0.0
  ```

## 🚀 Установка и развертывание
1. **Клонируйте репозиторий**:
   ```bash
   git clone <repository_url>
   cd nba_stats_pipeline
   ```

2. **Настройте Docker**:
   - Убедитесь, что Docker запущен.
   - Скопируйте `docker-compose.yml` в корень проекта.
   - Разместите файлы данных:
     - SQLite: `/opt/airflow/data/nba.sqlite`
     - CSV: `/opt/airflow/data/csv/NBA_{year}_Shots.csv`

3. **Установите зависимости Python**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Запустите Docker Compose**:
   ```bash
   docker-compose up -d
   ```
   - Airflow: `http://localhost:8080` (логин: `admin`, пароль: `admin`)
   - Metabase: `http://localhost:3000`
   - PostgreSQL: `host=postgres_db1`, `port=5432`, `user=admin`, `password=admin`

5. **Инициализируйте Airflow**:
   - Откройте Airflow UI.
   - Запустите `dag_load_static.py` вручную, чтобы начать ETL.

## 📂 Структура проекта
```
🏠 dockercompose/                  
├── 🌬️ dags/                           
│   ├── dag_load_static.py             # 🗄️ Статические таблицы
│   ├── dag_load_incremental_game.py   # 🎮 Данные об играх
│   ├── dag_load_incremental_line_score_game.py  # 🕒 Счёт по четвертям
│   ├── dag_load_incremental_shot_data.py       # 🏀 Данные о бросках
│   ├── dag_load_dds.py                # 🗂️ Слой DDS
│   ├── dag_data_marts.py              # 📊 Витрины данных
│   ├── load_static_tables.py         
│   ├── load_incremental_game.py       
│   ├── load_incremental_line_score_game.py  
│   ├── load_incremental_shot_data.py 
│   ├── load_dds_tables.py            
│   ├── load_data_marts.py             
├── 🐳 docker-compose.yml              
├── 📖 README.md                       
```

## 🏀 Запуск пайплайна
1. Откройте Airflow UI (`http://localhost:8080`).
2. Включите и запустите `dag_load_static.py` вручную(или по расписанию).
3. Пайплайн последовательно выполнит DAGs:
   - Статические таблицы → Игры → Счёт по четвертям → Броски → DDS → Витрины.
4. Следите за логами в `/opt/airflow/logs` или в таблице `tech.load_logs` в PostgreSQL.

## 📊 Доступ к дашбордам
- Откройте Metabase: `http://localhost:3000`
- Проверьте данные в Metabase (используйте схему dm)
- Примеры дашбордов:
  - 🏀 **Эффективность бросков игроков**: Сравните точность Леброна на средней дистанции и Карри с трёхочковой.
  - 🏀 **Результативность команд**: Узнайте, кто доминирует в краске.
  - 🏀 **Тренды трёхочковых**: Отследите рост популярности трёхочковых по сезонам.

## 🔧 Устранение неполадок
- **Ошибки Airflow**: Проверьте логи в `/opt/airflow/logs`.
- **Проблемы с данными**: Изучите `tech.load_logs` для выявления сбоев.
- **Metabase**: Убедитесь в корректности подключения к PostgreSQL (`host=postgres_db1`, `port=5432`).

## 🌟 Как внести вклад
Хотите добавить новые метрики или дашборды? Форкните репозиторий, создайте ветку и отправьте PR! 🏀
