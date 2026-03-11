
# ── Chart Query ────────────────────────────────────────────
import pandas as pd
from db import get_connection


def fetch_level_data(locations: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT 
            timestamp AS time,
            location_name AS metric,
            value,
            AVG(value) OVER(PARTITION BY location_name) AS avg_value
        FROM sensor_readings_view
        WHERE 
            parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
            AND location_name IN ({location_list})
            AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(date_from, date_to))
    conn.close()
    df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_convert('Asia/Almaty').dt.tz_localize(None)
    return df

# ── Summary Table Query ────────────────────────────────────
def fetch_summary_table(locations: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        WITH BaseData AS (
          SELECT 
            location_name, 
            AVG(value) AS avg_val, 
            (
              SELECT value 
              FROM sensor_readings_view s2 
              WHERE s2.location_name = s1.location_name 
                AND s2.parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
                AND s2.timestamp BETWEEN %s AND %s
              ORDER BY timestamp DESC 
              LIMIT 1
            ) AS last_val,
            (
              SELECT timestamp 
              FROM sensor_readings_view s2 
              WHERE s2.location_name = s1.location_name 
                AND s2.parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
                AND s2.timestamp BETWEEN %s AND %s
              ORDER BY timestamp DESC 
              LIMIT 1
            ) AS last_time
          FROM sensor_readings_view s1
          WHERE 
            parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
            AND location_name IN ({location_list})
            AND timestamp BETWEEN %s AND %s
          GROUP BY location_name
        )
        SELECT 
          location_name AS "Название локации",
          avg_val AS "Средняя",
          last_val AS "Последняя",
          last_time AS "Время",
          CASE
            WHEN location_name = '60 к накопитель' AND last_val >= 0   AND last_val <= 0.8  THEN 'Стабильно'
            WHEN location_name = 'DH-1'            AND last_val >= 70  AND last_val <= 100  THEN 'Стабильно'
            WHEN location_name = 'DH-3'            AND last_val >= 40  AND last_val <= 80   THEN 'Стабильно'
            WHEN location_name = 'DHW-1'           AND last_val >= 44  AND last_val <= 93   THEN 'Стабильно'
            WHEN location_name = 'DHW-2'           AND last_val >= 30  AND last_val <= 90   THEN 'Стабильно'
            ELSE 'Угроза'
          END AS "Статус"
        FROM BaseData
        ORDER BY location_name
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(
        date_from, date_to,  # подзапрос last_val
        date_from, date_to,  # подзапрос last_time
        date_from, date_to,  # основной WHERE
    ))
    conn.close()
    df['Время'] = pd.to_datetime(df['Время'], utc=True).dt.tz_convert('Asia/Almaty').dt.tz_localize(None)
    return df

# ── Temperature Chart Query ────────────────────────────────
def fetch_temperature_data(locations: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT 
            timestamp AS time,
            location_name AS metric,
            value,
            AVG(value) OVER(PARTITION BY location_name) AS avg_value
        FROM sensor_readings_view
        WHERE 
            parameter_name = 'Temperature'
            AND location_name IN ({location_list})
            AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(date_from, date_to))
    conn.close()
    df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_convert('Asia/Almaty').dt.tz_localize(None)
    return df

# ── Temperature Table Query ────────────────────────────────
def fetch_temperature_table(locations: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT 
          location_name AS "Название локации", 
          AVG(value) AS "Средняя температура", 
          (
            SELECT value 
            FROM sensor_readings_view s2 
            WHERE s2.location_name = s1.location_name 
              AND s2.parameter_name = 'Temperature'
              AND s2.timestamp BETWEEN %s AND %s
            ORDER BY timestamp DESC 
            LIMIT 1
          ) AS "Последняя температура"
        FROM sensor_readings_view s1
        WHERE 
          parameter_name = 'Temperature'
          AND location_name IN ({location_list})
          AND timestamp BETWEEN %s AND %s
        GROUP BY location_name
        ORDER BY location_name
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(
        date_from, date_to,  # подзапрос
        date_from, date_to,  # основной WHERE
    ))
    conn.close()
    return df

# ── Pressure Chart Query ───────────────────────────────────
def fetch_pressure_data(locations: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT 
            timestamp AS time,
            location_name AS metric,
            value,
            AVG(value) OVER(PARTITION BY location_name) AS avg_value
        FROM sensor_readings_view
        WHERE 
            parameter_name = 'Pressure'
            AND location_name IN ({location_list})
            AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(date_from, date_to))
    conn.close()
    df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_convert('Asia/Almaty').dt.tz_localize(None)
    return df

# ── Pressure Table Query ───────────────────────────────────
def fetch_pressure_table(locations: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT 
          location_name AS "Название локации", 
          AVG(value) AS "Среднее давление", 
          (
            SELECT value 
            FROM sensor_readings_view s2 
            WHERE s2.location_name = s1.location_name 
              AND s2.parameter_name = 'Pressure'
              AND s2.timestamp BETWEEN %s AND %s
            ORDER BY timestamp DESC 
            LIMIT 1
          ) AS "Последнее давление"
        FROM sensor_readings_view s1
        WHERE 
          parameter_name = 'Pressure'
          AND location_name IN ({location_list})
          AND timestamp BETWEEN %s AND %s
        GROUP BY location_name
        ORDER BY location_name
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(
        date_from, date_to,  # подзапрос
        date_from, date_to,  # основной WHERE
    ))
    conn.close()
    return df

# ── Locations Summary Query ────────────────────────────────
def fetch_locations_summary(locations: list[str], date_from: str, date_to: str) -> pd.DataFrame:
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT 
          location_id AS "Айди локации", 
          location_name AS "Название локации", 
          AVG(latitude) AS "Широта", 
          AVG(longitude) AS "Долгота", 
          CASE parameter_name
            WHEN 'Pressure'              THEN 'Давление (psi)'
            WHEN 'Temperature'           THEN 'Температура (°C)'
            WHEN 'Level: Depth to Water' THEN 'Уровень: Глубина до воды (м)'
            WHEN 'Level: Elevation'      THEN 'Уровень: Высота (м)'
            WHEN 'Depth'                 THEN 'Глубина (м)'
            ELSE parameter_name
          END AS "Параметр",
          AVG(value) AS "Срд значение"
        FROM sensor_readings_view
        WHERE 
          location_name IN ({location_list})
          AND timestamp BETWEEN %s AND %s
        GROUP BY location_id, location_name, parameter_name
        ORDER BY location_name, "Параметр"
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(date_from, date_to))
    conn.close()
    return df

