import os
import psycopg2
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from fpdf import FPDF
from datetime import datetime

load_dotenv()

# ── Database Connection ────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

# ── Chart Query ────────────────────────────────────────────
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



# ── Generate Chart ─────────────────────────────────────────
def generate_chart(df: pd.DataFrame, title: str, ylabel: str = "Уровень (мм)",
                   convert_to_mm: bool = True, filename: str = "chart_temp.png",
                   unit: str = "") -> str:
    fig, ax = plt.subplots(figsize=(11, 4))

    for metric, group in df.groupby("metric"):
        values = group["value"] * 1000 if convert_to_mm else group["value"]
        avg = group["avg_value"].iloc[0] * 1000 if convert_to_mm else group["avg_value"].iloc[0]

        ax.plot(group["time"], values,
                label=f"{metric}", linewidth=1.8, marker='o', markersize=2)
        ax.axhline(avg, linestyle="--", linewidth=1.2, alpha=0.6,
                   label=f"срд {metric}: {avg:.2f}")

    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.set_xlabel("Время", fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)

    # Форматтер — unit подставляется автоматически
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f"{x:.2f} {unit}".strip())
    )

    ax.legend(fontsize=8, loc='best')
    ax.grid(True, alpha=0.3, linestyle='--')
    plt.xticks(rotation=30, ha='right', fontsize=8)
    plt.tight_layout()

    fig.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)
    return filename

def draw_table(pdf: FPDF, df: pd.DataFrame, title: str, col_widths: list = None, right_align_cols: list = None, font_size: int = 8, header_font_size: int = 9):
    """
    title         — заголовок над таблицей
    col_widths    — ширины колонок (если None — делится поровну)
    right_align_cols — индексы колонок с выравниванием по правому краю
    """
    columns = list(df.columns)
    n = len(columns)

    if col_widths is None:
        col_widths = [190 // n] * n
    if right_align_cols is None:
        right_align_cols = list(range(1, n))  # все кроме первой — по правому краю

    # Заголовок секции
    pdf.set_font("DejaVu", "B", 13)
    pdf.set_text_color(30, 80, 160)
    pdf.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # Header row
    pdf.set_font("DejaVu", "B", header_font_size)
    pdf.set_fill_color(50, 50, 50)
    pdf.set_text_color(255, 255, 255)
    for i, col in enumerate(columns):
        align = "R" if i in right_align_cols else "C"
        pdf.cell(col_widths[i], 7, col, border=1, fill=True, align=align)
    pdf.ln()

    # Data rows
    pdf.set_font("DejaVu", "", font_size)
    for idx, row in df.iterrows():
        pdf.set_fill_color(245, 245, 245) if idx % 2 == 0 else pdf.set_fill_color(255, 255, 255)

        for i, col in enumerate(columns):
            pdf.set_text_color(0, 0, 0)
            align = "R" if i in right_align_cols else "C"
            val = row[col]

            # Форматирование числовых значений
            if isinstance(val, float):
                text = f"{val:.3f}"
            else:
                text = str(val)[:30]

            # Статус — цветная ячейка
            if col == "Статус":
                if val == "Стабильно":
                    pdf.set_fill_color(200, 230, 201)
                    pdf.set_text_color(27, 94, 32)
                else:
                    pdf.set_fill_color(255, 205, 210)
                    pdf.set_text_color(183, 28, 28)
                align = "C"

            pdf.cell(col_widths[i], 6, text, border=1, fill=True, align=align)

        pdf.ln()

# ── PDF ────────────────────────────────────────────────────
FONT_DIR = os.path.dirname(os.path.abspath(__file__))

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("DejaVu", "",  os.path.join(FONT_DIR, "DejaVuSans.ttf"))
        self.add_font("DejaVu", "B", os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf"))
        self.add_font("DejaVu", "I", os.path.join(FONT_DIR, "DejaVuSans-Oblique.ttf"))

    def header(self):
        self.set_font("DejaVu", "B", 9)
        self.set_text_color(120, 120, 120)
        self.cell(0, 8, "piezometrics | отчет по скважинам | hydrovu",
                  align="R", new_x="LMARGIN", new_y="NEXT")
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("DejaVu", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Страница {self.page_no()}", align="C")



# ── Create PDF ─────────────────────────────────────────────
def create_pdf(locations: list[str], date_from: str, date_to: str, output: str = "report.pdf"):
    pdf = PDF()
    pdf.set_auto_page_break(auto=True, margin=15)

    # ── Title Page ─────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("DejaVu", "B", 26)
    pdf.set_text_color(30, 80, 160)
    pdf.ln(60)
    pdf.cell(0, 12, "Отчет по скважинам", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 12, "HydroVu", align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("DejaVu", "", 12)
    pdf.set_text_color(100, 100, 100)
    pdf.ln(15)
    pdf.cell(0, 8, f"Период: {date_from}  →  {date_to}",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.cell(0, 8, f"Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(12)

    # ── Data Page ──────────────────────────────────────────
    pdf.add_page()

    # График уровня
    print("Загрузка данных уровня...")
    df_level = fetch_level_data(locations, date_from, date_to)

    if not df_level.empty:
        print(f"✓ Уровень: {len(df_level)} записей")
        chart_path = generate_chart(df_level,    title="Уровень", ylabel="Уровень",       convert_to_mm=True,  unit="мм",  filename="chart_level.png")
        pdf.set_font("DejaVu", "B", 13)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(0, 8, "Уровень (Высота/Глубина до воды)", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    pdf.ln(3)

    # Сводная таблица
    print("Загрузка сводной таблицы...")
    all_locations = ["60 к накопитель", "DH-1", "DH-3", "DHW-1", "DHW-2", "ГГ-4"]
    df_table = fetch_summary_table(all_locations, date_from, date_to)
    draw_table(pdf, df_table,
        title="Уровень (Высота/Глубина до воды) в метрах",
        col_widths=[50, 30, 30, 50, 30]
    )

    pdf.ln(3)

    # График температуры
    print("Загрузка данных температуры...")
    df_temp = fetch_temperature_data(locations, date_from, date_to)

    if not df_temp.empty:
        print(f"✓ Температура: {len(df_temp)} записей")
        chart_path = generate_chart(df_temp,     title="Температура", ylabel="Температура",    convert_to_mm=False, unit="°C",  filename="chart_temperature.png")
        pdf.set_font("DejaVu", "B", 13)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(0, 8, "График температуры", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    pdf.ln(3)  # уменьшен отступ

    # Таблица температур
    print("Загрузка таблицы температур...")
    all_locations = ["60 к накопитель", "DH-1", "DH-3", "DHW-1", "DHW-2", "ГГ-4"]
    df_temp_table = fetch_temperature_table(all_locations, date_from, date_to)
    if not df_temp_table.empty:
        draw_table(pdf, df_temp_table,
            title="Температура (°C)",
            col_widths=[70, 60, 60]
        )

    # ── Страница 3: Давление + Сводная ────────────────
    pdf.add_page()

    # График давления
    print("Загрузка данных давления...")
    df_pressure = fetch_pressure_data(locations, date_from, date_to)

    if not df_pressure.empty:
        print(f"✓ Давление: {len(df_pressure)} записей")
        chart_path = generate_chart(df_pressure, title="Давление", ylabel="Давление",          convert_to_mm=False, unit="psi", filename="chart_pressure.png")
        pdf.set_font("DejaVu", "B", 13)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(0, 8, "График давления", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    # Таблица давления
    print("Загрузка таблицы давления...")
    df_pressure_table = fetch_pressure_table(all_locations, date_from, date_to)
    if not df_pressure_table.empty:
        pdf.ln(3)
        draw_table(pdf, df_pressure_table,
            title="Давление (psi)",
            col_widths=[70, 60, 60]
        )

    pdf.ln(10)

    print("Загрузка сводной таблицы локаций...")
    all_locations = ["60 к накопитель", "DH-1", "DH-3", "DHW-1", "DHW-2", "ГГ-4"]
    df_locations = fetch_locations_summary(all_locations, date_from, date_to)

    if not df_locations.empty:
        draw_table(pdf, df_locations,
            title="Сводная информация по локациям",
            col_widths=[24, 35, 22, 22, 50, 27],
            right_align_cols=[0, 2, 3, 5],
            font_size=6,
            header_font_size=7
        )


    pdf.output(output)
    print(f"\n✅ PDF сохранен: {output}")


# ── Main ───────────────────────────────────────────────────
if __name__ == "__main__":
    create_pdf(
        locations=["60 к накопитель"],
        date_from="2026-02-09",
        date_to="2026-03-11",
        output="hydrovu_report.pdf"
    )
