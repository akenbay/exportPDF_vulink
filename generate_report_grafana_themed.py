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

# ── Цвета для matplotlib (0-1 float) ──────────────────────
TEXT_WHITE     = (204/255, 204/255, 220/255)
TEXT_MUTED     = (120/255, 120/255, 130/255)
CHART_AVG      = "#F2CC0C"

# ── Цвета для fpdf (0-255 int) ────────────────────────────
DARK_BG_PDF        = (23,  23,  23)
PANEL_BG_PDF       = (30,  30,  30)
HEADER_BG_PDF      = (40,  40,  40)
ROW_EVEN_PDF       = (36,  36,  36)
ROW_ODD_PDF        = (30,  30,  30)
TEXT_WHITE_PDF     = (204, 204, 220)
TEXT_MUTED_PDF     = (120, 120, 130)
ACCENT_BLUE_PDF    = (110, 159, 255)
GREEN_STABLE_PDF   = (39,  174, 96)
RED_THREAT_PDF     = (235, 59,  90)

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
def fetch_level_data(locations, date_from, date_to):
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT timestamp AS time, location_name AS metric, value,
               AVG(value) OVER(PARTITION BY location_name) AS avg_value
        FROM sensor_readings_view
        WHERE parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
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
def fetch_summary_table(locations, date_from, date_to):
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        WITH BaseData AS (
          SELECT location_name, AVG(value) AS avg_val,
            (SELECT value FROM sensor_readings_view s2
             WHERE s2.location_name = s1.location_name
               AND s2.parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
               AND s2.timestamp BETWEEN %s AND %s
             ORDER BY timestamp DESC LIMIT 1) AS last_val,
            (SELECT timestamp FROM sensor_readings_view s2
             WHERE s2.location_name = s1.location_name
               AND s2.parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
               AND s2.timestamp BETWEEN %s AND %s
             ORDER BY timestamp DESC LIMIT 1) AS last_time
          FROM sensor_readings_view s1
          WHERE parameter_name IN ('Level: Elevation', 'Level: Depth to Water')
            AND location_name IN ({location_list})
            AND timestamp BETWEEN %s AND %s
          GROUP BY location_name
        )
        SELECT
          location_name AS "Название локации",
          avg_val AS "Средняя длина",
          last_val AS "Последняя длина",
          last_time AS "Время",
          CASE
            WHEN location_name = '60 к накопитель' AND last_val >= 0  AND last_val <= 0.8  THEN 'Стабильно'
            WHEN location_name = 'DH-1'  AND last_val >= 70 AND last_val <= 100 THEN 'Стабильно'
            WHEN location_name = 'DH-3'  AND last_val >= 40 AND last_val <= 80  THEN 'Стабильно'
            WHEN location_name = 'DHW-1' AND last_val >= 44 AND last_val <= 93  THEN 'Стабильно'
            WHEN location_name = 'DHW-2' AND last_val >= 30 AND last_val <= 90  THEN 'Стабильно'
            ELSE 'Угроза'
          END AS "Статус"
        FROM BaseData ORDER BY location_name
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(
        date_from, date_to,
        date_from, date_to,
        date_from, date_to,
    ))
    conn.close()
    df['Время'] = pd.to_datetime(df['Время'], utc=True).dt.tz_convert('Asia/Almaty').dt.tz_localize(None)
    return df

# ── Temperature Chart Query ────────────────────────────────
def fetch_temperature_data(locations, date_from, date_to):
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT timestamp AS time, location_name AS metric, value,
               AVG(value) OVER(PARTITION BY location_name) AS avg_value
        FROM sensor_readings_view
        WHERE parameter_name = 'Temperature'
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
def fetch_temperature_table(locations, date_from, date_to):
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT location_name AS "Название локации",
               AVG(value) AS "Средняя температура",
               (SELECT value FROM sensor_readings_view s2
                WHERE s2.location_name = s1.location_name
                  AND s2.parameter_name = 'Temperature'
                  AND s2.timestamp BETWEEN %s AND %s
                ORDER BY timestamp DESC LIMIT 1) AS "Последняя температура"
        FROM sensor_readings_view s1
        WHERE parameter_name = 'Temperature'
          AND location_name IN ({location_list})
          AND timestamp BETWEEN %s AND %s
        GROUP BY location_name ORDER BY location_name
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(date_from, date_to, date_from, date_to))
    conn.close()
    return df

# ── Pressure Chart Query ───────────────────────────────────
def fetch_pressure_data(locations, date_from, date_to):
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT timestamp AS time, location_name AS metric, value,
               AVG(value) OVER(PARTITION BY location_name) AS avg_value
        FROM sensor_readings_view
        WHERE parameter_name = 'Pressure'
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
def fetch_pressure_table(locations, date_from, date_to):
    location_list = ", ".join([f"'{loc}'" for loc in locations])
    query = f"""
        SELECT location_name AS "Название локации",
               AVG(value) AS "Среднее давление",
               (SELECT value FROM sensor_readings_view s2
                WHERE s2.location_name = s1.location_name
                  AND s2.parameter_name = 'Pressure'
                  AND s2.timestamp BETWEEN %s AND %s
                ORDER BY timestamp DESC LIMIT 1) AS "Последнее давление"
        FROM sensor_readings_view s1
        WHERE parameter_name = 'Pressure'
          AND location_name IN ({location_list})
          AND timestamp BETWEEN %s AND %s
        GROUP BY location_name ORDER BY location_name
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(date_from, date_to, date_from, date_to))
    conn.close()
    return df

# ── Locations Summary Query ────────────────────────────────
def fetch_locations_summary(locations, date_from, date_to):
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
        WHERE location_name IN ({location_list})
          AND timestamp BETWEEN %s AND %s
        GROUP BY location_id, location_name, parameter_name
        ORDER BY location_name, "Параметр"
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(date_from, date_to))
    conn.close()
    return df

# ── Generate Chart (Grafana Dark Theme) ───────────────────
def generate_chart(df, title, ylabel="", convert_to_mm=True, filename="chart_temp.png", unit=""):
    colors = ["#73BF69", "#5794F2", "#FF9830", "#F2CC0C", "#E02F44", "#B877D9"]

    with plt.style.context("dark_background"):
        fig, ax = plt.subplots(figsize=(11, 3.8))
        fig.patch.set_facecolor("#111217")
        ax.set_facecolor("#111217")

        for i, (metric, group) in enumerate(df.groupby("metric")):
            color = colors[i % len(colors)]
            values = group["value"] * 1000 if convert_to_mm else group["value"]
            avg = group["avg_value"].iloc[0] * 1000 if convert_to_mm else group["avg_value"].iloc[0]

            ax.plot(group["time"], values, label=metric,
                    color=color, linewidth=1.5)
            ax.axhline(avg, linestyle="--", linewidth=1.2,
                       color=CHART_AVG, alpha=0.8,
                       label=f"срд {metric}: {avg:.2f}")

        ax.set_title(title, fontsize=12, fontweight='bold', color=TEXT_WHITE, pad=8)
        ax.set_xlabel("")
        ax.set_ylabel(ylabel, fontsize=9, color=TEXT_MUTED)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f} {unit}".strip()))
        ax.tick_params(colors="#9FA7B3", labelsize=8)
        ax.xaxis.set_tick_params(rotation=25)

        for spine in ax.spines.values():
            spine.set_edgecolor("#2C3235")

        ax.grid(True, color="#2C3235", linewidth=0.6, linestyle='-')
        ax.legend(fontsize=7, loc='best',
                  facecolor="#1F2126", edgecolor="#2C3235",
                  labelcolor=TEXT_WHITE)

        plt.tight_layout(pad=0.8)
        fig.savefig(filename, dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
        plt.close(fig)
    return filename

# ── Draw Table (Grafana Dark Theme) ───────────────────────
def draw_table(pdf, df, title, col_widths=None, right_align_cols=None, font_size=8, header_font_size=9):
    columns = list(df.columns)
    n = len(columns)

    if col_widths is None:
        col_widths = [190 // n] * n
    if right_align_cols is None:
        right_align_cols = list(range(1, n))

    # Section title
    pdf.set_font("DejaVu", "B", 12)
    pdf.set_text_color(*TEXT_WHITE_PDF)
    pdf.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)

    # Header row
    pdf.set_font("DejaVu", "B", header_font_size)
    pdf.set_fill_color(*HEADER_BG_PDF)
    pdf.set_text_color(*TEXT_MUTED_PDF)
    for i, col in enumerate(columns):
        align = "R" if i in right_align_cols else "L"
        pdf.cell(col_widths[i], 7, col, border=0, fill=True, align=align)
    pdf.ln()

    # Separator line
    pdf.set_draw_color(60, 60, 60)
    pdf.set_line_width(0.3)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(1)

    # Data rows
    pdf.set_font("DejaVu", "", font_size)
    for idx, row in df.iterrows():
        if idx % 2 == 0:
            pdf.set_fill_color(*ROW_EVEN_PDF)
        else:
            pdf.set_fill_color(*ROW_ODD_PDF)

        for i, col in enumerate(columns):
            align = "R" if i in right_align_cols else "L"
            val = row[col]

            if isinstance(val, float):
                text = f"{val:.3f}"
            else:
                text = str(val)[:35]

            if col == "Статус":
                if val == "Стабильно":
                    pdf.set_fill_color(*GREEN_STABLE_PDF)
                    pdf.set_text_color(255, 255, 255)
                else:
                    pdf.set_fill_color(*RED_THREAT_PDF)
                    pdf.set_text_color(255, 255, 255)
                align = "C"
            else:
                pdf.set_text_color(*TEXT_WHITE_PDF)

            pdf.cell(col_widths[i], 6, text, border=0, fill=True, align=align)

        pdf.ln()

    pdf.ln(1)

# ── PDF ────────────────────────────────────────────────────
FONT_DIR = os.path.dirname(os.path.abspath(__file__))

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.add_font("DejaVu", "",  os.path.join(FONT_DIR, "DejaVuSans.ttf"))
        self.add_font("DejaVu", "B", os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf"))
        self.add_font("DejaVu", "I", os.path.join(FONT_DIR, "DejaVuSans-Oblique.ttf"))

    def header(self):
        self.set_fill_color(*DARK_BG_PDF)
        self.rect(0, 0, 210, 14, style='F')
        self.set_font("DejaVu", "B", 8)
        self.set_text_color(*TEXT_MUTED_PDF)
        self.set_y(4)
        self.cell(0, 6, "piezometrics | отчет по скважинам | hydrovu",
                  align="R", new_x="LMARGIN", new_y="NEXT")
        self.ln(4)

    def footer(self):
        self.set_fill_color(*DARK_BG_PDF)
        self.rect(0, 287, 210, 10, style='F')
        self.set_y(-12)
        self.set_font("DejaVu", "I", 8)
        self.set_text_color(*TEXT_MUTED_PDF)
        self.cell(0, 8, f"Страница {self.page_no()}", align="C")

    def add_page(self, *args, **kwargs):
        super().add_page(*args, **kwargs)
        self.set_fill_color(*DARK_BG_PDF)
        self.rect(0, 0, 210, 297, style='F')

# ── Create PDF ─────────────────────────────────────────────
def create_pdf(locations, date_from, date_to, output="report.pdf"):
    pdf = PDF()
    pdf.set_auto_page_break(auto=True, margin=18)

    all_locations = ["60 к накопитель", "DH-1", "DH-3", "DHW-1", "DHW-2", "ГГ-4"]

    # ── Title Page ─────────────────────────────────────────
    pdf.add_page()
    pdf.ln(55)

    pdf.set_font("DejaVu", "B", 28)
    pdf.set_text_color(*TEXT_WHITE_PDF)
    pdf.cell(0, 14, "Отчет по скважинам", align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("DejaVu", "B", 18)
    pdf.set_text_color(*TEXT_WHITE_PDF)
    pdf.ln(2)
    pdf.cell(0, 10, "HydroVu", align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(8)
    pdf.set_draw_color(*TEXT_WHITE_PDF)
    pdf.set_line_width(0.5)
    pdf.line(50, pdf.get_y(), 160, pdf.get_y())
    pdf.ln(10)

    pdf.set_font("DejaVu", "", 11)
    pdf.set_text_color(*TEXT_MUTED_PDF)
    pdf.cell(0, 8, f"Период:  {date_from}  →  {date_to}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.cell(0, 8, f"Дата создания:  {datetime.now().strftime('%Y-%m-%d %H:%M')}", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── Page 2: Уровень + Температура ─────────────────────
    pdf.add_page()

    print("Загрузка данных уровня...")
    df_level = fetch_level_data(locations, date_from, date_to)
    if not df_level.empty:
        chart_path = generate_chart(df_level, title="Уровень (Высота/Глубина до воды)",
                                    ylabel="", convert_to_mm=True, unit="мм",
                                    filename="chart_level.png")
        pdf.set_font("DejaVu", "B", 12)
        pdf.set_text_color(*TEXT_WHITE_PDF)
        pdf.cell(0, 8, "Уровень (Высота/Глубина до воды)", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    pdf.ln(3)

    print("Загрузка сводной таблицы...")
    df_table = fetch_summary_table(all_locations, date_from, date_to)
    draw_table(pdf, df_table,
               title="Уровень (Высота/Глубина до воды) в метрах",
               col_widths=[50, 28, 28, 52, 32])

    pdf.ln(3)

    print("Загрузка данных температуры...")
    df_temp = fetch_temperature_data(locations, date_from, date_to)
    if not df_temp.empty:
        chart_path = generate_chart(df_temp, title="Температура",
                                    ylabel="", convert_to_mm=False, unit="°C",
                                    filename="chart_temperature.png")
        pdf.set_font("DejaVu", "B", 12)
        pdf.set_text_color(*TEXT_WHITE_PDF)
        pdf.cell(0, 8, "Температура", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    pdf.ln(3)

    print("Загрузка таблицы температур...")
    df_temp_table = fetch_temperature_table(all_locations, date_from, date_to)
    if not df_temp_table.empty:
        draw_table(pdf, df_temp_table,
                   title="Температура (°C)",
                   col_widths=[70, 60, 60])

    # ── Page 3: Давление + Сводная ────────────────────────
    pdf.add_page()

    print("Загрузка данных давления...")
    df_pressure = fetch_pressure_data(locations, date_from, date_to)
    if not df_pressure.empty:
        chart_path = generate_chart(df_pressure, title="Давление",
                                    ylabel="", convert_to_mm=False, unit="psi",
                                    filename="chart_pressure.png")
        pdf.set_font("DejaVu", "B", 12)
        pdf.set_text_color(*TEXT_WHITE_PDF)
        pdf.cell(0, 8, "Давление", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    pdf.ln(3)

    print("Загрузка таблицы давления...")
    df_pressure_table = fetch_pressure_table(all_locations, date_from, date_to)
    if not df_pressure_table.empty:
        draw_table(pdf, df_pressure_table,
                   title="Давление (psi)",
                   col_widths=[70, 60, 60])

    pdf.ln(8)

    print("Загрузка сводной таблицы локаций...")
    df_locations = fetch_locations_summary(all_locations, date_from, date_to)
    if not df_locations.empty:
        draw_table(pdf, df_locations,
                   title="Сводная информация по локациям",
                   col_widths=[24, 35, 22, 22, 57, 30],
                   right_align_cols=[0, 2, 3, 5],
                   font_size=6,
                   header_font_size=7)

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
