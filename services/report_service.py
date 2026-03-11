import os
import tempfile
from io import BytesIO
from datetime import datetime

from db import (
    fetch_level_data,
    fetch_summary_table,
    fetch_temperature_data,
    fetch_temperature_table,
    fetch_pressure_data,
    fetch_pressure_table,
    fetch_locations_summary,
)
from pdf.charts import generate_chart
from pdf.builder import PDF, draw_table

ALL_LOCATIONS = ["60 к накопитель", "DH-1", "DH-3", "DHW-1", "DHW-2", "ГГ-4"]


def generate_report_bytes(locations: list[str], date_from: str, date_to: str) -> BytesIO:
    pdf = PDF()
    pdf.set_auto_page_break(auto=True, margin=15)

    # ── Title Page ─────────────────────────────────────────────────────────────
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

    # ── Page 2: Level ──────────────────────────────────────────────────────────
    pdf.add_page()

    df_level = fetch_level_data(locations, date_from, date_to)
    if not df_level.empty:
        chart_path = _temp_chart(
            df_level, title="Уровень", ylabel="Уровень",
            convert_to_mm=True, unit="мм", filename="chart_level.png"
        )
        pdf.set_font("DejaVu", "B", 13)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(0, 8, "Уровень (Высота/Глубина до воды)", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    pdf.ln(3)

    df_summary = fetch_summary_table(ALL_LOCATIONS, date_from, date_to)
    if not df_summary.empty:
        draw_table(pdf, df_summary,
                   title="Уровень (Высота/Глубина до воды) в метрах",
                   col_widths=[50, 30, 30, 50, 30])

    pdf.ln(3)

    # Temperature chart
    df_temp = fetch_temperature_data(locations, date_from, date_to)
    if not df_temp.empty:
        chart_path = _temp_chart(
            df_temp, title="Температура", ylabel="Температура",
            convert_to_mm=False, unit="°C", filename="chart_temperature.png"
        )
        pdf.set_font("DejaVu", "B", 13)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(0, 8, "График температуры", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    pdf.ln(3)

    # Temperature table
    df_temp_table = fetch_temperature_table(ALL_LOCATIONS, date_from, date_to)
    if not df_temp_table.empty:
        draw_table(pdf, df_temp_table,
                   title="Температура (°C)",
                   col_widths=[70, 60, 60])

    # ── Page 3: Pressure + Locations Summary ───────────────────────────────────
    pdf.add_page()

    df_pressure = fetch_pressure_data(locations, date_from, date_to)
    if not df_pressure.empty:
        chart_path = _temp_chart(
            df_pressure, title="Давление", ylabel="Давление",
            convert_to_mm=False, unit="psi", filename="chart_pressure.png"
        )
        pdf.set_font("DejaVu", "B", 13)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(0, 8, "График давления", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.image(chart_path, x=10, w=190)
        os.remove(chart_path)

    df_pressure_table = fetch_pressure_table(ALL_LOCATIONS, date_from, date_to)
    if not df_pressure_table.empty:
        pdf.ln(3)
        draw_table(pdf, df_pressure_table,
                   title="Давление (psi)",
                   col_widths=[70, 60, 60])

    pdf.ln(10)

    df_locations = fetch_locations_summary(ALL_LOCATIONS, date_from, date_to)
    if not df_locations.empty:
        draw_table(pdf, df_locations,
                   title="Сводная информация по локациям",
                   col_widths=[24, 35, 22, 22, 50, 27],
                   right_align_cols=[0, 2, 3, 5],
                   font_size=6,
                   header_font_size=7)

    # ── Output to BytesIO ──────────────────────────────────────────────────────
    buffer = BytesIO()
    pdf.output(buffer)
    buffer.seek(0)
    return buffer


def _temp_chart(df, title, ylabel, convert_to_mm, unit, filename) -> str:
    """Saves chart to /tmp and returns its path."""
    tmp_path = os.path.join(tempfile.gettempdir(), filename)
    return generate_chart(
        df, title=title, ylabel=ylabel,
        convert_to_mm=convert_to_mm, unit=unit,
        filename=tmp_path
    )
