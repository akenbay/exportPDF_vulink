import os
from fpdf import FPDF
import pandas as pd


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
FONT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/fonts"

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