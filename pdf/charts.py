# ── Generate Chart ─────────────────────────────────────────
import matplotlib.pyplot as plt
import pandas as pd


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