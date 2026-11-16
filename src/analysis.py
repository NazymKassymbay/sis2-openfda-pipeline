"""Stage 6 - Exploratory data analysis and visualization.

The analysis reads the data back from the SQLite database (the final stored
output of the pipeline), so every figure reflects what was actually loaded.

Research questions
  Q1. Which adverse reactions are reported most often?
  Q2. How does the share of serious reports change with age group and sex?
  Q3. How is patient age distributed for serious vs non-serious reports?
  Q4. Which seriousness criteria (hospitalization, death, ...) dominate?
  Q5. How does the share of serious reports change from month to month?
"""

import json
import sqlite3

import matplotlib

matplotlib.use("Agg")  # file output only, no display needed
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src import config
from src.cleaner import AGE_GROUP_LABELS
from src.metrics import update_metrics

# Colour assignment by job: identity (sex / seriousness) uses fixed categorical
# slots; Unknown is neutral grey so it never competes with real categories.
BLUE, ORANGE, GREY = "#2a78d6", "#eb6834", "#9a9893"
INK, INK_MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
SEX_COLORS = {"Female": BLUE, "Male": ORANGE, "Unknown": GREY}
SERIOUS_COLORS = {"Serious": ORANGE, "Non-serious": BLUE}

plt.rcParams.update({
    "figure.dpi": 110, "savefig.dpi": 160, "font.size": 10,
    "axes.edgecolor": INK_MUTED, "axes.labelcolor": INK, "axes.titlecolor": INK,
    "axes.titlesize": 12, "axes.axisbelow": True, "axes.titleweight": "bold", "axes.spines.top": False,
    "axes.spines.right": False, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "legend.frameon": False,
})


def load_from_db() -> tuple[pd.DataFrame, pd.DataFrame]:
    with sqlite3.connect(config.DB_PATH) as conn:
        events = pd.read_sql_query("SELECT * FROM adverse_events", conn,
                                   parse_dates=["receive_date", "receipt_date", "transmission_date"])
        reactions = pd.read_sql_query("SELECT * FROM reactions", conn)
    return events, reactions


def describe_dataset(events: pd.DataFrame, reactions: pd.DataFrame) -> dict:
    print("\n========== EDA: DATASET OVERVIEW ==========")
    print(f"adverse_events shape: {events.shape} | reactions shape: {reactions.shape}")
    print("\nData types:\n" + events.dtypes.value_counts().to_string())
    numeric_cols = ["age_years", "patient_weight_kg", "num_drugs", "num_reactions",
                    "seriousness_criteria_count", "days_to_latest_info"]
    desc = events[numeric_cols].describe().round(2)
    print("\nDescriptive statistics:\n" + desc.to_string())
    desc.to_csv(config.DATA_DIR / "eda_descriptive_statistics.csv")

    categorical = {}
    for col in ["serious_label", "patient_sex", "age_group", "reporter_type", "report_type",
                "occur_country", "primary_drug_route"]:
        counts = events[col].value_counts()
        categorical[col] = {"distinct": int(counts.size), "top": counts.head(5).to_dict()}
        print(f"\n{col}: {counts.size} distinct | top: {counts.head(5).to_dict()}")

    stats = {
        "reports": len(events),
        "reaction_rows": len(reactions),
        "distinct_reactions": int(reactions["reaction_pt"].nunique()),
        "distinct_suspect_drugs": int(events["primary_suspect_drug"].nunique()),
        "serious_share_pct": round(100 * events["is_serious"].mean(), 1),
        "death_share_pct": round(100 * events["seriousness_death"].mean(), 1),
        "median_age_years": float(np.nanmedian(events["age_years"])) if events["age_years"].notna().any() else None,
        "mean_drugs_per_report": round(float(events["num_drugs"].mean()), 2),
        "polypharmacy_share_pct": round(100 * events["is_polypharmacy"].mean(), 1),
        "age_missing_pct": round(100 * events["age_years"].isna().mean(), 1),
        "weight_missing_pct": round(100 * events["patient_weight_kg"].isna().mean(), 1),
        "categorical": categorical,
    }
    return stats


def grouped_analysis(events: pd.DataFrame) -> dict:
    print("\n========== EDA: GROUPED ANALYSIS ==========")
    by_reporter = (events.groupby("reporter_type")
                   .agg(reports=("safety_report_id", "count"), serious_rate=("is_serious", "mean"),
                        mean_drugs=("num_drugs", "mean")).round(3).sort_values("reports", ascending=False))
    print("\nBy reporter type:\n" + by_reporter.to_string())
    by_poly = (events.groupby("is_polypharmacy")
               .agg(reports=("safety_report_id", "count"), serious_rate=("is_serious", "mean"),
                    death_rate=("seriousness_death", "mean")).round(3))
    print("\nPolypharmacy (5+ drugs) vs not:\n" + by_poly.to_string())
    by_reporter.to_csv(config.DATA_DIR / "eda_by_reporter_type.csv")
    by_poly.to_csv(config.DATA_DIR / "eda_by_polypharmacy.csv")
    return {"by_reporter_type": by_reporter.reset_index().to_dict(orient="records"),
            "by_polypharmacy": by_poly.reset_index().to_dict(orient="records")}


def _save(fig, name: str) -> str:
    path = config.FIGURES_DIR / name
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"[EDA] Figure saved -> figures/{name}")
    return name


def plot_top_reactions(reactions: pd.DataFrame, n_reports: int) -> dict:
    top = reactions["reaction_pt"].value_counts().head(15).sort_values()
    fig, ax = plt.subplots(figsize=(8, 5.5))
    ax.barh(top.index, top.values, color=BLUE, height=0.65)
    for y, v in enumerate(top.values):
        ax.text(v + 0.3, y, f"{v}", va="center", fontsize=8, color=INK_MUTED)
    ax.set_title("Q1. 15 most frequently reported adverse reactions")
    ax.set_xlabel("Number of reports mentioning the reaction")
    ax.set_ylabel("MedDRA preferred term")
    ax.grid(axis="y", visible=False)
    _save(fig, "fig1_top_reactions.png")
    return {"top_reactions": {k: int(v) for k, v in top.sort_values(ascending=False).items()},
            "top_reaction_share_pct": round(100 * top.max() / n_reports, 1)}


def plot_serious_rate_by_age_sex(events: pd.DataFrame) -> dict:
    data = events[(events["age_group"] != "Unknown") & (events["patient_sex"] != "Unknown")]
    pivot = pd.pivot_table(data, values="is_serious", index="age_group", columns="patient_sex",
                           aggfunc="mean").reindex([g for g in AGE_GROUP_LABELS if g in set(data["age_group"])])
    counts = pd.pivot_table(data, values="is_serious", index="age_group", columns="patient_sex",
                            aggfunc="count").reindex(pivot.index)
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(pivot.index))
    width = 0.38
    for i, sex in enumerate([s for s in ["Female", "Male"] if s in pivot.columns]):
        bars = ax.bar(x + (i - 0.5) * width, pivot[sex] * 100, width - 0.03,
                      color=SEX_COLORS[sex], label=sex)
        for bar, n in zip(bars, counts[sex]):
            if not np.isnan(bar.get_height()):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1, f"n={int(n)}",
                        ha="center", fontsize=7, color=INK_MUTED)
    ax.set_xticks(x, pivot.index, rotation=15)
    ax.set_ylim(0, 125)
    ax.set_yticks(range(0, 101, 20))
    ax.set_title("Q2. Share of serious reports by age group and sex (n = reports in the bar)")
    ax.set_xlabel("Patient age group")
    ax.set_ylabel("Serious reports (%)")
    ax.legend(title="Patient sex", loc="upper center", ncol=2, bbox_to_anchor=(0.5, 1.0))
    ax.grid(axis="x", visible=False)
    _save(fig, "fig2_serious_rate_by_age_sex.png")
    return {"serious_rate_pct_by_age_sex": (pivot * 100).round(1).to_dict()}


def plot_age_distribution(events: pd.DataFrame) -> dict:
    data = events.dropna(subset=["age_years"])
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    bins = np.arange(0, 105, 5)
    medians = {}
    for label in ["Non-serious", "Serious"]:
        ages = data.loc[data["serious_label"] == label, "age_years"]
        if ages.empty:
            continue
        medians[label] = float(ages.median())
        ax.hist(ages, bins=bins, histtype="step", linewidth=2, color=SERIOUS_COLORS[label],
                label=f"{label} (n={len(ages)}, median {ages.median():.0f} y)")
        ax.axvline(ages.median(), color=SERIOUS_COLORS[label], linestyle="--", linewidth=1)
    ax.set_title("Q3. Patient age distribution: serious vs non-serious reports")
    ax.set_xlabel("Patient age (years, 5-year bins)")
    ax.set_ylabel("Number of reports")
    ax.legend(loc="upper left")
    _save(fig, "fig3_age_distribution_by_seriousness.png")
    return {"median_age_by_seriousness": medians}


def plot_seriousness_criteria(events: pd.DataFrame) -> dict:
    flag_cols = [c for c in events.columns if c.startswith("seriousness_") and c != "seriousness_criteria_count"]
    long = events.melt(id_vars=["patient_sex"], value_vars=flag_cols, var_name="criterion", value_name="flag")
    long = long[long["flag"] == 1]
    long["criterion"] = long["criterion"].str.replace("seriousness_", "").str.replace("_", " ").str.capitalize()
    table = long.groupby(["criterion", "patient_sex"]).size().unstack(fill_value=0)
    table = table.loc[table.sum(axis=1).sort_values().index]
    fig, ax = plt.subplots(figsize=(8.5, 4.5))
    left = np.zeros(len(table))
    for sex in [s for s in ["Female", "Male", "Unknown"] if s in table.columns]:
        ax.barh(table.index, table[sex], left=left, color=SEX_COLORS[sex], label=sex,
                height=0.6, edgecolor="white", linewidth=1.5)
        left += table[sex].to_numpy()
    for y, total in enumerate(left):
        ax.text(total + 0.5, y, f"{int(total)}", va="center", fontsize=8, color=INK_MUTED)
    ax.set_title("Q4. Seriousness criteria among serious reports, by patient sex")
    ax.set_xlabel("Number of reports with the criterion")
    ax.set_ylabel("Seriousness criterion")
    ax.legend(title="Patient sex", loc="lower right")
    ax.grid(axis="y", visible=False)
    _save(fig, "fig4_seriousness_criteria.png")
    return {"seriousness_criteria_totals": {k: int(v) for k, v in table.sum(axis=1).items()}}


def plot_monthly_trend(events: pd.DataFrame) -> dict:
    """Monthly share of serious reports and of the two most severe criteria."""
    monthly = (events.groupby("receive_month")
               .agg(reports=("safety_report_id", "count"), serious=("is_serious", "mean"),
                    hospitalization=("seriousness_hospitalization", "mean"),
                    death=("seriousness_death", "mean"))
               .sort_index())
    fig, ax = plt.subplots(figsize=(9, 4.6))
    for column, label, color in [("serious", "Serious (any criterion)", ORANGE),
                                 ("hospitalization", "Hospitalization", BLUE),
                                 ("death", "Death", INK_MUTED)]:
        ax.plot(monthly.index, monthly[column] * 100, marker="o", markersize=5, linewidth=2,
                color=color, label=label)
    ax.set_title(f"Q5. Monthly share of serious reports ({int(monthly['reports'].median())} sampled reports per month)")
    ax.set_xlabel("Month the report was received by FDA (receivedate)")
    ax.set_ylabel("Share of the month's reports (%)")
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylim(0, 100)
    ax.legend(loc="upper left", ncol=3)
    _save(fig, "fig5_monthly_serious_share.png")
    return {"monthly": (monthly.assign(serious=monthly["serious"] * 100,
                                       hospitalization=monthly["hospitalization"] * 100,
                                       death=monthly["death"] * 100).round(1).to_dict(orient="index"))}


def run_analysis() -> dict:
    config.ensure_directories()
    events, reactions = load_from_db()
    summary = describe_dataset(events, reactions)
    summary["grouped"] = grouped_analysis(events)
    summary.update(plot_top_reactions(reactions, len(events)))
    summary.update(plot_serious_rate_by_age_sex(events))
    summary.update(plot_age_distribution(events))
    summary.update(plot_seriousness_criteria(events))
    summary.update(plot_monthly_trend(events))
    with open(config.DATA_DIR / "analysis_summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False, default=str)
    update_metrics("analysis", {"reports_analyzed": len(events), "figures_created": 5})
    return summary


if __name__ == "__main__":
    run_analysis()
