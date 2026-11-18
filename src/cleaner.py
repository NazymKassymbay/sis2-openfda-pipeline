import re

import numpy as np
import pandas as pd

from src import config
from src.metrics import update_metrics

SEX_LABELS = {"0": "Unknown", "1": "Male", "2": "Female"}
SERIOUS_LABELS = {"1": "Serious", "2": "Non-serious"}
QUALIFICATION_LABELS = {"1": "Physician", "2": "Pharmacist", "3": "Other health professional",
                        "4": "Lawyer", "5": "Consumer"}
REPORT_TYPE_LABELS = {"1": "Spontaneous", "2": "Study", "3": "Other", "4": "Not available to sender"}
OUTCOME_LABELS = {"1": "Recovered", "2": "Recovering", "3": "Not recovered",
                  "4": "Recovered with sequelae", "5": "Fatal", "6": "Unknown"}
SERIOUSNESS_COLUMNS = ["seriousness_death", "seriousness_hospitalization",
                       "seriousness_life_threatening", "seriousness_disabling",
                       "seriousness_congenital_anomaly", "seriousness_other"]
AGE_GROUP_BINS = [0, 2, 12, 18, 45, 65, 121]
AGE_GROUP_LABELS = ["Infant (<2)", "Child (2-11)", "Adolescent (12-17)",
                    "Adult (18-44)", "Middle-aged (45-64)", "Elderly (65+)"]


def normalize_drug_name(value) -> str:
    """'  Adalimumab. ' -> 'ADALIMUMAB'; empty -> 'UNKNOWN'."""
    if not isinstance(value, str) or not value.strip():
        return "UNKNOWN"
    text = re.sub(r"\s+", " ", value).strip().rstrip(".,;").upper()
    return text or "UNKNOWN"


def normalize_term(value) -> str:
    """MedDRA terms / indications: trimmed, single spaces, sentence case."""
    if not isinstance(value, str) or not value.strip():
        return "Unknown"
    text = re.sub(r"\s+", " ", value).strip().rstrip(".")
    return text[:1].upper() + text[1:].lower()


def analyze_missing(df: pd.DataFrame) -> pd.DataFrame:
    report = pd.DataFrame({
        "missing_count": df.isna().sum(),
        "missing_percent": (df.isna().mean() * 100).round(2),
    })
    return report[report["missing_count"] > 0].sort_values("missing_count", ascending=False)


def clean_reports(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = valid_df.copy()
    info: dict = {"rows_in": len(df)}

    # 1) Missing-value analysis (before any change)
    missing = analyze_missing(df.drop(columns=["reactions"]))
    missing.to_csv(config.DATA_DIR / "transform_missing_values.csv", index_label="column")
    info["missing_before"] = missing["missing_count"].to_dict()

    # 2) Duplicate detection and removal
    df["report_version"] = pd.to_numeric(df["report_version"], errors="coerce").fillna(1).astype(int)
    info["duplicate_ids_found"] = int(df["safety_report_id"].duplicated().sum())
    df = (df.sort_values(["safety_report_id", "report_version"])
            .drop_duplicates(subset="safety_report_id", keep="last")  # keep latest version
            .reset_index(drop=True))
    info["rows_after_dedup"] = len(df)

    # 3) Type conversion
    for col in ["receive_date", "receipt_date", "transmission_date"]:
        df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce")
    for col in ["num_drugs", "num_suspect_drugs", "num_reactions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["patient_age"] = pd.to_numeric(df["patient_age"], errors="coerce")
    df["patient_weight_kg"] = pd.to_numeric(df["patient_weight_kg"], errors="coerce")
    # openFDA omits a seriousness flag when it does not apply -> missing means 0
    for col in SERIOUSNESS_COLUMNS:
        df[col] = (df[col] == "1").astype(int)

    # 4) Text standardization and code -> label mapping
    df["primary_suspect_drug"] = df["primary_suspect_drug"].map(normalize_drug_name)
    df["primary_drug_indication"] = df["primary_drug_indication"].map(normalize_term)
    df["primary_drug_route"] = df["primary_drug_route"].fillna("UNKNOWN").str.strip().str.upper()
    for col in ["occur_country", "primary_source_country"]:
        df[col] = df[col].fillna("UNKNOWN").str.strip().str.upper()
    df["patient_sex"] = df["patient_sex"].map(SEX_LABELS).fillna("Unknown")
    df["serious_label"] = df["serious"].map(SERIOUS_LABELS)
    df["is_serious"] = (df["serious"] == "1").astype(int)
    df["reporter_type"] = df["reporter_qualification"].map(QUALIFICATION_LABELS).fillna("Unknown")
    df["report_type"] = df["report_type"].map(REPORT_TYPE_LABELS).fillna("Unknown")

    # 5) NumPy numerical operations
    unit_factor = np.select(
        [df["patient_age_unit"] == "800", df["patient_age_unit"] == "801",
         df["patient_age_unit"] == "802", df["patient_age_unit"] == "803",
         df["patient_age_unit"] == "804", df["patient_age_unit"] == "805"],
        [10.0, 1.0, 1 / 12, 1 / 52.1775, 1 / 365.25, 1 / 8766],
        default=np.nan,
    )
    df["age_years"] = np.round(df["patient_age"].to_numpy(dtype=float) * unit_factor, 2)
    age = df["age_years"].to_numpy(dtype=float)
    df["age_zscore"] = np.round((age - np.nanmean(age)) / np.nanstd(age), 3)
    df["log_num_drugs"] = np.round(np.log1p(df["num_drugs"].to_numpy(dtype=float)), 4)
    df["is_polypharmacy"] = np.where(df["num_drugs"].to_numpy(dtype=float) >= 5, 1, 0)
    df["seriousness_criteria_count"] = np.sum(df[SERIOUSNESS_COLUMNS].to_numpy(), axis=1)
    df["days_to_latest_info"] = (df["receipt_date"] - df["receive_date"]).dt.days
    df["age_group"] = pd.cut(df["age_years"], bins=AGE_GROUP_BINS, labels=AGE_GROUP_LABELS,
                             right=False).astype("string").fillna("Unknown")
    df["receive_month"] = df["receive_date"].dt.to_period("M").astype(str)

    # Missing numeric demographics are NOT imputed: inventing an age or weight
    # for a patient would bias safety signals. They are flagged instead.
    df["age_missing"] = df["age_years"].isna().astype(int)
    df["weight_missing"] = df["patient_weight_kg"].isna().astype(int)

    info["rows_out"] = len(df)
    info["age_missing_after"] = int(df["age_missing"].sum())
    info["weight_missing_after"] = int(df["weight_missing"].sum())
    return df, info


def build_reactions_table(df: pd.DataFrame) -> pd.DataFrame:
    """Explode the nested reaction list into a relational child table."""
    exploded = df[["safety_report_id", "reactions"]].explode("reactions").dropna(subset=["reactions"])
    reactions = pd.DataFrame({
        "safety_report_id": exploded["safety_report_id"].to_numpy(),
        "reaction_pt": [normalize_term(r.get("reaction_pt")) for r in exploded["reactions"]],
        "reaction_outcome": [OUTCOME_LABELS.get(r.get("reaction_outcome"), "Unknown")
                             for r in exploded["reactions"]],
    })
    return reactions.drop_duplicates(subset=["safety_report_id", "reaction_pt"]).reset_index(drop=True)


def run_transformations(df: pd.DataFrame, reactions: pd.DataFrame) -> dict:
    """Filtering, GroupBy aggregation and reshaping on the cleaned data."""
    results: dict = {}

    # Filtering with multiple conditions: serious reports in elderly patients on 5+ drugs
    high_risk = df[(df["is_serious"] == 1) & (df["age_years"] >= 65) & (df["is_polypharmacy"] == 1)]
    high_risk.to_csv(config.DATA_DIR / "transform_filter_elderly_serious_polypharmacy.csv", index=False)
    results["filter_elderly_serious_polypharmacy"] = len(high_risk)

    # GroupBy with several aggregation functions
    by_group = (df.groupby(["age_group", "patient_sex"], observed=True)
                  .agg(reports=("safety_report_id", "count"),
                       serious_rate=("is_serious", "mean"),
                       deaths=("seriousness_death", "sum"),
                       mean_drugs=("num_drugs", "mean"),
                       median_age=("age_years", "median"))
                  .round(3).reset_index())
    by_group.to_csv(config.DATA_DIR / "transform_groupby_age_sex.csv", index=False)

    by_drug = (df[df["primary_suspect_drug"] != "UNKNOWN"]
               .groupby("primary_suspect_drug")
               .agg(reports=("safety_report_id", "count"),
                    serious_rate=("is_serious", "mean"),
                    mean_age=("age_years", "mean"))
               .query("reports >= 3").sort_values("reports", ascending=False).round(3))
    by_drug.to_csv(config.DATA_DIR / "transform_groupby_drug.csv")
    results["drugs_with_3plus_reports"] = len(by_drug)

    # Reshaping 1: pivot_table (serious-report rate, age group x sex)
    pivot = pd.pivot_table(df, values="is_serious", index="age_group", columns="patient_sex",
                           aggfunc="mean").round(3)
    pivot.to_csv(config.DATA_DIR / "transform_pivot_serious_rate.csv")

    # Reshaping 2: melt (wide seriousness flags -> long format)
    long_flags = df.melt(id_vars=["safety_report_id", "patient_sex"], value_vars=SERIOUSNESS_COLUMNS,
                         var_name="criterion", value_name="flag")
    criterion_counts = (long_flags[long_flags["flag"] == 1]
                        .assign(criterion=lambda d: d["criterion"].str.replace("seriousness_", ""))
                        .groupby(["criterion", "patient_sex"]).size().unstack(fill_value=0))
    criterion_counts.to_csv(config.DATA_DIR / "transform_melt_seriousness_by_sex.csv")

    results["reaction_rows"] = len(reactions)
    results["distinct_reactions"] = int(reactions["reaction_pt"].nunique())
    return results


def run_cleaning(valid_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    clean_df, info = clean_reports(valid_df)
    reactions = build_reactions_table(clean_df)
    transform_info = run_transformations(clean_df, reactions)

    clean_df = clean_df.drop(columns=["reactions", "is_test_record"], errors="ignore")
    clean_df.to_csv(config.CLEAN_CSV_PATH, index=False)
    reactions.to_csv(config.CLEAN_REACTIONS_PATH, index=False)

    update_metrics("cleaning", {**info, **transform_info})
    print(f"[CLEAN] Rows in                  : {info['rows_in']}")
    print(f"[CLEAN] Duplicate ids removed    : {info['duplicate_ids_found']}")
    print(f"[CLEAN] Rows after cleaning      : {info['rows_out']}")
    print(f"[CLEAN] Reaction rows (child tbl): {len(reactions)}")
    print(f"[CLEAN] Elderly+serious+5+ drugs : {transform_info['filter_elderly_serious_polypharmacy']}")
    return clean_df, reactions


if __name__ == "__main__":
    from src.consumer import load_consumed_records
    from src.validator import run_validation

    run_cleaning(run_validation(load_consumed_records()))
