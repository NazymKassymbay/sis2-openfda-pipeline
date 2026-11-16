"""Stage 3 - Data validation with Pandera.

The records received from Kafka are validated *before* cleaning.
Strategy for invalid data: QUARANTINE - every row that breaks at least one
rule is written to data/invalid_records.csv together with its error messages
and is excluded from the following stages. Valid rows continue.

Deliberately invalid test records (is_test_record = True) are appended to
prove that each rule really detects errors. They are always excluded from the
final processed dataset.
"""

import json
from datetime import date

import pandas as pd
from pandera.errors import SchemaErrors
from pandera.pandas import Check, Column, DataFrameSchema

from src import config
from src.metrics import update_metrics

TODAY = pd.Timestamp(date.today())
EARLIEST_FAERS_DATE = pd.Timestamp("1968-01-01")  # FAERS/AERS history start

# Factors that convert openFDA age units to years (E2B code list).
AGE_UNIT_TO_YEARS = {"800": 10.0, "801": 1.0, "802": 1 / 12, "803": 1 / 52.1775,
                     "804": 1 / 365.25, "805": 1 / 8766}


def _is_real_date(series: pd.Series) -> pd.Series:
    """Element-wise: YYYYMMDD string that is a real calendar date in range."""
    parsed = pd.to_datetime(series, format="%Y%m%d", errors="coerce")
    return parsed.between(EARLIEST_FAERS_DATE, TODAY) | series.isna()


def _is_number_or_missing(series: pd.Series) -> pd.Series:
    """Element-wise data-type rule: value is missing or parses as a number."""
    return series.isna() | pd.to_numeric(series, errors="coerce").notna()


def _numeric_between(low: float, high: float, low_inclusive: bool = True):
    """Range rule applied to the numeric value of a text field (missing passes)."""
    def check(series: pd.Series) -> pd.Series:
        values = pd.to_numeric(series, errors="coerce")
        above_low = values >= low if low_inclusive else values > low
        return values.isna() | (above_low & (values <= high))
    return check


def _receipt_not_before_receive(df: pd.DataFrame) -> pd.Series:
    receive = pd.to_datetime(df["receive_date"], format="%Y%m%d", errors="coerce")
    receipt = pd.to_datetime(df["receipt_date"], format="%Y%m%d", errors="coerce")
    return receipt.isna() | receive.isna() | (receipt >= receive)


def _seriousness_consistent(df: pd.DataFrame) -> pd.Series:
    """A non-serious report (serious == '2') must not carry seriousness flags."""
    flag_cols = [c for c in df.columns if c.startswith("seriousness_")]
    any_flag = (df[flag_cols] == "1").any(axis=1)
    return ~((df["serious"] == "2") & any_flag)


def _age_in_years_plausible(df: pd.DataFrame) -> pd.Series:
    factor = df["patient_age_unit"].map(AGE_UNIT_TO_YEARS)
    age_years = pd.to_numeric(df["patient_age"], errors="coerce") * factor
    return age_years.isna() | (age_years <= 120)


def severity_flag() -> Column:
    return Column(str, Check.isin(["1", "2"]), nullable=True)

adverse_event_schema = DataFrameSchema(
    columns={
        # R1 - required unique identifier with the openFDA format
        "safety_report_id": Column(
            str, [Check.str_matches(r"^\d+(-\d+)?$", error="id_format")],
            nullable=False, unique=False,
        ),
        # R2 - date/time validation (format, real calendar date, plausible range)
        "receive_date": Column(
            str, [Check.str_matches(r"^\d{8}$", error="date_format"),
                  Check(_is_real_date, error="date_out_of_range_or_invalid")],
            nullable=False,
        ),
        "receipt_date": Column(
            str, [Check(_is_real_date, error="date_out_of_range_or_invalid")], nullable=True,
        ),
        # R3 - allowed categorical values (E2B code lists)
        "serious": Column(str, Check.isin(["1", "2"]), nullable=False),
        "patient_sex": Column(str, Check.isin(["0", "1", "2"]), nullable=True),
        "patient_age_unit": Column(str, Check.isin(list(AGE_UNIT_TO_YEARS)), nullable=True),
        "reporter_qualification": Column(str, Check.isin(["1", "2", "3", "4", "5"]), nullable=True),
        "report_type": Column(str, Check.isin(["1", "2", "3", "4"]), nullable=True),
        "seriousness_death": severity_flag(),
        "seriousness_hospitalization": severity_flag(),
        "seriousness_life_threatening": severity_flag(),
        "seriousness_disabling": severity_flag(),
        "seriousness_congenital_anomaly": severity_flag(),
        "seriousness_other": severity_flag(),
        # R4 - data type (text from JSON must be numeric) + numerical ranges
        "patient_age": Column(
            object, [Check(_is_number_or_missing, error="age_not_numeric"),
                     Check(_numeric_between(0, 1e6), error="age_negative")],
            nullable=True,
        ),
        "patient_weight_kg": Column(
            object, [Check(_is_number_or_missing, error="weight_not_numeric"),
                     Check(_numeric_between(0, 400, low_inclusive=False), error="weight_out_of_range_0_400kg")],
            nullable=True,
        ),
        # R5 - every report must describe at least one drug and one reaction
        "num_drugs": Column(int, Check.ge(1, error="no_drugs"), coerce=True),
        "num_reactions": Column(int, Check.ge(1, error="no_reactions"), coerce=True),
    },
    # R6-R8 - custom cross-field conditions
    checks=[
        Check(_receipt_not_before_receive, error="receipt_date_before_receive_date"),
        Check(_seriousness_consistent, error="non_serious_report_has_seriousness_flag"),
        Check(_age_in_years_plausible, error="age_above_120_years"),
    ],
    strict=False,  # nested 'reactions' list and descriptive text are not validated here
    name="openfda_adverse_event_schema",
)


def build_invalid_test_records() -> list[dict]:
    """Four deliberately broken records, clearly marked as test data."""
    base = {
        "report_version": "1", "transmission_date": "20240601", "report_type": "1",
        "seriousness_death": None, "seriousness_hospitalization": None,
        "seriousness_life_threatening": None, "seriousness_disabling": None,
        "seriousness_congenital_anomaly": None, "seriousness_other": None,
        "occur_country": "US", "primary_source_country": "US", "reporter_qualification": "1",
        "patient_age_unit": "801", "num_suspect_drugs": 1, "primary_suspect_drug": "TEST DRUG",
        "primary_drug_indication": "TEST", "primary_drug_route": "ORAL",
        "reactions": [{"reaction_pt": "Test reaction", "reaction_outcome": "1"}],
        "is_test_record": True,
    }
    return [
        {**base, "safety_report_id": "990000001", "receive_date": "20240510", "receipt_date": "20240510",
         "serious": "3", "patient_sex": "7", "patient_age": "40", "patient_weight_kg": "70",
         "num_drugs": 1, "num_reactions": 1},                          # invalid categories
        {**base, "safety_report_id": "990000002", "receive_date": "20240510", "receipt_date": "20240510",
         "serious": "1", "patient_sex": "1", "patient_age": "-5", "patient_weight_kg": "0",
         "num_drugs": 1, "num_reactions": 0, "seriousness_other": "1"},  # negative age, 0 kg, no reactions
        {**base, "safety_report_id": "990000003", "receive_date": "20241345", "receipt_date": "20240101",
         "serious": "2", "patient_sex": "2", "patient_age": "35", "patient_weight_kg": "60",
         "num_drugs": 1, "num_reactions": 1, "seriousness_death": "1"},  # bad date, inconsistent flags
        {**base, "safety_report_id": None, "receive_date": "20240510", "receipt_date": "20240510",
         "serious": "1", "patient_sex": "1", "patient_age": "200", "patient_weight_kg": "abc",
         "num_drugs": 1, "num_reactions": 1, "seriousness_hospitalization": "1"},  # missing id, 200 y, text weight
    ]


def split_valid_invalid(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Validate lazily (collect *all* errors) and split rows into valid/invalid."""
    try:
        validated = adverse_event_schema.validate(df, lazy=True)
        empty_errors = pd.DataFrame(columns=["row_index", "column", "check", "failure_case"])
        return validated, df.iloc[0:0], empty_errors
    except SchemaErrors as exc:
        failures = exc.failure_cases.copy()

    row_failures = failures[failures["index"].notna()].copy()
    table_failures = failures[failures["index"].isna()]
    if not table_failures.empty:
        # Structural problems (e.g. a missing column) cannot be fixed per row.
        raise ValueError(f"Schema-level validation failure:\n{table_failures}")

    row_failures["index"] = row_failures["index"].astype(int)
    # Row-level (cross-field) checks are reported once per cell by Pandera;
    # keep one entry per row and rule.
    is_row_check = row_failures["schema_context"] == "DataFrameSchema"
    row_failures.loc[is_row_check, "column"] = "<cross-field>"
    row_failures.loc[is_row_check, "failure_case"] = None
    errors = (row_failures.rename(columns={"index": "row_index"})
              [["row_index", "column", "check", "failure_case"]]
              .drop_duplicates(subset=["row_index", "column", "check"])
              .reset_index(drop=True))
    invalid_idx = sorted(errors["row_index"].unique())

    invalid_df = df.loc[invalid_idx].copy()
    messages = (
        errors.assign(msg=errors["column"].astype(str) + ": " + errors["check"].astype(str))
        .groupby("row_index")["msg"].apply(lambda s: "; ".join(sorted(set(s))))
    )
    invalid_df["validation_errors"] = messages

    valid_df = adverse_event_schema.validate(df.drop(index=invalid_idx), lazy=True)
    return valid_df, invalid_df, errors


def run_validation(records: list[dict]) -> pd.DataFrame:
    real_df = pd.DataFrame(records)
    real_df["is_test_record"] = False
    test_df = pd.DataFrame(build_invalid_test_records())
    df = pd.concat([real_df, test_df], ignore_index=True)

    valid_df, invalid_df, errors = split_valid_invalid(df)

    # Test records must never reach the final dataset, even if they passed.
    valid_df = valid_df[~valid_df["is_test_record"].astype(bool)].copy()

    invalid_out = invalid_df.copy()
    invalid_out["reactions"] = invalid_out["reactions"].apply(lambda v: json.dumps(v, ensure_ascii=False))
    invalid_out.to_csv(config.INVALID_RECORDS_PATH, index_label="row_index")
    errors.to_csv(config.VALIDATION_ERRORS_PATH, index=False)
    valid_out = valid_df.copy()
    valid_out["reactions"] = valid_out["reactions"].apply(lambda v: json.dumps(v, ensure_ascii=False))
    valid_out.to_csv(config.VALID_RECORDS_PATH, index=False)

    is_test = invalid_df["is_test_record"].astype(bool)
    errors_by_rule = errors.groupby(["column", "check"]).size().sort_values(ascending=False)
    summary = {
        "records_received": len(real_df),
        "test_records_added": len(test_df),
        "rows_validated": len(df),
        "valid_real_records": len(valid_df),
        "invalid_real_records": int((~is_test).sum()),
        "invalid_test_records_detected": int(is_test.sum()),
        "total_rule_violations": len(errors),
        "violations_by_rule": {f"{c} | {k}": int(v) for (c, k), v in errors_by_rule.items()},
    }
    update_metrics("validation", summary)

    print(f"[VALIDATE] Rows validated (real + test)  : {len(df)}")
    print(f"[VALIDATE] Valid real records            : {len(valid_df)}")
    print(f"[VALIDATE] Invalid real records          : {summary['invalid_real_records']}")
    print(f"[VALIDATE] Test records detected invalid : {summary['invalid_test_records_detected']}/{len(test_df)}")
    print("[VALIDATE] Violations by rule:")
    for rule, count in summary["violations_by_rule"].items():
        print(f"           {count:>4}  {rule}")
    return valid_df


if __name__ == "__main__":
    from src.consumer import load_consumed_records

    run_validation(load_consumed_records())
