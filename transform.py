import re
import pandas as pd


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["case_owner", "case_owner_description", "created_year_month",
            "goal_days", "issue_description", "location_city"]
    return df.drop(columns=cols, errors="ignore")


def date_time_fix(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["ticket_created_date_time", "ticket__last_update_date_time", "ticket_closed_date_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")
    return df


def normalize_capitalization(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.title()
    for col in ["city", "ticket_status"]:
        if col in df.columns:
            df[col] = df[col].str.replace("_", " ")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset="ticket_id")


def validate_zipcode(series: pd.Series) -> pd.Series:
    def clean_zip(z):
        if pd.isna(z):
            return None
        digits = re.sub(r"\D", "", str(z).strip())
        return int(digits[:5]) if len(digits) >= 5 else None
    return series.apply(clean_zip).astype("Int64")


def fix_priority_typo(df: pd.DataFrame) -> pd.DataFrame:
    if "sr_priority" in df.columns:
        df["sr_priority"] = df["sr_priority"].str.replace(
            "Emergncy", "Emergency", regex=False)
    return df


def drop_null_required(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=["ticket_id"])


def fix_negative_days(df: pd.DataFrame) -> pd.DataFrame:
    if "actual_completed_days" in df.columns:
        df.loc[df["actual_completed_days"] < 0, "actual_completed_days"] = pd.NA
    return df


def validate_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    if "latitude" in df.columns and "longitude" in df.columns:
        invalid = (
            df["latitude"].notna() & df["longitude"].notna() & (
                (df["latitude"] < 24) | (df["latitude"] > 27) |
                (df["longitude"] < -82) | (df["longitude"] > -79)
            )
        )
        df.loc[invalid, ["latitude", "longitude"]] = pd.NA
    return df


def rename_update_column(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"ticket__last_update_date_time": "ticket_last_update_date_time"})


def transform(df: pd.DataFrame) -> pd.DataFrame:
    steps = [
        drop_columns,
        date_time_fix,
        normalize_capitalization,
        remove_duplicates,
        fix_priority_typo,
        drop_null_required,
        fix_negative_days,
        validate_coordinates,
        rename_update_column,
    ]
    for fn in steps:
        df = fn(df)
    df["zip_code"] = validate_zipcode(df["zip_code"])
    print(f"  Transformed: {len(df):,} rows")
    return df
