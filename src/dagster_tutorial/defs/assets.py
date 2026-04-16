import dagster as dg
import pandas as pd

from dagster_tutorial.defs.resources import SalesIO


REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "region",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "sales_rep",
]


@dg.asset
def raw_sales(context: dg.AssetExecutionContext, sales_io: SalesIO) -> pd.DataFrame:
    context.log.info(f"Reading CSV from: {sales_io.source_csv}")

    df = sales_io.read_sales_csv()
    context.log.info(f"Columns read: {df.columns.tolist()}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise dg.Failure(
            f"Reading CSV from: {sales_io.source_csv} | "
            f"Columns read: {df.columns.tolist()} | "
            f"Missing required columns: {missing}"
        )

    return df


@dg.asset
def clean_sales(raw_sales: pd.DataFrame, sales_io: SalesIO) -> pd.DataFrame:
    df = raw_sales.copy()

    df = df.drop_duplicates(subset=["order_id"])

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    df["region"] = df["region"].fillna("UNKNOWN").astype(str).str.strip().str.upper()
    df["customer_id"] = df["customer_id"].fillna("UNKNOWN").astype(str).str.strip()
    df["product_id"] = df["product_id"].fillna("UNKNOWN").astype(str).str.strip()
    df["sales_rep"] = df["sales_rep"].fillna("UNKNOWN").astype(str).str.strip()

    df = df.dropna(subset=["order_date", "quantity", "unit_price"])
    df = df[(df["quantity"] > 0) & (df["unit_price"] >= 0)]

    df["sales_amount"] = df["quantity"] * df["unit_price"]

    sales_io.write_csv("clean_sales", df)
    return df.reset_index(drop=True)


@dg.asset
def sales_by_region(clean_sales: pd.DataFrame, sales_io: SalesIO) -> pd.DataFrame:
    summary = (
        clean_sales.groupby("region", dropna=False)
        .agg(
            total_sales=("sales_amount", "sum"),
            total_orders=("order_id", "nunique"),
            unique_customers=("customer_id", "nunique"),
        )
        .reset_index()
        .sort_values("total_sales", ascending=False)
    )

    sales_io.write_csv("sales_by_region", summary)
    return summary


@dg.asset
def daily_metrics(
    clean_sales: pd.DataFrame,
    sales_by_region: pd.DataFrame,
    sales_io: SalesIO,
) -> pd.DataFrame:
    if sales_by_region.empty:
        top_region = "UNKNOWN"
        top_region_sales = 0.0
    else:
        top = sales_by_region.iloc[0]
        top_region = str(top["region"])
        top_region_sales = float(top["total_sales"])

    metrics = pd.DataFrame(
        [
            {
                "metric_date": clean_sales["order_date"].dt.date.max()
                if not clean_sales.empty
                else None,
                "total_sales": float(clean_sales["sales_amount"].sum()),
                "total_orders": int(clean_sales["order_id"].nunique()),
                "unique_customers": int(clean_sales["customer_id"].nunique()),
                "top_region_by_sales": top_region,
                "top_region_sales": top_region_sales,
            }
        ]
    )

    sales_io.write_csv("daily_metrics", metrics)
    sales_io.write_json("daily_metrics", metrics.iloc[0].to_dict())
    return metrics


@dg.asset_check(asset="clean_sales")
def clean_sales_quality_check(clean_sales: pd.DataFrame) -> dg.AssetCheckResult:
    duplicate_orders = int(clean_sales["order_id"].duplicated().sum())
    null_dates = int(clean_sales["order_date"].isna().sum())
    invalid_qty = int((clean_sales["quantity"] <= 0).sum())
    invalid_price = int((clean_sales["unit_price"] < 0).sum())
    invalid_sales = int((clean_sales["sales_amount"] < 0).sum())

    passed = (
        len(clean_sales) > 0
        and duplicate_orders == 0
        and null_dates == 0
        and invalid_qty == 0
        and invalid_price == 0
        and invalid_sales == 0
    )

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "row_count": int(len(clean_sales)),
            "duplicate_orders": duplicate_orders,
            "null_dates": null_dates,
            "invalid_qty": invalid_qty,
            "invalid_price": invalid_price,
            "invalid_sales": invalid_sales,
        },
    )


@dg.asset_check(asset="daily_metrics")
def daily_metrics_business_rule_check(daily_metrics: pd.DataFrame) -> dg.AssetCheckResult:
    row = daily_metrics.iloc[0]

    total_sales = float(row["total_sales"])
    total_orders = int(row["total_orders"])
    unique_customers = int(row["unique_customers"])
    top_region = str(row["top_region_by_sales"])

    passed = (
        total_sales >= 0
        and total_orders >= 0
        and unique_customers >= 0
        and ((unique_customers <= total_orders) if total_orders > 0 else unique_customers == 0)
        and ((top_region not in {"", "NAN"}) if total_orders > 0 else True)
    )

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "total_sales": total_sales,
            "total_orders": total_orders,
            "unique_customers": unique_customers,
            "top_region_by_sales": top_region,
        },
    )