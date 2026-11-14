"""CI helpers: seed tiny raw tables, simulate a source change, assert results."""
import os
import sys
from datetime import datetime

import duckdb

DB = os.environ["DUCKDB_PATH"]

CUSTOMER_COLS = [
    "customer_id", "full_name", "email", "phone_number", "signup_date", "gender", "city",
    "region", "loyalty_tier", "preferred_payment", "age", "customer_segment",
    "acquisition_channel", "acquisition_campaign_id",
]
PRODUCT_COLS = [
    "product_id", "product_name", "category", "launch_date", "base_price", "unit_cost",
    "supplier_code", "country_of_origin", "is_active", "reorder_level",
    "current_stock_level", "weight_kg", "seasonality_tag",
]
SALES_TEXT_COLS = [
    "order_id", "line_item_id", "customer_id", "product_id", "quantity", "unit_price",
    "unit_cost", "order_date", "estimated_delivery", "actual_delivery", "delivery_status",
    "payment_method", "region", "discount_applied", "shipping_cost", "return_requested",
    "sales_channel", "notes", "campaign_id", "shipping_city", "warehouse_id",
    "order_priority", "return_reason", "cancellation_reason",
]

FCT = "marts.fct_sales_profitability"


def ddl(table, text_cols, ts_cols=()):
    cols = [f"{c} varchar" for c in text_cols] + [f"{c} timestamp" for c in ts_cols]
    return f"create or replace table raw_data.{table} ({', '.join(cols)})"


def insert(con, table, row):
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    con.execute(f"insert into raw_data.{table} ({cols}) values ({marks})", list(row.values()))


def sale(**kw):
    return kw


def seed():
    con = duckdb.connect(DB)
    con.execute("create schema if not exists raw_data")
    con.execute(ddl("customers_raw", CUSTOMER_COLS))
    con.execute(ddl("products_raw", PRODUCT_COLS))
    con.execute(ddl("sales_data_raw", SALES_TEXT_COLS, ["created_at", "updated_at"]))

    customers = [
        ("C000001", "Ada One", "ada@mail.com", "-8177968", "2-Nov-24", "Female", "Lagos",
         "West", "Gold", "Debit Card", "34", "Retail", "Google Ads", "CMP01"),
        ("C000002", "Bola Two", "", "-4854228", "23/08/2022", "MALE", "Kano",
         "North", "bronze", "CREDIT CARD", "52", "Online", "Organic", ""),
        ("C000003", "Chi Three", "chi@mail.com", "-2583470", "4/7/2025", "F", "Abuja",
         "Central", "Silver", "DebitCard", "29", "Walk-in", "Walk-in", ""),
    ]
    for c in customers:
        insert(con, "customers_raw", dict(zip(CUSTOMER_COLS, c)))

    products = [
        ("P00001", "Non-Stick Pan", "Kitchen", "4/5/2021", "122.82", "50.25", "S155",
         "Turkey", "Yes", "20", "97", "1.96", "Year-Round"),
        ("P00002", "Body Lotion", "Personal Care", "19/08/2021", "40.16", "17.88", "S178",
         "USA", "Yes", "75", "0", "3.19", "Year-Round"),
        ("P00003", "Portable Grill", "Outdoors", "9-Oct-23", "128.92", "65.13", "S169",
         "China", "No", "75", "260", "16.61", "Seasonal Peak: Apr"),
    ]
    for p in products:
        insert(con, "products_raw", dict(zip(PRODUCT_COLS, p)))

    sales = [
        # OL1: clean delivered line, revenue must be exactly 1589.62
        sale(order_id="O1", line_item_id="OL1", customer_id="C000001", product_id="P00001",
             quantity="1", unit_price="1589.62", unit_cost="889.12", order_date="1/1/2024",
             estimated_delivery="1/5/2024", actual_delivery="5-Jan-24",
             delivery_status="Delivered", payment_method="DEBIT CARD", region="North",
             discount_applied="0", shipping_cost="23.23", return_requested="No",
             sales_channel="In-Store", shipping_city="Kaduna", warehouse_id="WH002",
             order_priority="Standard", created_at=datetime(2024, 1, 1, 17, 14),
             updated_at=datetime(2024, 1, 5, 10, 58)),
        # OL2: second line of the same order (grain is the line, not the order)
        sale(order_id="O1", line_item_id="OL2", customer_id="C000001", product_id="P00002",
             quantity="2", unit_price="1221.45", unit_cost="666.38", order_date="1-Jan-24",
             estimated_delivery="6-Jan-24", actual_delivery="5-Jan-24",
             delivery_status="DELIVERED", payment_method="ussd", region="South",
             discount_applied="0.02", shipping_cost="0", return_requested="n",
             sales_channel="Mobile App", shipping_city="Calabar", warehouse_id="WH004",
             order_priority="Standard", created_at=datetime(2024, 1, 1, 18, 17),
             updated_at=datetime(2024, 1, 5, 17, 49)),
        # OL3: late delivery, typo status, return requested, ISO + dd/mm/yyyy dates
        sale(order_id="O2", line_item_id="OL3", customer_id="C000002", product_id="P00001",
             quantity="1", unit_price="1746.13", unit_cost="1107.76", order_date="2024-01-02",
             estimated_delivery="1/4/2024", actual_delivery="14/01/2024",
             delivery_status="Delayyed", payment_method="Bank transfer", region="West",
             discount_applied="0.15", shipping_cost="7.2", return_requested="Y",
             sales_channel="Online", shipping_city="Lagos", warehouse_id="WH001",
             order_priority="Standard", return_reason="Wrong item",
             created_at=datetime(2024, 1, 2, 13, 12),
             updated_at=datetime(2024, 1, 14, 11, 21)),
        # OL4: still pending (no actual delivery). The change step updates this row.
        sale(order_id="O3", line_item_id="OL4", customer_id="C000003", product_id="P00003",
             quantity="5", unit_price="200.82", unit_cost="104.03", order_date="3-Jan-2024",
             estimated_delivery="4-Jan-24", actual_delivery="",
             delivery_status="Pending", payment_method="PayPal", region="West",
             discount_applied="0.05", shipping_cost="5.77", return_requested="No",
             sales_channel="Phone", shipping_city="Lagos", warehouse_id="WH001",
             order_priority="Standard", created_at=datetime(2024, 1, 3, 9, 0),
             updated_at=datetime(2024, 1, 3, 9, 0)),
        # OL5: cancelled, recognized revenue must be 0
        sale(order_id="O4", line_item_id="OL5", customer_id="C000002", product_id="P00002",
             quantity="3", unit_price="100", unit_cost="40", order_date="5-Jan-24",
             estimated_delivery="8-Jan-24", actual_delivery="",
             delivery_status="Cancelled", payment_method="Cash", region="West",
             discount_applied="0", shipping_cost="0", return_requested="No",
             sales_channel="Online", shipping_city="Lagos", warehouse_id="WH001",
             order_priority="Standard", cancellation_reason="Customer request",
             created_at=datetime(2024, 1, 5, 8, 0), updated_at=datetime(2024, 1, 5, 9, 0)),
    ]
    for s in sales:
        insert(con, "sales_data_raw", s)
    con.close()
    print("seeded: 3 customers, 3 products, 5 sales lines")


def change():
    """Simulate Postgres: OL4 gets delivered and its updated_at moves forward."""
    con = duckdb.connect(DB)
    con.execute(
        "update raw_data.sales_data_raw set delivery_status = 'Delivered', "
        "actual_delivery = '4-Jan-24', updated_at = ? where line_item_id = 'OL4'",
        [datetime(2024, 2, 1, 10, 0)],
    )
    con.close()
    print("changed: OL4 -> Delivered")


def check():
    con = duckdb.connect(DB, read_only=True)
    errors = []

    def one(sql):
        return con.execute(sql).fetchone()[0]

    def expect(name, ok, detail=""):
        print(("PASS " if ok else "FAIL ") + name, detail)
        if not ok:
            errors.append(name)

    # 1. row counts and duplicates
    stg = one("select count(*) from staging.stg_sales")
    fct = one(f"select count(*) from {FCT}")
    dist = one(f"select count(distinct line_item_id) from {FCT}")
    expect("fact rows equal staging rows", fct == stg == 5, f"stg={stg} fct={fct}")
    expect("no duplicate line items after 3 runs", fct == dist, f"rows={fct} distinct={dist}")

    # 2. golden row and cancellation
    ol1 = float(one(f"select recognized_revenue from {FCT} where line_item_id = 'OL1'"))
    expect("OL1 revenue = 1589.62 (golden row)", abs(ol1 - 1589.62) < 0.01, str(ol1))

    ol5 = float(one(f"select recognized_revenue from {FCT} where line_item_id = 'OL5'"))
    expect("cancelled line OL5 has zero recognized revenue", ol5 == 0, str(ol5))

    # 3. the changed row was picked up
    status = one(f"select delivery_status from {FCT} where line_item_id = 'OL4'")
    expect("changed row OL4 shows new status", status == "Delivered", str(status))

    # 4. which rows were reprocessed in run 3
    newest_sql = (
        f"select line_item_id from {FCT} "
        f"where dbt_loaded_at = (select max(dbt_loaded_at) from {FCT})"
    )
    newest = {row[0] for row in con.execute(newest_sql).fetchall()}
    expect(
        "run 3 reprocessed the changed line plus only the lag-window overlap",
        newest == {"OL3", "OL4"},
        f"reprocessed = {sorted(newest)}",
    )

    untouched_sql = (
        f"select count(*) from {FCT} "
        f"where line_item_id in ('OL1', 'OL2', 'OL5') "
        f"and dbt_loaded_at < (select dbt_loaded_at from {FCT} where line_item_id = 'OL4')"
    )
    untouched = one(untouched_sql)
    expect("old lines OL1, OL2 and OL5 were NOT reprocessed", untouched == 3, f"untouched = {untouched}")

    # 5. layers agree
    rev_int = float(one("select sum(recognized_revenue) from intermediate.int_sales_enriched"))
    rev_fct = float(one(f"select sum(recognized_revenue) from {FCT}"))
    expect("intermediate and mart revenue reconcile", abs(rev_int - rev_fct) < 0.01,
           f"{rev_int} vs {rev_fct}")

    con.close()
    if errors:
        sys.exit(f"{len(errors)} check(s) failed: {errors}")


if __name__ == "__main__":
    {"seed": seed, "change": change, "check": check}[sys.argv[1]]()
