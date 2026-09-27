"""Smoke test: proves the local stack supports the Iceberg features the design relies on
(MERGE INTO, branches, fast-forward publish). If this fails, nothing else is meaningful."""


def test_iceberg_merge_and_branch_publish(spark, namespace):
    table = f"{namespace}.accounts"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, balance_paise BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1, 100), (2, 200)")

    spark.sql(f"""
        MERGE INTO {table} t
        USING (SELECT 1 AS id, 150L AS balance_paise) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.balance_paise = s.balance_paise
    """)

    spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
    spark.sql(f"INSERT INTO {table}.branch_audit VALUES (3, 300)")
    assert spark.table(table).count() == 2, "branch write must not be visible on main"

    short_name = table.removeprefix("lake.")
    spark.sql(f"CALL lake.system.fast_forward('{short_name}', 'main', 'audit')")

    rows = {r.id: r.balance_paise for r in spark.table(table).collect()}
    assert rows == {1: 150, 2: 200, 3: 300}
