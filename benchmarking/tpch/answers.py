from __future__ import annotations

import datetime
from typing import Callable

from daft import DataFrame, col

GetDFFunc = Callable[[str], DataFrame]


def q1(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")

    discounted_price = col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))
    taxed_discounted_price = discounted_price * (1 + col("L_TAX"))
    daft_df = (
        lineitem.where(col("L_SHIPDATE") <= datetime.date(1998, 9, 2))
        .groupby(col("L_RETURNFLAG"), col("L_LINESTATUS"))
        .agg(
            [
                (col("L_QUANTITY").alias("sum_qty"), "sum"),
                (col("L_EXTENDEDPRICE").alias("sum_base_price"), "sum"),
                (discounted_price.alias("sum_disc_price"), "sum"),
                (taxed_discounted_price.alias("sum_charge"), "sum"),
                (col("L_QUANTITY").alias("avg_qty"), "mean"),
                (col("L_EXTENDEDPRICE").alias("avg_price"), "mean"),
                (col("L_DISCOUNT").alias("avg_disc"), "mean"),
                (col("L_QUANTITY").alias("count_order"), "count"),
            ]
        )
        .sort(["L_RETURNFLAG", "L_LINESTATUS"])
    )
    return daft_df


def q2(get_df: GetDFFunc) -> DataFrame:
    region = get_df("region")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    part = get_df("part")

    europe = (
        region.where(col("R_NAME") == "EUROPE")
        .join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(partsupp, left_on=col("S_SUPPKEY"), right_on=col("PS_SUPPKEY"))
    )

    brass = part.where((col("P_SIZE") == 15) & col("P_TYPE").str.endswith("BRASS")).join(
        europe,
        left_on=col("P_PARTKEY"),
        right_on=col("PS_PARTKEY"),
    )
    min_cost = brass.groupby(col("P_PARTKEY")).agg(
        [
            (col("PS_SUPPLYCOST").alias("min"), "min"),
        ]
    )

    daft_df = (
        brass.join(min_cost, on=col("P_PARTKEY"))
        .where(col("PS_SUPPLYCOST") == col("min"))
        .select(
            col("S_ACCTBAL"),
            col("S_NAME"),
            col("N_NAME"),
            col("P_PARTKEY"),
            col("P_MFGR"),
            col("S_ADDRESS"),
            col("S_PHONE"),
            col("S_COMMENT"),
        )
        .sort(by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"], desc=[True, False, False, False])
        .limit(100)
    )
    return daft_df


def q3(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    customer = get_df("customer").where(col("C_MKTSEGMENT") == "BUILDING")
    orders = get_df("orders").where(col("O_ORDERDATE") < datetime.date(1995, 3, 15))
    lineitem = get_df("lineitem").where(col("L_SHIPDATE") > datetime.date(1995, 3, 15))

    daft_df = (
        customer.join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("O_ORDERKEY"), col("O_ORDERDATE"), col("O_SHIPPRIORITY"))
        .join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_ORDERKEY"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
            col("O_ORDERDATE"),
            col("O_SHIPPRIORITY"),
        )
        .groupby(col("O_ORDERKEY"), col("O_ORDERDATE"), col("O_SHIPPRIORITY"))
        .agg([(col("volume").alias("revenue"), "sum")])
        .sort(by=["revenue", "O_ORDERDATE"], desc=[True, False])
        .limit(10)
        .select("O_ORDERKEY", "revenue", "O_ORDERDATE", "O_SHIPPRIORITY")
    )
    return daft_df


def q4(get_df: GetDFFunc) -> DataFrame:
    orders = get_df("orders")
    lineitems = get_df("lineitem")

    orders = orders.where(
        (col("O_ORDERDATE") >= datetime.date(1993, 7, 1)) & (col("O_ORDERDATE") < datetime.date(1993, 10, 1))
    )

    lineitems = lineitems.where(col("L_COMMITDATE") < col("L_RECEIPTDATE")).select(col("L_ORDERKEY")).distinct()

    daft_df = (
        lineitems.join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .groupby(col("O_ORDERPRIORITY"))
        .agg([(col("L_ORDERKEY").alias("order_count"), "count")])
        .sort(col("O_ORDERPRIORITY"))
    )
    return daft_df


def q5(get_df: GetDFFunc) -> DataFrame:
    orders = get_df("orders").where(
        (col("O_ORDERDATE") >= datetime.date(1994, 1, 1)) & (col("O_ORDERDATE") < datetime.date(1995, 1, 1))
    )
    region = get_df("region").where(col("R_NAME") == "ASIA")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    daft_df = (
        region.join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(lineitem, left_on=col("S_SUPPKEY"), right_on=col("L_SUPPKEY"))
        .select(col("N_NAME"), col("L_EXTENDEDPRICE"), col("L_DISCOUNT"), col("L_ORDERKEY"), col("N_NATIONKEY"))
        .join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .join(customer, left_on=[col("O_CUSTKEY"), col("N_NATIONKEY")], right_on=[col("C_CUSTKEY"), col("C_NATIONKEY")])
        .select(col("N_NAME"), (col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("value"))
        .groupby(col("N_NAME"))
        .agg([(col("value").alias("revenue"), "sum")])
        .sort(col("revenue"), desc=True)
    )
    return daft_df


def q6(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")
    daft_df = lineitem.where(
        (col("L_SHIPDATE") >= datetime.date(1994, 1, 1))
        & (col("L_SHIPDATE") < datetime.date(1995, 1, 1))
        & (col("L_DISCOUNT") >= 0.05)
        & (col("L_DISCOUNT") <= 0.07)
        & (col("L_QUANTITY") < 24)
    ).sum(col("L_EXTENDEDPRICE") * col("L_DISCOUNT"))
    return daft_df


def q7(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(
        (col("L_SHIPDATE") >= datetime.date(1995, 1, 1)) & (col("L_SHIPDATE") <= datetime.date(1996, 12, 31))
    )
    nation = get_df("nation").where((col("N_NAME") == "FRANCE") | (col("N_NAME") == "GERMANY"))
    supplier = get_df("supplier")
    customer = get_df("customer")
    orders = get_df("orders")

    supNation = (
        nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(lineitem, left_on=col("S_SUPPKEY"), right_on=col("L_SUPPKEY"))
        .select(
            col("N_NAME").alias("supp_nation"),
            col("L_ORDERKEY"),
            col("L_EXTENDEDPRICE"),
            col("L_DISCOUNT"),
            col("L_SHIPDATE"),
        )
    )

    daft_df = (
        nation.join(customer, left_on=col("N_NATIONKEY"), right_on=col("C_NATIONKEY"))
        .join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("N_NAME").alias("cust_nation"), col("O_ORDERKEY"))
        .join(supNation, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .where(
            ((col("supp_nation") == "FRANCE") & (col("cust_nation") == "GERMANY"))
            | ((col("supp_nation") == "GERMANY") & (col("cust_nation") == "FRANCE"))
        )
        .select(
            col("supp_nation"),
            col("cust_nation"),
            col("L_SHIPDATE").dt.year().alias("l_year"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
        )
        .groupby(col("supp_nation"), col("cust_nation"), col("l_year"))
        .agg([(col("volume").alias("revenue"), "sum")])
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )
    return daft_df


def q8(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    region = get_df("region").where(col("R_NAME") == "AMERICA")
    orders = get_df("orders").where(
        (col("O_ORDERDATE") <= datetime.date(1996, 12, 31)) & (col("O_ORDERDATE") >= datetime.date(1995, 1, 1))
    )
    part = get_df("part").where(col("P_TYPE") == "ECONOMY ANODIZED STEEL")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    nat = nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))

    line = (
        lineitem.select(
            col("L_PARTKEY"),
            col("L_SUPPKEY"),
            col("L_ORDERKEY"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
        )
        .join(part, left_on=col("L_PARTKEY"), right_on=col("P_PARTKEY"))
        .join(nat, left_on=col("L_SUPPKEY"), right_on=col("S_SUPPKEY"))
    )

    daft_df = (
        nation.join(region, left_on=col("N_REGIONKEY"), right_on=col("R_REGIONKEY"))
        .select(col("N_NATIONKEY"))
        .join(customer, left_on=col("N_NATIONKEY"), right_on=col("C_NATIONKEY"))
        .select(col("C_CUSTKEY"))
        .join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("O_ORDERKEY"), col("O_ORDERDATE"))
        .join(line, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_ORDERDATE").dt.year().alias("o_year"),
            col("volume"),
            (col("N_NAME") == "BRAZIL").if_else(col("volume"), 0.0).alias("case_volume"),
        )
        .groupby(col("o_year"))
        .agg([(col("case_volume").alias("case_volume_sum"), "sum"), (col("volume").alias("volume_sum"), "sum")])
        .select(col("o_year"), col("case_volume_sum") / col("volume_sum"))
        .sort(col("o_year"))
    )

    return daft_df


def q9(get_df: GetDFFunc) -> DataFrame:
    lineitem = get_df("lineitem")
    part = get_df("part")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    orders = get_df("orders")

    def expr(x, y, v, w):
        return x * (1 - y) - (v * w)

    linepart = part.where(col("P_NAME").str.contains("green")).join(
        lineitem, left_on=col("P_PARTKEY"), right_on=col("L_PARTKEY")
    )
    natsup = nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))

    daft_df = (
        linepart.join(natsup, left_on=col("L_SUPPKEY"), right_on=col("S_SUPPKEY"))
        .join(partsupp, left_on=[col("L_SUPPKEY"), col("P_PARTKEY")], right_on=[col("PS_SUPPKEY"), col("PS_PARTKEY")])
        .join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .select(
            col("N_NAME"),
            col("O_ORDERDATE").dt.year().alias("o_year"),
            expr(col("L_EXTENDEDPRICE"), col("L_DISCOUNT"), col("PS_SUPPLYCOST"), col("L_QUANTITY")).alias("amount"),
        )
        .groupby(col("N_NAME"), col("o_year"))
        .agg([(col("amount"), "sum")])
        .sort(by=["N_NAME", "o_year"], desc=[False, True])
    )

    return daft_df


def q10(get_df: GetDFFunc) -> DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(col("L_RETURNFLAG") == "R")
    orders = get_df("orders")
    nation = get_df("nation")
    customer = get_df("customer")

    daft_df = (
        orders.where(
            (col("O_ORDERDATE") < datetime.date(1994, 1, 1)) & (col("O_ORDERDATE") >= datetime.date(1993, 10, 1))
        )
        .join(customer, left_on=col("O_CUSTKEY"), right_on=col("C_CUSTKEY"))
        .join(nation, left_on=col("C_NATIONKEY"), right_on=col("N_NATIONKEY"))
        .join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_CUSTKEY"),
            col("C_NAME"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
            col("C_ACCTBAL"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_PHONE"),
            col("C_COMMENT"),
        )
        .groupby(
            col("O_CUSTKEY"),
            col("C_NAME"),
            col("C_ACCTBAL"),
            col("C_PHONE"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_COMMENT"),
        )
        .agg([(col("volume").alias("revenue"), "sum")])
        .sort(col("revenue"), desc=True)
        .select(
            col("O_CUSTKEY"),
            col("C_NAME"),
            col("revenue"),
            col("C_ACCTBAL"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_PHONE"),
            col("C_COMMENT"),
        )
        .limit(20)
    )

    return daft_df
