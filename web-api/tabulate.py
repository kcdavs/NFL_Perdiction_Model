def load_and_pivot_acl(filepath, label):
    import json
    import pandas as pd
    from pyspark.sql.functions import first, lit
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Static partid → team map
    team_map = {
        1536: "Philadelphia", 1546: "Atlanta",
        1541: "Minnesota", 1547: "San Francisco",
        1525: "New England", 1530: "Houston",
        1521: "Baltimore", 1526: "Buffalo",
        1529: "Jacksonville", 1535: "N.Y. Giants",
        1527: "Indianapolis", 1522: "Cincinnati",
        1531: "Kansas City", 75380: "L.A. Chargers",
        1543: "New Orleans", 1544: "Tampa Bay",
        1523: "N.Y. Jets", 1539: "Detroit",
        1540: "Chicago", 1542: "Green Bay",
        1533: "Oakland", 1550: "L.A. Rams",
        1538: "Dallas", 1545: "Carolina",
        1534: "Denver", 1548: "Seattle",
        1537: "Washington", 1549: "Arizona",
        1524: "Miami", 1528: "Tennessee",
        1519: "Pittsburgh", 1520: "Cleveland"
    }
    map_df = pd.DataFrame(list(team_map.items()), columns=["partid", "team"])
    spark_map = spark.createDataFrame(map_df)

    with open(filepath, "r") as f:
        data = json.load(f)

    a_cl = pd.DataFrame(data["data"]["A_CL"])[["eid", "partid", "paid", "adj", "ap"]]
    spark_cl = spark.createDataFrame(a_cl)
    paid_vals = sorted(a_cl["paid"].unique())

    pivot_adj = spark_cl.groupBy("eid", "partid").pivot("paid", paid_vals).agg(first("adj"))
    pivot_ap = spark_cl.groupBy("eid", "partid").pivot("paid", paid_vals).agg(first("ap"))

    pivot_adj = pivot_adj.toDF("eid", "partid", *[f"adj_{p}" for p in paid_vals])
    pivot_ap = pivot_ap.toDF("eid", "partid", *[f"ap_{p}" for p in paid_vals])
    pivot_df = pivot_adj.join(pivot_ap, on=["eid", "partid"])

    a_co = pd.DataFrame(data["data"]["A_CO"])[["eid", "partid", "perc"]]
    spark_co = spark.createDataFrame(a_co).dropDuplicates(["eid", "partid"])
    pivot_df = pivot_df.join(spark_co, on=["eid", "partid"], how="left")

    a_ol = pd.DataFrame(data["data"]["A_OL"])[["eid", "partid", "adj", "ap"]]
    a_ol.columns = ["eid", "partid", "opening_adj", "opening_ap"]
    spark_ol = spark.createDataFrame(a_ol).dropDuplicates(["eid", "partid"])
    full_df = pivot_df.join(spark_ol, on=["eid", "partid"], how="left")

    full_df = full_df.join(spark_map, on="partid", how="left")
    full_df = full_df.withColumn("jsons", lit(label))

    interleaved = [col for pair in zip([f"adj_{p}" for p in paid_vals], [f"ap_{p}" for p in paid_vals]) for col in pair]
    base_cols = ["jsons", "eid", "partid", "team", "perc", "opening_adj", "opening_ap"]
    return full_df.select(base_cols + interleaved)
