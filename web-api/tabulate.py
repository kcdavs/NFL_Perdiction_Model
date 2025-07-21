import pandas as pd
import json

def load_and_pivot_acl(filepath, label):
    # Static partid → team mapping
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

    with open(filepath, "r") as f:
        data = json.load(f)

    # Load each section
    cl = pd.DataFrame(data["data"]["A_CL"])[["eid", "partid", "paid", "adj", "ap"]]
    co = pd.DataFrame(data["data"]["A_CO"])[["eid", "partid", "perc"]].drop_duplicates()
    ol = pd.DataFrame(data["data"]["A_OL"])[["eid", "partid", "adj", "ap"]]
    ol.columns = ["eid", "partid", "opening_adj", "opening_ap"]

    # Pivot current lines
    paid_vals = sorted(cl["paid"].unique())
    cl_adj = cl.pivot_table(index=["eid", "partid"], columns="paid", values="adj").add_prefix("adj_")
    cl_ap = cl.pivot_table(index=["eid", "partid"], columns="paid", values="ap").add_prefix("ap_")
    pivot_df = cl_adj.join(cl_ap).reset_index()

    # Merge all together
    df = (
        pivot_df
        .merge(co, on=["eid", "partid"], how="left")
        .merge(ol, on=["eid", "partid"], how="left")
    )

    # Add team name
    df["team"] = df["partid"].map(team_map)
    df["jsons"] = label
    return df
