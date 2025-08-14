# configuration.py

# ================================
# SEASON & EVENT GROUP IDs
# ================================

# Season IDs by year
SEASON_ID_MAP = {
    2018: 4494,
    2019: 5703,
    2020: 8582,
    2021: 29178,
    2022: 38109,
    2023: 38292,
    2024: 42499,
    2025: 59654
}

# Event Group IDs by (year, week)
EVENT_GROUP_ID_MAP = {}

# 2018-2020
for year in range(2018, 2021):
    for week in range(1, 18):
        EVENT_GROUP_ID_MAP[(year, week)] = 9 + week
    for week, egid in enumerate([28, 29, 30, 31], start=18):
        EVENT_GROUP_ID_MAP[(year, week)] = egid

# 2021-2025
for year in range(2021, 2026):
    for week in range(1, 18):
        EVENT_GROUP_ID_MAP[(year, week)] = 9 + week
    EVENT_GROUP_ID_MAP[(year, 18)] = 33573
    for week, egid in enumerate([28, 29, 30, 31], start=19):
        EVENT_GROUP_ID_MAP[(year, week)] = egid


# ================================
# TEAM MAPPINGS
# ================================

TEAM_TO_PARTID = {
    "CAROLINA": 1545, "DALLAS": 1538, "L.A. RAMS": 1550, "PITTSBURGH": 1519,
    "CLEVELAND": 1520, "BALTIMORE": 1521, "CINCINNATI": 1522, "N.Y. JETS": 1523,
    "MIAMI": 1524, "NEW ENGLAND": 1525, "BUFFALO": 1526, "INDIANAPOLIS": 1527,
    "TENNESSEE": 1528, "JACKSONVILLE": 1529, "HOUSTON": 1530, "KANSAS CITY": 1531,
    "DENVER": 1534, "N.Y. GIANTS": 1535, "PHILADELPHIA": 1536, "WASHINGTON": 1537,
    "DETROIT": 1539, "CHICAGO": 1540, "MINNESOTA": 1541, "GREEN BAY": 1542,
    "NEW ORLEANS": 1543, "TAMPA BAY": 1544, "ATLANTA": 1546, "SAN FRANCISCO": 1547,
    "SEATTLE": 1548, "ARIZONA": 1549, "L.A. CHARGERS": 75380,
    "OAKLAND": 1533, "LAS VEGAS": 1533
}

# Reverse mapping if needed
PARTID_TO_TEAM = {v: k for k, v in TEAM_TO_PARTID.items()}
