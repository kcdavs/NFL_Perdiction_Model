import requests
import time

RENDER_BASE = "https://nfl-gambling-addiction-ml.onrender.com"

seid_map = {
    2018: 4494,
    2019: 5703,
    2020: 8582,
    2021: 29178,
    2022: 38109,
    2023: 38292,
    2024: 42499,
    2025: 59654
}

for year, seid in seid_map.items():
    print(f"\n📅 Processing season {year}...")

    week_num = 1
    for egid in range(10, 32):
        if egid == 27:
            continue  # Skip the missing week

        # Insert week 18 for 2021+ after egid 26
        if year >= 2021 and egid == 28:
            print(f"🌐 {year} Week 18 → egid 33573")
            try:
                url = f"{RENDER_BASE}/combined/{year}/18"
                res = requests.get(url + "?special=33573", timeout=60)
                print(f"✅ {res.text.strip()}")
            except Exception as e:
                print(f"❌ Week 18 failed: {e}")
            time.sleep(2.5)

        print(f"🌐 {year} Week {week_num} → egid {egid}")
        try:
            url = f"{RENDER_BASE}/combined/{year}/{week_num}"
            res = requests.get(url, timeout=60)
            print(f"✅ {res.text.strip()}")
        except Exception as e:
            print(f"❌ Week {week_num} failed: {e}")
        week_num += 1
        time.sleep(2.5)
