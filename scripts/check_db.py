import os
from supabase import create_client
from dotenv import load_dotenv
import pandas as pd

# Load securely from .env.local
load_dotenv("/Users/karthik/Dev_Projects/agy/MoMoRebal-Kite/.env.local")
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

print("--- Check 1: Contiguity of strategies.momentum_weekly_history from 2026-07-10 ---")
try:
    res = sb.schema("strategies").table("momentum_weekly_history").select("asof_date").gte("asof_date", "2026-07-10").execute()
    dates = [row["asof_date"] for row in res.data]
    if dates:
        df = pd.DataFrame(dates, columns=["asof_date"])
        df['asof_date'] = pd.to_datetime(df['asof_date'])
        unique_dates = df['asof_date'].dt.date.unique()
        unique_dates.sort()
        
        start_date = pd.to_datetime("2026-07-10")
        end_date = pd.to_datetime("2026-07-23")
        expected_fridays = pd.date_range(start=start_date, end=end_date, freq='W-FRI').date
        missing_fridays = [d for d in expected_fridays if d not in unique_dates]
        if missing_fridays:
            print("Missing Fridays (History):", [d.strftime('%Y-%m-%d') for d in missing_fridays])
        else:
            print("No missing Fridays in History! Contiguous.")
    else:
        print("No data in strategies.momentum_weekly_history from 2026-07-10")
except Exception as e:
    print("Error querying strategies.momentum_weekly_history:", e)

print("\n--- Check 2: Coverage of strategies.momentum_weekly_regime ---")
try:
    res = sb.schema("strategies").table("momentum_weekly_regime").select("asof_date").execute()
    dates = [row["asof_date"] for row in res.data]
    if dates:
        df = pd.DataFrame(dates, columns=["asof_date"])
        df['asof_date'] = pd.to_datetime(df['asof_date'])
        unique_dates = df['asof_date'].dt.date.unique()
        unique_dates.sort()
        
        print(f"MIN regime asof_date: {unique_dates[0]}")
        print(f"MAX regime asof_date: {unique_dates[-1]}")
        
        # Check gaps from 2026-07-10
        start_date = pd.to_datetime("2026-07-10")
        end_date = pd.to_datetime("2026-07-23")
        expected_fridays = pd.date_range(start=start_date, end=end_date, freq='W-FRI').date
        missing_fridays = [d for d in expected_fridays if d not in unique_dates]
        if missing_fridays:
            print("Missing Fridays (Regime):", [d.strftime('%Y-%m-%d') for d in missing_fridays])
        else:
            print("No missing Fridays in Regime! Contiguous.")
    else:
        print("No data in strategies.momentum_weekly_regime")
except Exception as e:
    print("Error querying strategies.momentum_weekly_regime:", e)
