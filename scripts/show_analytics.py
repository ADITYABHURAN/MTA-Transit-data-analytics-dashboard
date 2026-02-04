"""
Display analytics summary from generated data
"""
import pandas as pd
import numpy as np
import os

def main():
    print("=" * 70)
    print("MTA TRANSIT DATA ANALYTICS - SUMMARY REPORT")
    print("=" * 70)
    
    data_dir = "data/synthetic"
    
    # Load data
    ridership = pd.read_csv(f"{data_dir}/ridership.csv")
    delays = pd.read_csv(f"{data_dir}/delays.csv")
    perf = pd.read_csv(f"{data_dir}/performance.csv")
    stations = pd.read_csv(f"{data_dir}/stations.csv")
    
    print("\n📊 TOP 10 BUSIEST STATIONS:")
    print("-" * 50)
    top_stations = ridership.groupby("station_name")["entries"].sum().sort_values(ascending=False).head(10)
    for i, (name, entries) in enumerate(top_stations.items(), 1):
        print(f"{i:2}. {name:30} {entries:>12,} entries")
    
    print("\n🚇 RIDERSHIP BY BOROUGH:")
    print("-" * 50)
    merged = ridership.merge(stations[["station_name", "borough"]], on="station_name", how="left")
    borough_traffic = merged.groupby("borough")["entries"].sum().sort_values(ascending=False)
    for borough, entries in borough_traffic.items():
        if pd.notna(borough):
            pct = entries / merged["entries"].sum() * 100
            print(f"{borough:20} {entries:>15,} entries ({pct:.1f}%)")
    
    print("\n⏰ PEAK HOURS ANALYSIS:")
    print("-" * 50)
    hourly = ridership.groupby("hour")["entries"].sum()
    peak_morning = hourly[7:10].sum()
    peak_evening = hourly[17:20].sum()
    off_peak = hourly.sum() - peak_morning - peak_evening
    total = hourly.sum()
    print(f"Morning Rush (7-10 AM):  {peak_morning:>12,} ({peak_morning/total*100:.1f}%)")
    print(f"Evening Rush (5-8 PM):   {peak_evening:>12,} ({peak_evening/total*100:.1f}%)")
    print(f"Off-Peak Hours:          {off_peak:>12,} ({off_peak/total*100:.1f}%)")
    
    print("\n📈 ON-TIME PERFORMANCE BY LINE (Top 10):")
    print("-" * 50)
    line_perf = perf.groupby("line_name")["on_time_percentage"].mean().sort_values(ascending=False)
    for line, otp in line_perf.head(10).items():
        bar = "█" * int(otp / 5)
        status = "✓" if otp >= 85 else "○" if otp >= 75 else "✗"
        print(f"Line {line:2}: {otp:5.1f}% {bar} {status}")
    
    print("\n⚠️ DELAYS BY CAUSE (Top 8):")
    print("-" * 50)
    delay_causes = delays.groupby("delay_reason").agg(
        count=("delay_duration_minutes", "count"),
        avg_min=("delay_duration_minutes", "mean")
    ).sort_values("count", ascending=False).head(8)
    for reason, row in delay_causes.iterrows():
        print(f"{reason:25} {int(row['count']):>5} incidents  ({row['avg_min']:.1f} min avg)")
    
    print("\n🚨 DELAYS BY LINE:")
    print("-" * 50)
    line_delays = delays.groupby("line_name").agg(
        count=("delay_duration_minutes", "count"),
        avg_min=("delay_duration_minutes", "mean")
    ).sort_values("count", ascending=False).head(10)
    for line, row in line_delays.iterrows():
        bar = "▓" * min(int(row["count"] / 20), 20)
        print(f"Line {line:2}: {int(row['count']):>4} delays ({row['avg_min']:5.1f} min avg) {bar}")
    
    print("\n📅 WEEKDAY VS WEEKEND:")
    print("-" * 50)
    ridership["date"] = pd.to_datetime(ridership["date"])
    ridership["is_weekend_calc"] = ridership["date"].dt.dayofweek >= 5
    weekday_entries = ridership[~ridership["is_weekend_calc"]]["entries"].sum()
    weekend_entries = ridership[ridership["is_weekend_calc"]]["entries"].sum()
    weekday_days = ridership[~ridership["is_weekend_calc"]]["date"].nunique()
    weekend_days = ridership[ridership["is_weekend_calc"]]["date"].nunique()
    print(f"Weekday average: {weekday_entries/weekday_days:>12,.0f} entries/day")
    print(f"Weekend average: {weekend_entries/weekend_days:>12,.0f} entries/day")
    print(f"Weekend drop:    {(1 - weekend_entries/weekday_days*weekday_days/weekend_entries)*100:>11.1f}%")
    
    print("\n" + "=" * 70)
    print("DATA SUMMARY")
    print("=" * 70)
    print(f"Total Stations:           {len(stations):>10,}")
    print(f"Total Ridership Records:  {len(ridership):>10,}")
    print(f"Total Delay Incidents:    {len(delays):>10,}")
    print(f"Total Performance Records:{len(perf):>10,}")
    print(f"Date Range:               {ridership['date'].min()} to {ridership['date'].max()}")
    print(f"Total Entries:            {ridership['entries'].sum():>10,}")
    print(f"Total Exits:              {ridership['exits'].sum():>10,}")
    print("=" * 70)

if __name__ == "__main__":
    main()
