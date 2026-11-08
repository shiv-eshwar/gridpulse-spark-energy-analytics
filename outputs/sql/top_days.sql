SELECT to_date(event_time) day, COUNT(*) valid_minutes,
        ROUND(SUM(energy_kwh),3) observed_kwh FROM energy GROUP BY 1
        HAVING COUNT(*) >= 1380 ORDER BY observed_kwh DESC LIMIT 10