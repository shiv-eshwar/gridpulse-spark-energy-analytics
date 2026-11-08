SELECT date_format(event_time,'yyyy-MM') month, COUNT(*) valid_minutes,
        ROUND(SUM(energy_kwh),4) observed_kwh, ROUND(AVG(active_kw),4) mean_kw
        FROM energy GROUP BY 1 ORDER BY 1