SELECT hour(event_time) hour, COUNT(*) n, AVG(active_kw) mean_kw,
        percentile_approx(active_kw,0.95,10000) p95_kw FROM energy GROUP BY 1 ORDER BY 1