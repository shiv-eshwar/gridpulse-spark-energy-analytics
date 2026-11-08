SELECT SUM(kitchen_wh)/1000 kitchen_kwh, SUM(laundry_wh)/1000 laundry_kwh,
        SUM(hvac_wh)/1000 hvac_kwh, SUM(unmetered_wh)/1000 unmetered_kwh FROM energy