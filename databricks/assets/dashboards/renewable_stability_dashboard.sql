SELECT country_code, total_generation, volatility_index
FROM emit_dev.gold.renewable_stability
ORDER BY total_generation DESC;
