SELECT zone, price_eur_mwh, is_price_spike
FROM emit_dev.gold.price_spike_analysis
WHERE is_price_spike = true;
