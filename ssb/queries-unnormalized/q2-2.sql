SELECT 
  SUM(lo_revenue),
  d_order_year,
  p_brand1
FROM 
  %s
WHERE 
  p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228'
  AND s_region = 'ASIA'
  %s
GROUP BY 
  d_order_year,
  p_brand1
ORDER BY 
  d_order_year,
  p_brand1;
