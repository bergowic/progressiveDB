SELECT 
  SUM(lo_revenue),
  d_order_year,
  p_brand1
FROM 
  %s
WHERE 
  p_brand1 = 'MFGR#2221'
  AND s_region = 'EUROPE'
  %s
GROUP BY 
  d_order_year,
  p_brand1
ORDER BY 
  d_order_year,
  p_brand1;
