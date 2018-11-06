SELECT 
  SUM(lo_revenue),
  d_order_year,
  p_brand1
FROM 
  %s
WHERE 
  p_category = 'MFGR#12'
  AND s_region = 'AMERICA'
GROUP BY 
  d_order_year,
  p_brand1
ORDER BY 
  d_order_year,
  p_brand1;
