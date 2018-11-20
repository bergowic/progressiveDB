SELECT 
  d_order_year,
  s_city,
  p_brand1,
  SUM(lo_revenue - lo_supplycost) AS profit
FROM 
  %s
WHERE 
  c_region = 'AMERICA'
  AND s_nation = 'UNITED STATES'
  AND (d_order_year = 1997
    OR d_order_year = 1998)
  AND p_category = 'MFGR#14'
  %s
GROUP BY 
  d_order_year,
  s_city,
  p_brand1
ORDER BY 
  d_order_year,
  s_city,
  p_brand1;
