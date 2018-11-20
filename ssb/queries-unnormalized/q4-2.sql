SELECT 
  d_order_year,
  s_nation,
  p_category,
  SUM(lo_revenue - lo_supplycost) AS profit
FROM 
  %s
WHERE 
  c_region = 'AMERICA'
  AND s_region = 'AMERICA'
  AND (d_order_year = 1997
    OR d_order_year = 1998)
  AND (p_mfgr = 'MFGR#1'
    OR p_mfgr = 'MFGR#2')
  %s
GROUP BY 
  d_order_year,
  s_nation,
  p_category
ORDER BY 
  d_order_year,
  s_nation,
  p_category;
