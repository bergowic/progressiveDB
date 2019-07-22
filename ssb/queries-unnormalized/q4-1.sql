SELECT %s
  %s
  d_order_year,
  c_nation,
  SUM(lo_revenue - lo_supplycost) AS profit
FROM 
  %s
WHERE 
  c_region = 'AMERICA'
  AND s_region = 'AMERICA'
  AND (p_mfgr = 'MFGR#1'
    OR p_mfgr = 'MFGR#2')
  %s
GROUP BY 
  d_order_year,
  c_nation
ORDER BY 
  d_order_year,
  c_nation