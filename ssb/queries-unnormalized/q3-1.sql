SELECT 
  c_nation,
  s_nation,
  d_order_year,
  SUM(lo_revenue) AS revenue
FROM 
  %s
WHERE 
  c_region = 'ASIA'
  AND s_region = 'ASIA'
  AND d_order_year >= 1992
  AND d_order_year <= 1997
  %s
GROUP BY 
  c_nation,
  s_nation,
  d_order_year
ORDER BY 
  d_order_year ASC, 
  revenue DESC;
