SELECT 
  c_city,
  s_city,
  d_order_year,
  SUM(lo_revenue) AS revenue
FROM 
  %s
WHERE 
  c_nation = 'UNITED STATES'
  AND s_nation = 'UNITED STATES'
  AND d_order_year >= 1992
  AND d_order_year <= 1997
GROUP BY 
  c_city,
  s_city,
  d_order_year
ORDER BY 
  d_order_year ASC, 
  revenue DESC;
