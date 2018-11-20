SELECT 
  c_city,
  s_city,
  d_order_year,
  SUM(lo_revenue) AS revenue
FROM 
  %s
WHERE 
  (c_city='UNITED KI1'
    OR c_city='UNITED KI5')
  AND (s_city='UNITED KI1'
    OR s_city='UNITED KI5')
  AND d_order_year >= 1992
  AND d_order_year <= 1997
  %s
GROUP BY 
  c_city,
  s_city,
  d_order_year
ORDER BY 
  d_order_year ASC, 
  revenue DESC;
