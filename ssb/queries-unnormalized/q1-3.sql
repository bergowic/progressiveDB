SELECT 
  SUM(lo_extendedprice * lo_discount) AS revenue
FROM 
  %s
WHERE 
  d_order_weeknuminyear = 6
  AND d_order_year = 1994
  AND lo_discount BETWEEN 5 AND 7
  AND lo_quantity BETWEEN 26 AND 35;
