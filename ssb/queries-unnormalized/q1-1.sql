SELECT 
  SUM(lo_extendedprice * lo_discount) AS revenue
FROM 
  %s
WHERE
  d_order_year = 1993
  AND lo_discount BETWEEN 1 AND 3
  AND lo_quantity < 25;
