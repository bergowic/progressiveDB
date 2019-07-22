select %s 
	sum(lo_revenue), 
	d_year, 
	p_brand1
from 
	lineorder, 
	dim_date, 
	part, 
	supplier
where 
	lo_orderdate = d_datekey and 
	lo_partkey = p_partkey and 
	lo_suppkey = s_suppkey and
	-- OK to add p_mfgr=’MFGR#2’
	-- OK to add p_category=’MFGR#22’
	p_brand1 = 'MFGR#2221' and 
	s_region = 'EUROPE'
group by 
	d_year, 
	p_brand1
order by 
	d_year, 
	p_brand1
