create table if not exists dim_country (
	id serial primary key,
	country_code varchar unique
	
);

insert into dim_country ( 
  country_code
)
(
select 
	 
	case when country_code is null then 'others' else country_code end as country_code
from (
	-- get data from companies
	select distinct 
		country_code as country_code
	from companies
	) stc
)
on conflict (country_code) do nothing
;