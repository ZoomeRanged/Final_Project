create table if not exists dim_currency (
	id serial,
	currency_name varchar,
	currency_code varchar primary key
	
);

insert into dim_currency ( 
  currency_name,
  currency_code
)
(
select 
    currency_name, 
    currency_code 
from(
	-- get data from topic_currency
	select distinct 
        currency_name, 
        currency_code as currency_code
	from currencies 
	) tc
)
on conflict (currency_code) do nothing
;