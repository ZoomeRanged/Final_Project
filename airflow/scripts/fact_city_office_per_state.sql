create table if not exists fact_city_office_per_state (
	state varchar unique,
	total_city varchar,
	total_office varchar
);

insert into fact_city_office_per_state (
	state,
	total_city,
	total_office
)
(
select 
	state
	, count(distinct city) as total_city
	, count(distinct office) as total_office
from (
	select 
		name as office,
		case when city is null or city = '' then 'others' else city end as city,
		case when state_code is null or state_code = '' then 'others' else state_code end as state
	from companies
) stc 
group by state
)
on conflict (state) do update 
set total_city = excluded.total_city,
	total_office = excluded.total_office
;