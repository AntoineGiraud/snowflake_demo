

-- 4s
create or replace table rentals_2020_csv_raw_gz as
select *
from read_csv('bikeshare\data/rentals_2020_gz.csv.gz');

-- 4s
create or replace table rentals_2020_csv_raw as
select *
from 'bikeshare\data/rentals_2020.csv';

-- 2s
create or replace table rentals_2020_parquet as
select *
from 'bikeshare\data/rentals_2020.parquet';

-- 0.035s
select start_date_month, count(1) nb_rentals,
	count(distinct start_date) nb_date
from rentals_2020_parquet
group by 1;
