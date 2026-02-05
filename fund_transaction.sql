SELECT *
FROM fund_transaction;

-- Create a new table same juct like fund_transaction

Create table  fund_transaction_clean
like fund_transaction;

-- Insert values same from fund_transaction

insert fund_transaction_clean
SELECT *
FROM fund_transaction;

SELECT *
FROM fund_transaction_clean;

-- Remove Duplicates

with cte as(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,
 'date', stage, country, funds_raised_millions  ) as rnk
FROM fund_transaction_clean
)
select *
from cte
where rnk>1;


with cte as(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,
 'date', stage, country, funds_raised_millions  ) as rnk
FROM fund_transaction_clean
)
Delete 
from cte
where rnk>1;

CREATE TABLE `fund_transaction_clean2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `rnk` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM fund_transaction_clean2;

insert fund_transaction_clean2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,
 'date', stage, country, funds_raised_millions  ) as rnk
FROM fund_transaction_clean;

select *
FROM fund_transaction_clean2
where rnk>1;

delete
FROM fund_transaction_clean2
where rnk>1;

-- standardizind data

select company,trim(company)
FROM fund_transaction_clean2;

update fund_transaction_clean2
set company=trim(company);

select *
FROM fund_transaction_clean2;

select distinct industry
FROM fund_transaction_clean2
order by 1;

select *
FROM fund_transaction_clean2
where industry like 'Crypto%';

update fund_transaction_clean2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
FROM fund_transaction_clean2
order by 1;

select distinct location
FROM fund_transaction_clean2
order by 1;

select *
FROM fund_transaction_clean2;

select distinct country
FROM fund_transaction_clean2
order by 1;

select *
FROM fund_transaction_clean2
where country like 'United States%'
order by 1;

select distinct country,trim(country)
FROM fund_transaction_clean2;

select distinct country,trim(trailing '.' from country)
FROM fund_transaction_clean2;

update fund_transaction_clean2
set country = trim(trailing '.' from country)
where country like 'United States%';


select *
FROM fund_transaction_clean2;

select `date`,
str_to_date(`date`,'%m/%d/%Y') as date
FROM fund_transaction_clean2;

update fund_transaction_clean2
set `date` = str_to_date(`date`,'%m/%d/%Y');

UPDATE fund_transaction_clean2
SET `date` = CASE
    WHEN `date` LIKE '%/%' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
    ELSE `date`
END;

select *
FROM fund_transaction_clean2;

alter table fund_transaction_clean2
modify column `date` date;



update fund_transaction_clean2
set industry= null
where industry='';

select *
from fund_transaction_clean2 t1
join fund_transaction_clean2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

update fund_transaction_clean2 t1
join fund_transaction_clean2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
FROM fund_transaction_clean2
where company ='Airbnb';

select *
FROM fund_transaction_clean2
where  total_laid_off is null
and percentage_laid_off is null ;

delete
FROM fund_transaction_clean2
where  total_laid_off is null
and percentage_laid_off is null ;

select *
FROM fund_transaction_clean2;

-- remove extra column 

alter table fund_transaction_clean2
drop column rnk;