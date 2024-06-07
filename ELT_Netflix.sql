
use ETL_projects;
/*
1.check the datatype and the length. In case there are any special characters like korean/chinese then 
use the datatype as nvarchar!
2.In SQL server,it is case insensitive.

*/
--drop table netflix_raw
select * from netflix_raw

--Cleaning & Transformation:

--1.handling foreign data
--no need to add as nvarchar as postgres is reading foreign language
SELECT * FROM netflix_raw
where show_id='s5023'; 

--2.remove duplicates
select * from netflix_raw
where lower(title) in
(
select lower(title) 
from netflix_raw
group by lower(title)
having count(*)>1
)
order by title;

--group by type too
select * from netflix_raw
where concat(lower(title),type)  in
(select concat(lower(title),type) 
from netflix_raw
group by concat(lower(title),type)
having count(*)>1
)
order by show_id

with cte1 as
(
	select *
	,row_number() over (partition by lower(title),type order by show_id) as _rn
	from netflix_raw
)
select show_id,type,title,cast(date_added as date) as date_added,release_year,rating,
CASE
	WHEN duration IS NULL THEN rating
	ELSE duration
END AS duration
,description  
into netflix_cleaned
from cte1
where _rn=1;

select * from netflix_cleaned

-------------------------3.Creating columns for comma seperated values in a cell

select show_id,trim(value) as directors
into netflix_directors
from netflix_raw
cross apply string_split(director,',')

select * from netflix_directors

select show_id,trim(value) as movie_cast
into netflix_cast
from netflix_raw
cross apply string_split(cast,',')

select * from netflix_cast

---------------------------------4.converting date column from varchar to date
select cast(date_added as date) as date from netflix_raw

---------------------------------5.populate missing values
--check null values using pandas: df.isna().sum()
--country we take same directors country and replace null
--duration takes rating
INSERT INTO netflix_country
SELECT nr.show_id, m1.country
FROM netflix_raw nr
INNER JOIN 
(
	SELECT nd.directors, nc.country
    FROM netflix_country nc
    INNER JOIN netflix_directors nd
    ON nc.show_id = nd.show_id
    GROUP BY nd.directors, nc.country
) m1 ON nr.director = m1.directors
WHERE nr.country IS NULL;

/*SELECT nd.directors, nc.country
    FROM netflix_country nc
    INNER JOIN netflix_directors nd
    ON nc.show_id = nd.show_id
    GROUP BY nd.directors, nc.country*/

/*select *,
CASE
	WHEN duration IS NULL THEN rating
	ELSE duration
END AS duration
from netflix_raw  --use this in the cte1 non duplicate table
WHERE duration IS NULL*/

-------------------------------------QUESTIONS-------------------------------------------------------------------
/*
1.For each director give the count of movies and Tv shows in seperate cols where the director has created both Tv shows & movies
*/

/*SELECT nd.directors,COUNT(DISTINCT n.type) AS distinct_type
from netflix_cleaned n
inner join netflix_directors nd
on n.show_id=nd.show_id
group by nd.directors
having count(distinct n.type)>1
order by distinct_type desc*/


select nd.directors
,count(distinct case when n.type='Movie' then n.show_id end) as no_of_movie
,count(distinct case when n.type='TV Show' then n.show_id end) as no_of_tvshow
from netflix_cleaned n
inner join netflix_directors nd
on n.show_id=nd.show_id
group by nd.directors
having count(distinct n.type)>1
order by nd.directors



--------------2.which country has the highest number of comedy movies
select top 1 nc.country,count(distinct ng.show_id) as no_of_comedy_movies
from netflix_genre ng
inner join netflix_country nc on ng.show_id=nc.show_id
inner join netflix_cleaned nc1 on ng.show_id=nc1.show_id
where ng.genre='Comedies' and nc1.type='Movie'
group by nc.country
order by no_of_comedy_movies desc

--------3 for each year (as per date added to netflix), which director has maximum number of movies released
with cte as (
select nd.directors,YEAR(date_added) as date_year,count(n.show_id) as no_of_movies
from netflix_cleaned n
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie'
group by nd.directors,YEAR(date_added)
)
, cte2 as (
select *
, ROW_NUMBER() over(partition by date_year order by no_of_movies desc, directors) as rn
from cte
--order by date_year, no_of_movies desc
)
select * from cte2 where rn=1



--4 what is average duration of movies in each genre
select ng.genre , avg(cast(REPLACE(duration,' min','') AS int)) as avg_duration
from netflix_cleaned n
inner join netflix_genre ng on n.show_id=ng.show_id
where type='Movie'
group by ng.genre

--5  find the list of directors who have created horror and comedy movies both.
-- display director names along with number of comedy and horror movies directed by them 
select nd.directors
, count(distinct case when ng.genre='Comedies' then n.show_id end) as no_of_comedy 
, count(distinct case when ng.genre='Horror Movies' then n.show_id end) as no_of_horror
from netflix_cleaned n
inner join netflix_genre ng on n.show_id=ng.show_id
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie' and ng.genre in ('Comedies','Horror Movies')
group by nd.directors
having COUNT(distinct ng.genre)=2;

select * from netflix_genre where show_id in 
(select show_id from netflix_directors where directors='Steve Brill')
order by genre



