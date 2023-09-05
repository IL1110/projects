#create database covid_project16;

DROP TABLE general;

create table general (
index bigint,
Province_State varchar,
Country_Region varchar,
Last_Update timestamp,
Confirmed bigint,
Deaths bigint,
Recovered bigint,
Active bigint,
Lat NUMERIC,
Long_ NUMERIC,
Incident_Rate NUMERIC,
Case_Fatality_Ratio NUMERIC
);

ALTER TABLE general ALTER COLUMN "Confirmed" TYPE bigint USING (NULLIF(trim("Confirmed"),'')::bigint);
ALTER TABLE general ALTER COLUMN "Deaths" TYPE bigint USING (trim("Deaths"),''::bigint);
ALTER TABLE general ALTER COLUMN "Recovered" TYPE bigint USING (NULLIF(trim("Recovered"),'')::bigint);
ALTER TABLE general ALTER COLUMN "Active" TYPE bigint USING (NULLIF(trim("Active"),'')::bigint);
ALTER TABLE general ALTER COLUMN "Lat" TYPE numeric USING (NULLIF(trim("Lat"),'')::numeric);
ALTER TABLE general ALTER COLUMN "Long_" TYPE numeric USING (NULLIF(trim("Long_"),'')::numeric);
ALTER TABLE general ALTER COLUMN "Case_Fatality_Ratio" TYPE numeric USING (NULLIF(trim("Case_Fatality_Ratio"),'')::numeric);
ALTER TABLE general ALTER COLUMN "Incident_Rate" TYPE numeric USING (NULLIF(trim("Incident_Rate"),'')::numeric);
ALTER TABLE general ALTER COLUMN "Last_Update" TYPE timestamp USING "Last_Update"::timestamp without time zone

ALTER TABLE general  RENAME COLUMN "Country_Region" TO Country_Region




create or replace  view view_example as
select Country_Region, sum("Deaths") as sum_death, sum("Confirmed") as sum_conf
from general group by Country_Region order by  sum_death desc limit 20;



create user graf_user_s16 with encrypted password 's16_password'; 
grant all PRIVILEGES on database covid_project16 to graf_user_s16;
grant select on table "general" to graf_user_s16;

select Country_Region,"Long_", "Lat", "Confirmed" from general 
order by "Confirmed" desc limit 100

select  Country_Region,"Long_", "Lat", sum("Confirmed") over (partition by Country_Region) as sum_conf from general 

select t1.Country_Region, sum_conf, "Long_", "Lat" from (select  distinct Country_Region from general)t1 left join
(select  Country_Region,"Long_", "Lat", sum("Confirmed") over (partition by Country_Region) as sum_conf from general) t2
on t1.Country_Region = t2.Country_Region


create table general_diseas (
		 index bigint,
	     updated DATE,
         country VARCHAR,
         cases BIGINT,
         deaths BIGINT,
         recovered BIGINT,
         active BIGINT,
         tests BIGINT,
         population BIGINT,
         lat NUMERIC,
         long NUMERIC
);

drop view view_general_diseas;
create view view_general_diseas as (select * from general_diseas)
        

