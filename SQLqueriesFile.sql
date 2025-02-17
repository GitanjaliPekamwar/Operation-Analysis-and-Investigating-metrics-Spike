create database Project3;
show databases;
use Project3;

#case study1 : job data analysis

create table jobdata(
ds int,
job_id int,
actor_id int,
event varchar(100),
language varchar(100),
time_spent int,
org varchar(50) 
);

ALTER TABLE jobdata MODIFY ds varchar(1000);

LOAD DATA INFILE "C:/job_data (1).csv"
into table jobdata
fields terminated by ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

select*from jobdata;

#A) jobs reviewed over time

select ds as date,
count(job_id) as job_review_counts,
round(count(job_id)/(sum(time_spent)/(60*60)),2) as job_review_per_hour_each_day
from jobdata
where ds between '01-11-2020' and '30-11-2020'
group by ds
order by ds;

#B) Throughput analysis
WITH total_events AS ( SELECT date(ds) 
AS event_date, COUNT(job_id) AS 
total_jobs, SUM(time_spent) AS 
total_time_spent
FROM jobdata
GROUP BY DATE(ds))
SELECT total_jobs / (total_time_spent / 3600) 
AS throughput, AVG(total_jobs / 
(total_time_spent / 3600)) OVER ( ORDER 
BY event_date ROWS BETWEEN 6 
PRECEDING AND CURRENT ROW ) AS 
rolling_average
FROM total_events;

#C) language analysi

SELECT 
    language,
    (COUNT(language) / (SELECT 
            COUNT(language)
        FROM
            jobdata)) * 100 AS percentage
FROM
    jobdata
GROUP BY language;

#D) duplicate row  detetction

SELECT 
    job_id, COUNT(*) AS duplicate
FROM
    jobdata
GROUP BY job_id
HAVING COUNT(*) > 1;


#table1 : users 
create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50) );
Show variables like "secure_file_priv";
LOAD DATA INFILE "C:/users (1).csv"
into table users
fields terminated by ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;
select * from users;
ALTER TABLE users ADD COLUMN temp_created_at DATETIME;
ALTER TABLE users RENAME COLUMN temp_created_at TO temp_created_at_old;
SELECT created_at FROM users
WHERE STR_TO_DATE(created_at, '%d-%m-%Y %H:%i:%s') IS NULL;
UPDATE users SET temp_created_at_old = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i:%s')
WHERE STR_TO_DATE(created_at, '%d-%m-%Y %H:%i:%s') IS NOT NULL;
alter table users drop column created_at;
alter table users change column temp_created_at_old created_at datetime;

#1) weekly user engagemenet

SELECT extract(week from occurred_at) as week_number,
count(distinct user_id) as num_users
from events
where event_type = 'engagement'
group by week_number
order by week_number;















#creating table-2 events
create table events(
user_id INT,
occurred_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int );

LOAD DATA INFILE "C:/events.csv"
INTO TABLE events
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;
select*from events;
ALTER TABLE events ADD COLUMN temp_occured_at DATETIME;
SET SQL_SAFE_UPDATES = 0;
UPDATE events
SET temp_occured_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i:%s')
WHERE STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i:%s') IS NOT NULL;
alter table events drop column occurred_at;
alter table events change column temp_occured_at occurred_at datetime;



























# table-3 emailEvents
create table emailEvents(
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int
);
LOAD DATA INFILE "C:/email_events.csv"
INTO TABLE emailevents
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
ignore 1 rows;

select*from emailevents;
ALTER TABLE emailevents ADD COLUMN temp_occured_at DATETIME;
UPDATE emailevents
SET temp_occured_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i:%s')
WHERE STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i:%s') IS NOT NULL;

alter table emailevents drop column occurred_at;
alter table emailevents change column temp_occured_at occurred_at datetime;


#B) user grouth analysis

WITH totalUsers AS ( SELECT 
WEEK(created_at) AS week, 
COUNT(*) AS users_2 FROM users 
GROUP BY WEEK(created_at)) 
SELECT week,users_2,((users_2-
LAG(users_2) OVER (ORDER BY 
week)) / LAG(users_2) OVER (ORDER 
BY week))*100 AS percentage_growth
FROM
totalUsers;

#C) weeekly retention anlysis

SELECT users.user_id, Date(activated_at) as Date, week(activated_at) as week, COUNT(distinct users.user_id) as num_users
FROM users JOIN events ON events.user_id=users.user_id
WHERE event_type='signup_flow'
GROUP BY users.user_id,activated_at,week;

SELECT 
    users.user_id,
    DATE(activated_at) AS Date,
    WEEK(activated_at) AS week,
    COUNT(DISTINCT users.user_id) AS users
FROM
    users
        JOIN
    events ON events.user_id = users.user_id
WHERE
    event_type = 'signup_flow'
GROUP BY users.user_id , activated_at , week;

SELECT users.user_id, Date(activated_at) as Date, week(activated_at) as week, COUNT(distinct users.user_id) as users
FROM usersinner JOIN events ON events.user_id=users.user_id
WHERE event_type='signup_flow'
GROUP BY users.user_id,activated_at,week;

#D) weekly engagement per device

SELECT COUNT(DISTINCT user_id) as total_users,
       COUNT(*) as engagements,
       WEEK(occurred_at) as week_no,
       device
FROM events
WHERE event_type = "engagement"
GROUP BY week_no, device;



#E) email engagement analysis

SELECT week(occurred_at) as week_no, 
count(distinct user_id) as userID,
SUM(case when action="email_open" then 1 
else 0 end) as total_emails_opened,
SUM(case when action="email_clickthrough" 
then 1 else 0 end) as total_emails_clicked,
SUM(case when action="sent_weekly_digest" 
then 1 else 0 end) as 
total_emails_sent,user_type
FROM emailevents
GROUP BY week_no, user_type;


#C) weekly retention analysis

select cohort_year, cohort_week, retention_week, engaged_users,
  max(engaged_users)





























