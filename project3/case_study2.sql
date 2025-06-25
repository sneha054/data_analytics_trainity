create database case_study2;
use case_study2;

#table1 users

create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 9.1/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

alter table users add column temp_created_at datetime;

SET SQL_SAFE_UPDATES = 0;

UPDATE users
SET temp_created_at = CASE
    WHEN created_at LIKE '%/%' THEN STR_TO_DATE(created_at, '%d/%m/%Y %H:%i')
    WHEN created_at LIKE '%-%' THEN STR_TO_DATE(created_at, '%d-%m-%Y %H:%i')
    ELSE NULL
END
WHERE user_id IS NOT NULL;


alter table users drop column created_at;

alter table users change column temp_created_at created_at datetime;

select * from users;

#table2 events

create table events(
user_id int,
occured_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device  varchar(50),
user_type int);

load data infile "C:/ProgramData/MySQL/MySQL Server 9.1/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

desc events;
select * from events;

alter table events add column temp_occured_at datetime;

SET SQL_SAFE_UPDATES = 0;

UPDATE events
SET temp_occured_at = CASE
    WHEN occured_at LIKE '%/%' THEN STR_TO_DATE(occured_at, '%d/%m/%Y %H:%i')
    WHEN occured_at LIKE '%-%' THEN STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i')
    ELSE NULL
END
WHERE user_id IS NOT NULL;


alter table events drop column occured_at;

alter table events change column temp_occured_at occured_at datetime;

select * from events;

#table3 email_events

create table email_events(
user_id int,
occured_at varchar(100),
action varchar(100),
user_type int);

SHOW VARIABLES LIKE 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 9.1/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

desc email_events;
select * from email_events;

alter table email_events add column temp_occured_at datetime;

SET SQL_SAFE_UPDATES = 0;

UPDATE email_events
SET temp_occured_at = CASE
    WHEN occured_at LIKE '%/%' THEN STR_TO_DATE(occured_at, '%d/%m/%Y %H:%i')
    WHEN occured_at LIKE '%-%' THEN STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i')
    ELSE NULL
END
WHERE user_id IS NOT NULL;


alter table email_events drop column occured_at;

alter table email_events change column temp_occured_at occured_at datetime;

select * from email_events;

-- weekly user engagement
SELECT 
    YEAR(e.occured_at) AS year,
    WEEK(e.occured_at) AS week,
    COUNT(DISTINCT e.user_id) AS active_users
FROM events e
GROUP BY year, week
ORDER BY year, week;

-- user growth analysis
SELECT 
    YEAR(created_at) AS year,
    MONTH(created_at) AS month,
    COUNT(user_id) AS new_users
FROM users
GROUP BY year, month
ORDER BY year, month;

-- weekly retention analysis
WITH signup_cohort AS (
    SELECT 
        user_id,
        DATE(created_at) AS signup_date
    FROM users
),
weekly_engagement AS (
    SELECT 
        u.user_id,
        s.signup_date,
        YEAR(e.occured_at) AS year,
        WEEK(e.occured_at) AS week
    FROM events e
    JOIN users u ON e.user_id = u.user_id
    JOIN signup_cohort s ON u.user_id = s.user_id
)
SELECT 
    signup_date,
    year,
    week,
    COUNT(DISTINCT user_id) AS retained_users
FROM weekly_engagement
GROUP BY signup_date, year, week
ORDER BY signup_date, year, week;

-- weekly engagement per device
SELECT 
    YEAR(e.occured_at) AS year,
    WEEK(e.occured_at) AS week,
    e.device,
    COUNT(DISTINCT e.user_id) AS active_users
FROM events e
GROUP BY year, week, e.device
ORDER BY year, week, active_users DESC;

-- email engagement analysis
SELECT DISTINCT action
FROM email_events;

SELECT 
    DATE(occured_at) AS email_date,
    COUNT(*) AS total_emails_sent,
    SUM(CASE WHEN action = 'email_open' THEN 1 ELSE 0 END) AS emails_opened,
    SUM(CASE WHEN action = 'email_clickthrough' THEN 1 ELSE 0 END) AS emails_clicked,
    ROUND((SUM(CASE WHEN action = 'email_open' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS open_rate,
    ROUND((SUM(CASE WHEN action = 'email_clickthrough' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS click_rate
FROM email_events
GROUP BY email_date
ORDER BY email_date;





