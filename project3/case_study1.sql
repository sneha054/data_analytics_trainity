-- job data analysis
show databases;
create database case_study1;    
use case_study1;

CREATE TABLE job_data (
    job_id INT PRIMARY KEY,
    actor_id INT,
    event VARCHAR(20),
    language VARCHAR(20),
    time_spent INT,
    org VARCHAR(50),
    ds DATE
);

-- Insert 10,000 random rows into job_data
INSERT INTO job_data (job_id, actor_id, event, language, time_spent, org, ds)
SELECT 
    t.n,
    FLOOR(1 + (RAND() * 1000)),
    ELT(FLOOR(1 + (RAND() * 3)), 'decision', 'skip', 'transfer'),
    ELT(FLOOR(1 + (RAND() * 5)), 'English', 'Spanish', 'French', 'German', 'Chinese'),
    FLOOR(1 + (RAND() * 300)),
    ELT(FLOOR(1 + (RAND() * 5)), 'OrgA', 'OrgB', 'OrgC', 'OrgD', 'OrgE'),
    DATE_ADD('2020-11-01', INTERVAL FLOOR(RAND() * 30) DAY)
FROM (
    SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS n
    FROM
        (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
        (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
        (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
        (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d
    LIMIT 100000
) t;

select * from job_data limit 0,10000;

-- jobs reviewed over time
-- throughput analysis
-- language share analysis
-- duplicate rows detection

-- jobs reviewed/day for nov 2020
SELECT 
    ds,
    COUNT(job_id) AS jobs_reviewed
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds
ORDER BY ds;

-- 7-day rolling avg of throughput or daily metric
WITH daily_events AS (
    SELECT 
        ds,
        COUNT(*) / 86400 AS events_per_second
    FROM job_data
    GROUP BY ds
)
SELECT 
    ds,
    AVG(events_per_second) OVER (
        ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_throughput
FROM daily_events;

-- percentage share/language over last 30 days
WITH last_30_days AS (
    SELECT language, COUNT(*) AS language_count
    FROM job_data
    WHERE ds >= DATE_SUB('2020-11-30', INTERVAL 30 DAY)
    GROUP BY language
)
SELECT 
    language,
    language_count,
    (language_count * 100.0) / SUM(language_count) OVER () AS language_share_percentage
FROM last_30_days
ORDER BY language_share_percentage DESC;

-- display duplicate rows
SELECT 
    job_id, actor_id, event, language, time_spent, org, ds,
    COUNT(*) AS duplicate_count
FROM job_data
GROUP BY job_id, actor_id, event, language, time_spent, org, ds
HAVING COUNT(*) > 1;
-- no duplicates in the dataset
-- adding the duplicates
-- Insert 10 duplicate rows into job_data
-- Insert EXACT duplicates 10 times
INSERT INTO job_data (job_id, actor_id, event, language, time_spent, org, ds)
VALUES 
(11001, 201, 'decision', 'English', 120, 'OrgA', '2020-11-10'),
(11002, 202, 'skip', 'Spanish', 80, 'OrgB', '2020-11-11'),
(11003, 203, 'transfer', 'French', 150, 'OrgC', '2020-11-12'),
(11004, 204, 'decision', 'German', 200, 'OrgD', '2020-11-13'),
(11005, 205, 'skip', 'Chinese', 90, 'OrgE', '2020-11-14'),
(11001, 201, 'decision', 'English', 120, 'OrgA', '2020-11-10'),
(11002, 202, 'skip', 'Spanish', 80, 'OrgB', '2020-11-11'),
(11003, 203, 'transfer', 'French', 150, 'OrgC', '2020-11-12'),
(11004, 204, 'decision', 'German', 200, 'OrgD', '2020-11-13'),
(11005, 205, 'skip', 'Chinese', 90, 'OrgE', '2020-11-14');

-- checking for duplicates again
SELECT 
    actor_id, event, language, org, ds,
    COUNT(*) AS duplicate_count
FROM job_data
GROUP BY actor_id, event, language, org, ds
HAVING duplicate_count > 1;


