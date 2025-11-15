-- EXPLORE RAW DATA--
SELECT * FROM USERPROFILE;
SELECT * FROM VIEWERSHIP;

-------------------------------------------------------------------------------
                        -- COUNTS --
SELECT COUNT(*) AS total_users 
FROM USERPROFILE;
SELECT COUNT(DISTINCT userid) AS unique_user_ids 
FROM USERPROFILE;

SELECT COUNT(*) AS total_view_records 
FROM VIEWERSHIP;
SELECT COUNT(DISTINCT userid) AS users_with_views 
FROM VIEWERSHIP;

--------------------------------------------------------------------------------
                     -- CHECK DUPLICATES --
SELECT userid, COUNT(*) 
FROM USERPROFILE
GROUP BY userid
HAVING COUNT(*) > 1;

SELECT userid, recorddate2, COUNT(*)
FROM VIEWERSHIP
GROUP BY userid, recorddate2
HAVING COUNT(*) > 1;
-------------------------------------------------------------------------------
                     -- DROP EXISTING TABLES TO ENSURE CLEAN START --
-- Drop the final output table if it exists
DROP TABLE IF EXISTS BRIGHTTV; 

DROP TABLE IF EXISTS viewership_clean;
DROP TABLE IF EXISTS users_clean;
DROP TABLE IF EXISTS user_viewership;
DROP TABLE IF EXISTS viewership_final;
DROP TABLE IF EXISTS day_summary;
DROP TABLE IF EXISTS average_sessions;
DROP TABLE IF EXISTS province_top_channel;

--------------------------------------------------------------------------------
            -- CLEAN VIEWERSHIP (REMOVE DUPES + FIX UTC→SA) --
-- Use the explicit format 'YYYY-MM-DD HH: MI' to ensure correct timestamp conversion
CREATE OR REPLACE TEMPORARY TABLE viewership_clean AS
SELECT DISTINCT
    userid,
    channel2 AS channel,
    duration2,
    -- Apply the correct format to convert string to UTC timestamp
    TRY_TO_TIMESTAMP(recorddate2, 'YYYY/MM/DD HH24: MI') AS recorddate_utc,
    -- Convert the valid UTC timestamp to the SA time zone
    CONVERT_TIMEZONE('UTC','Africa/Johannesburg', recorddate_utc) AS recorddate_sa
    FROM VIEWERSHIP;

--------------------------------------------------------------------------------
                         -- CLEAN USERS -- 
CREATE OR REPLACE TEMPORARY TABLE users_clean AS
SELECT
    userid,
    IFNULL(name, 'None') AS name,
    IFNULL(surname, 'None') AS surname,
    IFNULL(email, 'None') AS email,
    IFNULL(gender, 'None') AS gender,
    IFNULL(race, 'None') AS race,
    IFNULL(province, 'None') AS province,
    IFNULL(social_media_handle, 'None') AS social_media_handle,
    IFNULL(age, 0) AS age,
    CASE
        WHEN age BETWEEN 1 AND 12 THEN 'Children'
        WHEN age BETWEEN 13 AND 19 THEN 'Teenagers'
        WHEN age BETWEEN 20 AND 35 THEN 'Young Adults'
        WHEN age BETWEEN 36 AND 59 THEN 'Adults'
        WHEN age >= 60 THEN 'Seniors'
        ELSE 'Not Specified'
    END AS age_group
FROM USERPROFILE;

--------------------------------------------------------------------------------
               -- MERGE USERS + VIEW SESSIONS --
-- All time-related columns will now populate correctly because recorddate_sa is a valid timestamp
CREATE OR REPLACE TEMPORARY TABLE user_viewership AS
SELECT
    u.userid,
    u.name,
    u.surname,
    u.gender,
    u.race,
    u.province,
    u.age_group,
    v.channel,
    v.duration2,
    v.recorddate_sa,

    -- Time parts
    TO_CHAR(v.recorddate_sa, 'DY') AS day_name,
    EXTRACT(MONTH FROM v.recorddate_sa) AS month,
    EXTRACT(YEAR FROM v.recorddate_sa) AS year,
    TO_CHAR(v.recorddate_sa, 'HH24:MI:SS') AS time_of_day,

    CASE
        WHEN TO_TIME(TO_CHAR(v.recorddate_sa,'HH24:MI:SS')) BETWEEN '06:00:00' AND '11:59:59' THEN 'Morning'
        WHEN TO_TIME(TO_CHAR(v.recorddate_sa,'HH24:MI:SS')) BETWEEN '12:00:00' AND '17:59:59' THEN 'Afternoon'
        WHEN TO_TIME(TO_CHAR(v.recorddate_sa,'HH24:MI:SS')) BETWEEN '18:00:00' AND '23:59:59' THEN 'Evening'
        ELSE 'Night'
    END AS time_type

FROM users_clean u
JOIN viewership_clean v 
    ON u.userid = v.userid;

--------------------------------------------------------------------------------
                  -- WATCH DURATION CATEGORY --
CREATE OR REPLACE TEMPORARY TABLE viewership_final AS
SELECT
    *,
    
    -- Convert duration string HH:MM:SS into total seconds safely
    SPLIT_PART(duration2, ':', 1)::INT * 3600 +  -- hours
    SPLIT_PART(duration2, ':', 2)::INT * 60 +    -- minutes
    SPLIT_PART(duration2, ':', 3)::INT AS duration_seconds,
    
    -- Convert seconds to hours
    (SPLIT_PART(duration2, ':', 1)::INT * 3600 +
     SPLIT_PART(duration2, ':', 2)::INT * 60 +
     SPLIT_PART(duration2, ':', 3)::INT) / 3600 AS watch_hours,
    
    -- Categorize into watch duration
    CASE
        WHEN duration2 IS NULL THEN 'Unknown'
        WHEN (SPLIT_PART(duration2, ':', 1)::INT * 3600 +
              SPLIT_PART(duration2, ':', 2)::INT * 60 +
              SPLIT_PART(duration2, ':', 3)::INT) BETWEEN 0 AND 10799 THEN '0 - 3 Hrs'
        WHEN (SPLIT_PART(duration2, ':', 1)::INT * 3600 +
              SPLIT_PART(duration2, ':', 2)::INT * 60 +
              SPLIT_PART(duration2, ':', 3)::INT) BETWEEN 10800 AND 21599 THEN '3 - 6 Hrs'
        WHEN (SPLIT_PART(duration2, ':', 1)::INT * 3600 +
              SPLIT_PART(duration2, ':', 2)::INT * 60 +
              SPLIT_PART(duration2, ':', 3)::INT) BETWEEN 21600 AND 32399 THEN '6 - 9 Hrs'
        ELSE '9+ Hrs'
    END AS watch_duration

FROM user_viewership;


--------------------------------------------------------------------------------
                  -- LEAST ACTIVE DAYS --
CREATE OR REPLACE TEMPORARY TABLE day_summary AS
SELECT 
    day_name,
    COUNT(*) AS sessions
FROM viewership_final
GROUP BY day_name;

CREATE OR REPLACE TEMPORARY TABLE average_sessions AS
SELECT AVG(sessions) AS avg_sessions
FROM day_summary;

--------------------------------------------------------------------------------
                  -- TOP CHANNEL PER PROVINCE --
CREATE OR REPLACE TEMPORARY TABLE province_top_channel AS
SELECT
    province,
    channel,
    COUNT(*) AS views,
    ROW_NUMBER() OVER (PARTITION BY province ORDER BY COUNT(*) DESC) AS rank
FROM viewership_final
GROUP BY province, channel
QUALIFY rank = 1;

--------------------------------------------------------------------------------
              -- FINAL TABLE: BRIGHTTV --
CREATE OR REPLACE TABLE BRIGHTTV AS
SELECT
    v.userid,
    v.name,
    v.surname,
    v.gender,
    v.race,
    v.province,
    v.age_group,
    v.channel,
    v.day_name,
    v.month,
    v.year,
    v.time_of_day,
    v.time_type,
    v.watch_duration,
    v.watch_hours,

    d.sessions AS total_sessions_day,
    CASE 
        WHEN d.sessions < a.avg_sessions THEN 'Yes'
        ELSE 'No'
    END AS low_consumption_flag,

    p.channel AS recommended_content,
    p.views AS recommended_content_views

FROM viewership_final v
LEFT JOIN day_summary d 
    ON v.day_name = d.day_name
CROSS JOIN average_sessions a
LEFT JOIN province_top_channel p
    ON v.province = p.province
ORDER BY v.province, v.channel, v.day_name;
--------------------------------------------------------------------------------------------------
                                    ---Viewing the entire table --
SELECT *
FROM BRIGHTTV;
