CREATE SCHEMA CONSUME;

-- Base table for TOTD data. Some joins required to find tags, full authors list, leaderboards, etc.
CREATE TABLE CONSUME.TOTD (
    TOTD_DATE DATE,
    TOTD_DAY_OF_WEEK VARCHAR(10),
    EXCHANGE_ID INTEGER,
    MAP_NAME VARCHAR(200),
    PRIMARY_STYLE VARCHAR(50),
    IS_MULTILAP_FLAG BOOLEAN,
    PRIMARY_AUTHOR_USER_ID VARCHAR(50),
    AUTHOR_TIME FLOAT,
    GOLD_TIME FLOAT,
    SILVER_TIME FLOAT,
    BRONZE_TIME FLOAT,
    UPLOADED_DATE DATE,
    DAYS_BEFORE_TOTD INTEGER,
    UNIQUE(TOTD_DATE, EXCHANGE_ID)
);

-- Table for all tags associated with a TOTD map
CREATE TABLE CONSUME.TOTD_TAGS (
    EXCHANGE_ID INTEGER,
    TAG_NAME VARCHAR(50),
    UNIQUE(EXCHANGE_ID, TAG_NAME)
);

-- View for information about TOTD authors. Not a table because we don't need any ETL from conform
CREATE VIEW CONSUME.TOTD_AUTHORS AS
SELECT
    TRACK_ID AS EXCHANGE_ID,
    USER_ID,
    USERNAME,
    AUTHOR_ROLE
FROM CONFORM.TMX_AUTHORS;

-- Table for all TOTD world records. Saves a lot of painful aggregate queries
CREATE TABLE CONSUME.TOTD_WORLDRECORDS(
    EXCHANGE_ID INTEGER,
    USERNAME VARCHAR(50),
    USER_ID VARCHAR(50),
    REPLAY_TIME FLOAT,
    DRIVEN_DATE DATE,
    UNIQUE(EXCHANGE_ID, USER_ID)
);

