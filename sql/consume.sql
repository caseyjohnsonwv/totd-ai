CREATE SCHEMA CONSUME;

CREATE TABLE consume.totd (
    TOTD_DATE DATE,
    TOTD_DAY_OF_WEEK VARCHAR(10),
    EXCHANGE_ID INTEGER,
    TRACK_NAME VARCHAR(200),
    PRIMARY_STYLE VARCHAR(50),
    IS_MULTILAP_FLAG BOOLEAN,
    WR_USERNAME VARCHAR(50),
    WR_TIME FLOAT,
    PRIMARY_AUTHOR_USERNAME VARCHAR(50),
    AUTHOR_TIME FLOAT,
    GOLD_TIME FLOAT,
    SILVER_TIME FLOAT,
    BRONZE_TIME FLOAT,
    UPLOADED_DATE DATE,
    DAYS_BEFORE_TOTD INTEGER,
    UNIQUE(TOTD_DATE, EXCHANGE_ID)
);