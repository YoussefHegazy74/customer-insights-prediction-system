-- 5- DimTime
USE ChurnDWH;

CREATE TABLE DimTime (
    TimeKey INT PRIMARY KEY,                -- Format: YYYYMMDD
    FullDate DATE NOT NULL,
    [Day] INT NOT NULL,
    [Month] INT NOT NULL,
    [Year] INT NOT NULL,
    [Quarter] INT NOT NULL,
    [WeekOfYear] INT NOT NULL,
    [MonthName] NVARCHAR(20) NOT NULL,
    [DayName] NVARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL
);

-- Query for inserting Data
DECLARE @Date DATE = '2020-01-01';
WHILE @Date <= '2030-12-31'
BEGIN
    INSERT INTO DimTime (
        TimeKey, FullDate, [Day], [Month], [Year],
        [Quarter], [WeekOfYear], [MonthName], [DayName], IsWeekend
    )
    VALUES (
        CONVERT(INT, FORMAT(@Date, 'yyyyMMdd')),
        @Date,
        DAY(@Date),
        MONTH(@Date),
        YEAR(@Date),
        DATEPART(QUARTER, @Date),
        DATEPART(WEEK, @Date),
        DATENAME(MONTH, @Date),
        DATENAME(WEEKDAY, @Date),
        CASE WHEN DATEPART(WEEKDAY, @Date) IN (1,7) THEN 1 ELSE 0 END
    );
    SET @Date = DATEADD(DAY, 1, @Date);
END;
GO