USE metro_dwh;
SET sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''));

-- Query1
SELECT 
    p.ProductName,
    MONTH(t.TransactionDate) AS TransactionMonth,
    CASE 
        WHEN t.TransactionDay IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END AS DayType,
    SUM(s.Sales) AS TotalRevenue
FROM 
    Sales s
JOIN 
    Product p ON s.ProductID = p.ProductID
JOIN 
    Transaction_Time t ON s.TimePK = t.TimePK
WHERE 
    t.TransactionYear = 2019
GROUP BY 
    p.ProductName, TransactionMonth, DayType
ORDER BY 
    TransactionMonth, DayType, TotalRevenue DESC
LIMIT 5;



-- Query 2
WITH QuarterlyRevenue AS (
    SELECT 
        s.StoreID,
        st.StoreName,
        t.TransactionYear,
        t.TransactionQuarter,
        SUM(s.Sales) AS TotalRevenue
    FROM 
        Sales s
    JOIN 
        Store st ON s.StoreID = st.StoreID
    JOIN 
        Transaction_Time t ON s.TimePK = t.TimePK
    WHERE 
        t.TransactionYear = 2019
    GROUP BY 
        s.StoreID, st.StoreName, t.TransactionYear, t.TransactionQuarter
),
QuarterlyGrowth AS (
    SELECT 
        q1.StoreID,
        q1.StoreName,
        q1.TransactionQuarter AS CurrentQuarter,
        q1.TotalRevenue AS CurrentRevenue,
        q2.TotalRevenue AS PreviousRevenue,
        CASE 
            WHEN q2.TotalRevenue IS NOT NULL THEN 
                ((q1.TotalRevenue - q2.TotalRevenue) / q2.TotalRevenue) * 100
            ELSE 
                NULL
        END AS GrowthRate
    FROM 
        QuarterlyRevenue q1
    LEFT JOIN 
        QuarterlyRevenue q2
    ON 
        q1.StoreID = q2.StoreID
        AND q1.TransactionYear = q2.TransactionYear
        AND q1.TransactionQuarter = q2.TransactionQuarter + 1
)
SELECT 
    StoreID,
    StoreName,
    CurrentQuarter,
    CurrentRevenue,
    PreviousRevenue,
    GrowthRate
FROM 
    QuarterlyGrowth
ORDER BY 
    StoreID, CurrentQuarter;



-- Query3
SELECT 
    st.StoreName AS Store,
    sp.SupplierName AS Supplier,
    p.ProductName AS Product,
    SUM(s.Sales) AS TotalSales
FROM 
    Sales s
JOIN 
    Store st ON s.StoreID = st.StoreID
JOIN 
    Supplier sp ON s.SupplierID = sp.SupplierID
JOIN 
    Product p ON s.ProductID = p.ProductID
GROUP BY 
    st.StoreName, sp.SupplierName, p.ProductName
ORDER BY 
    st.StoreName, sp.SupplierName, p.ProductName;



-- Query4
SELECT 
    p.ProductName AS Product,
    CASE 
        WHEN MONTH(t.TransactionDate) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(t.TransactionDate) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(t.TransactionDate) IN (9, 10, 11) THEN 'Fall'
        WHEN MONTH(t.TransactionDate) IN (12, 1, 2) THEN 'Winter'
    END AS Season,
    SUM(s.Sales) AS TotalSales
FROM 
    Sales s
JOIN 
    Product p ON s.ProductID = p.ProductID
JOIN 
    Transaction_Time t ON s.TimePK = t.TimePK
GROUP BY 
    p.ProductName, Season
ORDER BY 
    p.ProductName, 
    FIELD(Season, 'Spring', 'Summer', 'Fall', 'Winter');



-- Query5
WITH MonthlyRevenue AS (
    SELECT 
        s.StoreID,
        st.StoreName,
        s.SupplierID,
        sp.SupplierName,
        YEAR(t.TransactionDate) AS Year,
        MONTH(t.TransactionDate) AS Month,
        SUM(s.Sales) AS TotalRevenue
    FROM 
        Sales s
    JOIN 
        Store st ON s.StoreID = st.StoreID
    JOIN 
        Supplier sp ON s.SupplierID = sp.SupplierID
    JOIN 
        Transaction_Time t ON s.TimePK = t.TimePK
    GROUP BY 
        s.StoreID, st.StoreName, s.SupplierID, sp.SupplierName, Year, Month
),
MonthlyVolatility AS (
    SELECT 
        mr1.StoreID,
        mr1.StoreName,
        mr1.SupplierID,
        mr1.SupplierName,
        mr1.Year,
        mr1.Month AS CurrentMonth,
        mr1.TotalRevenue AS CurrentRevenue,
        mr2.TotalRevenue AS PreviousRevenue,
        CASE 
            WHEN mr2.TotalRevenue IS NOT NULL THEN 
                ((mr1.TotalRevenue - mr2.TotalRevenue) / mr2.TotalRevenue) * 100
            ELSE 
                NULL
        END AS RevenueVolatility
    FROM 
        MonthlyRevenue mr1
    LEFT JOIN 
        MonthlyRevenue mr2
    ON 
        mr1.StoreID = mr2.StoreID
        AND mr1.SupplierID = mr2.SupplierID
        AND mr1.Year = mr2.Year
        AND mr1.Month = mr2.Month + 1
)
SELECT 
    StoreID,
    StoreName,
    SupplierID,
    SupplierName,
    Year,
    CurrentMonth,
    CurrentRevenue,
    PreviousRevenue,
    RevenueVolatility
FROM 
    MonthlyVolatility
ORDER BY 
    StoreID, SupplierID, Year, CurrentMonth;



-- Query6
SELECT 
    p1.ProductName AS ProductA,
    p2.ProductName AS ProductB,
    COUNT(*) AS Frequency
FROM 
    Sales s1
JOIN 
    Sales s2 ON s1.OrderID = s2.OrderID AND s1.ProductID < s2.ProductID
JOIN 
    Product p1 ON s1.ProductID = p1.ProductID
JOIN 
    Product p2 ON s2.ProductID = p2.ProductID
GROUP BY 
    p1.ProductName, p2.ProductName
ORDER BY 
    Frequency DESC
LIMIT 5;



-- Query7
SELECT 
    st.StoreName AS Store,
    sp.SupplierName AS Supplier,
    p.ProductName AS Product,
    t.TransactionYear AS Year,
    SUM(s.Sales) AS TotalRevenue
FROM 
    Sales s
JOIN 
    Store st ON s.StoreID = st.StoreID
JOIN 
    Supplier sp ON s.SupplierID = sp.SupplierID
JOIN 
    Product p ON s.ProductID = p.ProductID
JOIN 
    Transaction_Time t ON s.TimePK = t.TimePK
GROUP BY 
    ROLLUP (st.StoreName, sp.SupplierName, p.ProductName, t.TransactionYear)
ORDER BY 
    st.StoreName, sp.SupplierName, p.ProductName, t.TransactionYear;



-- Query8
SELECT 
    p.ProductName AS Product,
    CASE 
        WHEN MONTH(t.TransactionDate) BETWEEN 1 AND 6 THEN 'H1'
        WHEN MONTH(t.TransactionDate) BETWEEN 7 AND 12 THEN 'H2'
        ELSE 'Yearly Total'
    END AS TimePeriod,
    YEAR(t.TransactionDate) AS Year,
    SUM(s.Sales) AS TotalRevenue,
    SUM(s.Quantity) AS TotalQuantity
FROM 
    Sales s
JOIN 
    Product p ON s.ProductID = p.ProductID
JOIN 
    Transaction_Time t ON s.TimePK = t.TimePK
GROUP BY 
    p.ProductName, Year, TimePeriod
ORDER BY 
    p.ProductName, Year, FIELD(TimePeriod, 'H1', 'H2', 'Yearly Total');



-- Query9
WITH DailyAverageSales AS (
    SELECT 
        p.ProductName AS Product,
        t.TransactionDate AS SaleDate,
        SUM(s.Sales) / COUNT(DISTINCT t.TransactionDate) AS DailyAverageSales
    FROM 
        Sales s
    JOIN 
        Product p ON s.ProductID = p.ProductID
    JOIN 
        Transaction_Time t ON s.TimePK = t.TimePK
    GROUP BY 
        p.ProductName, t.TransactionDate
),
SalesWithOutliers AS (
    SELECT 
        p.ProductName AS Product,
        t.TransactionDate AS SaleDate,
        SUM(s.Sales) AS TotalSales,
        da.DailyAverageSales,
        CASE 
            WHEN SUM(s.Sales) > (2 * da.DailyAverageSales) THEN 'Outlier'
            ELSE 'Normal'
        END AS SalesStatus
    FROM 
        Sales s
    JOIN 
        Product p ON s.ProductID = p.ProductID
    JOIN 
        Transaction_Time t ON s.TimePK = t.TimePK
    JOIN 
        DailyAverageSales da ON p.ProductName = da.Product AND t.TransactionDate = da.SaleDate
    GROUP BY 
        p.ProductName, t.TransactionDate, da.DailyAverageSales
)
SELECT 
    Product,
    SaleDate,
    TotalSales,
    DailyAverageSales,
    SalesStatus
FROM 
    SalesWithOutliers
ORDER BY 
    Product, SaleDate;



-- Query10
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    st.StoreName AS Store,
    QUARTER(t.TransactionDate) AS Quarter,
    YEAR(t.TransactionDate) AS Year,
    SUM(s.Sales) AS TotalSales
FROM 
    Sales s
JOIN 
    Store st ON s.StoreID = st.StoreID
JOIN 
    Transaction_Time t ON s.TimePK = t.TimePK
GROUP BY 
    st.StoreName, QUARTER(t.TransactionDate), YEAR(t.TransactionDate)
ORDER BY 
    st.StoreName, Year, Quarter;