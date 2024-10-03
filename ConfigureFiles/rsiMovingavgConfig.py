
# Creating fact table of moving averages over 7,14,21,28 days moving averages
createTableMovingAverageFact = """

CREATE TABLE IF NOT EXISTS new_moving_average_fact(
rank_ INT,
date DATE,
symbol VARCHAR(25),
close DOUBLE,
avg_7_days DOUBLE,
avg_14_days DOUBLE,
avg_21_days DOUBLE,
avg_28_days DOUBLE
);
"""

# Truncate data from moving average fact table
truncateTableMovingAverageFact = """
TRUNCATE TABLE new_moving_average_fact;
"""

# This is the query that calculates the moving averages of 7, 14, 21, 28 days
# Source table :- daily_fact_stock table
# target table :- moving_average_fact table
calculateMovingAverage = f"""
                insert into new_moving_average_fact
                with cte as(
                select max(ds.date) as max_dt 
                from stock_daily_fact ds
                ),
                cte2 as (
                SELECT td.rank_ ,ds.DATE, SYMBOL, CLOSE,
                            AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7,
                            AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS moving_avg_14,
                            AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS moving_avg_21,
                            AVG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS moving_avg_28
                            from stock_daily_fact ds
                            join trading_days td on ds.date = td.date 
                            -- where ds.date between (select date_sub(cte.max_dt, interval 60 day) from cte) and (SELECT max_dt FROM cte)
                )
                select * from cte2
                -- where date > (select max(date) from new_moving_average_fact)
            """


# Creating rsi_index fact table for calculating rsi index
createTableRsiIndexFact = """
CREATE TABLE IF NOT EXISTS rsi_index_fact(
rank_ INT,
date_ DATE,
symbol VARCHAR(20),
close_ DOUBLE,
last_date_price DOUBLE,
curr_date_price DOUBLE,
changed DOUBLE,
loss DOUBLE,
gain DOUBLE,
avg_gain_7 DOUBLE,
avg_loss_7 DOUBLE,
avg_gain_14 DOUBLE,
avg_loss_14 DOUBLE,
avg_gain_21 DOUBLE,
avg_loss_21 DOUBLE,
avg_gain_28 DOUBLE,
avg_loss_28 DOUBLE,
rs_7 DOUBLE,
rs_14 DOUBLE,
rs_21 DOUBLE,
rs_28 DOUBLE,
rsi_7 DOUBLE,
rsi_14 DOUBLE,
rsi_21 DOUBLE,
rsi_28 DOUBLE
);
"""

# Truncate data from rsi_index_fact table
truncateTableRsiIndexFact = """
TRUNCATE TABLE rsi_index_fact;
"""

# This query calculated the rsi index over 7, 14, 21, 28 days
# source table :- daily_fact_stock
# target table :- rsi_index_fact
calculateRsiIndex = f"""
                INSERT INTO rsi_index_fact
                 with cte0 as(
                                select max(ds.date) as max_dt 
                                from stock_daily_fact ds
                ),
                cte AS (
                    SELECT 
                        td.Rank_,
                        ds.DATE, 
                        ds.SYMBOL, 
                        ds.CLOSE, 
                        ds.CLOSE AS last_date_price, 
                        LEAD(ds.CLOSE, 1) OVER (PARTITION BY ds.SYMBOL ORDER BY ds.DATE) AS curr_date_price
                    FROM 
                        new_moving_average_fact ds
                        JOIN trading_days td ON td.DATE = ds.DATE
                        -- where ds.date between (select date_sub(cte0.max_dt, interval 60 day) from cte0) and (select max_dt from cte0)
                ),
                cte2 AS (
                    SELECT 
                        *, 
                        (curr_date_price - last_date_price) AS changed,
                        CASE 
                            WHEN curr_date_price - last_date_price < 0 
                            THEN ABS(curr_date_price - last_date_price)  
                            ELSE 0 
                        END AS Loss,
                        CASE 
                            WHEN curr_date_price - last_date_price > 0 
                            THEN curr_date_price - last_date_price 
                            ELSE 0 
                        END AS Gain
                    FROM 
                        cte
                ),
                cte3 AS (
                    SELECT 
                        *,
                        AVG(Gain) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Avg_Gain_7,
                        AVG(Loss) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Avg_Loss_7,
                        AVG(Gain) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS Avg_Gain_14,
                        AVG(Loss) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS Avg_Loss_14,
                        AVG(Gain) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS Avg_Gain_21,
                        AVG(Loss) OVER (PARTITION BY SYMBOL ORDER BY DATE ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS Avg_Loss_21,
                        AVG(Gain) OVER (partition by SYMBOL order by DATE rows between 27 preceding and current row) as Avg_Gain_28,
                        AVG(Loss) over (partition by SYMBOL order by DATE rows between 27 preceding and current row) as Avg_Loss_28
                    FROM 
                        cte2
                ),
                cte4 AS (
                    SELECT 
                        *,
                        COALESCE((Avg_Gain_7 / NULLIF(Avg_Loss_7, 0)), 0) AS rs_7,
                        COALESCE((Avg_Gain_14 / NULLIF(Avg_Loss_14, 0)), 0) AS rs_14,
                        COALESCE((Avg_Gain_21 / NULLIF(Avg_Loss_21, 0)), 0) AS rs_21,
                        coalesce ((Avg_Gain_28 / nullif(Avg_Loss_28, 0)), 0) as rs_28
                    FROM 
                        cte3
                )
                SELECT 
                    *,
                    COALESCE(100 - (100 / (1 + rs_7)), 0) AS rsi_7,
                    COALESCE(100 - (100 / (1 + rs_14)), 0) AS rsi_14,
                    COALESCE(100 - (100 / (1 + rs_21)), 0) AS rsi_21,
                    coalesce(100 - (100 / (1 + rs_28)), 0) as rsi_28
                FROM 
                    cte4
                    -- where date > (select max(date) from rsi_index_fact )
            """

