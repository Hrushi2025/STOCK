# snapShotConfig.py

def get_sql_queries():
    return [
#         """
# WITH FirstLastTransactionDates AS (
#     -- Get the first and last transaction dates for each member_id and symbol combination
#     SELECT
#         member_id,
#         symbol,
#         MIN(date) AS first_transaction_date,
#         MAX(date) AS last_transaction_date
#     FROM
#         trans
#     WHERE
#         action IN ('BUY', 'SELL')
#     GROUP BY
#         member_id, symbol
# ),
# AllTransactionDays AS (
#     -- Select all transaction days (BUY/SELL) and generate missing days in a single step
#     SELECT
#         td.date AS transaction_date,
#         ftd.member_id,
#         ftd.symbol,
#         COALESCE(tbs.action, NULL) AS action,
#         COALESCE(tbs.qty, 0) AS qty
#     FROM
#         trading_days td
#     JOIN
#         FirstLastTransactionDates ftd ON td.date BETWEEN ftd.first_transaction_date AND ftd.last_transaction_date
#     LEFT JOIN
#         trans tbs ON td.date = tbs.date AND ftd.member_id = tbs.member_id AND ftd.symbol = tbs.symbol
#     WHERE
#         td.date >= ftd.first_transaction_date
# ),
# PriceData AS (
#     -- Get the closing price for each transaction and missing day based on stock_daily_fact
#     SELECT
#         atd.transaction_date,
#         atd.member_id,
#         atd.symbol,
#         atd.action,
#         atd.qty,
#         COALESCE(sd.CLOSE, 0) AS current_price
#     FROM
#         AllTransactionDays atd
#     LEFT JOIN
#         stock_daily_fact sd ON atd.transaction_date = sd.DATE AND atd.symbol = sd.symbol
# )
#
# -- Final Output: Select data and include row count in the output
# SELECT
#     transaction_date,
#     member_id,
#     symbol,
#     action,
#     qty,
#     current_price -- Subquery to count total rows
# FROM
#     PriceData
# ORDER BY
#     member_id, symbol, transaction_date;

        """
SET @prev_member_id = NULL;
        """,
        """
SET @prev_symbol = NULL;
        """,
        """
SET @running_qty = 0;
        """,
        """

UPDATE trans AS yt
JOIN (
    SELECT
        MEMBER_ID,
        SYMBOL,
        DATE,
        ACTION,
        QTY,
        -- Calculate the cumulative remaining quantity
        @running_qty := CASE
            WHEN @prev_member_id = MEMBER_ID AND @prev_symbol = SYMBOL THEN
                CASE
                    WHEN ACTION = 'BUY' THEN @running_qty + QTY  -- Increase running quantity on BUY
                    WHEN ACTION = 'SELL' THEN GREATEST(@running_qty - QTY, 0)  -- Decrease on SELL, but don't go below 0
                END
            ELSE
                CASE
                    WHEN ACTION = 'BUY' THEN QTY  -- If new member_id and symbol, start with the BUY quantity
                    WHEN ACTION = 'SELL' THEN 0    -- Start with 0 for SELL, since no remaining quantity exists
                END
        END AS REMAINING_QTY,
        -- Update tracking variables for the next iteration
        @prev_member_id := MEMBER_ID,
        @prev_symbol := SYMBOL
    FROM trans
    ORDER BY MEMBER_ID, SYMBOL, DATE
) AS calculated
ON yt.MEMBER_ID = calculated.MEMBER_ID AND yt.SYMBOL = calculated.SYMBOL AND yt.DATE = calculated.DATE
SET yt.REMAINING_QTY = calculated.REMAINING_QTY;

        """,
        """
        WITH cumulative_data AS (
            SELECT
                MEMBER_ID,
                SYMBOL,
                `DATE`,
                SUM(CASE WHEN ACTION = 'BUY' THEN ABS(QTY) ELSE 0 END)
                    OVER (PARTITION BY MEMBER_ID, SYMBOL ORDER BY `DATE`) AS total_buy_qty,
                SUM(CASE WHEN ACTION = 'SELL' THEN ABS(QTY) ELSE 0 END)
                    OVER (PARTITION BY MEMBER_ID, SYMBOL ORDER BY `DATE`) AS total_sell_qty
            FROM trans
        )
        UPDATE trans AS t
        JOIN cumulative_data AS c
        ON t.MEMBER_ID = c.MEMBER_ID AND t.SYMBOL = c.SYMBOL AND t.`DATE` = c.`DATE`
        SET t.total_buy_qty = c.total_buy_qty,
            t.total_sell_qty = c.total_sell_qty;
        """,
        """
        UPDATE trans
        SET current_price = remaining_qty * CLOSE
        WHERE current_price IS NULL;
        """,
        """
        WITH cumulative_investment AS (
            SELECT MEMBER_ID, SYMBOL, `DATE`,
                   SUM(CASE
                           WHEN ACTION = 'BUY' THEN qty * CLOSE
                           ELSE 0
                       END) OVER (PARTITION BY MEMBER_ID, SYMBOL ORDER BY `DATE`) AS total_investment,
                   SUM(CASE
                           WHEN ACTION = 'SELL' THEN ABS(QTY) * CLOSE
                           ELSE 0
                       END) OVER (PARTITION BY MEMBER_ID, SYMBOL ORDER BY `DATE`) AS total_sell
            FROM trans
        )
        UPDATE trans AS t
        JOIN cumulative_investment ci
        ON t.MEMBER_ID = ci.MEMBER_ID AND t.SYMBOL = ci.SYMBOL AND t.`DATE` = ci.`DATE`
        SET t.total_investment = ci.total_investment,
            t.total_sell = ci.total_sell;
        """,
        """
        UPDATE trans
        SET average_price =
            CASE
                WHEN (total_buy_qty - total_sell_qty) > 0 THEN
                    (total_investment ) / (total_buy_qty)
                ELSE 0
            END;
        """,
        """
        UPDATE trans
        SET current_investment = remaining_qty * average_price,
        profit = CASE
                    WHEN current_price - (total_investment - total_sell) < 0
                    THEN -ABS(current_price - (total_investment - total_sell))
                    ELSE current_price - (total_investment - total_sell)
                 END;
        """
    ]
#         """
#         WITH cte1 AS (
#     SELECT
#         date,
#         member_id,
#         symbol,
#         action,
#         qty,
#         CASE
#             WHEN action = 'BUY' THEN ABS(qty)
#             WHEN action = 'SELL' THEN -ABS(qty)
#             ELSE 0
#         END AS changed_qty,
#         SUM(CASE
#             WHEN action = 'BUY' THEN ABS(qty)
#             WHEN action = 'SELL' THEN -ABS(qty)
#             ELSE 0
#         END) OVER (
#             PARTITION BY member_id, symbol ORDER BY date
#             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
#         ) AS remaining_qty,
#         close,
#         (qty * close) AS invested_amt
#     FROM trans
#     ORDER BY symbol, date
# ),
# cte2 AS (
#     SELECT *,
#         COALESCE(LAG(remaining_qty) OVER (PARTITION BY member_id, symbol ORDER BY date), 0) AS prev_remain_qty,
#         AVG(close) OVER (PARTITION BY member_id, symbol ORDER BY date) AS avg_price
#     FROM cte1
# ),
# cte3 AS (
#     SELECT *,
#         COALESCE(LAG(avg_price) OVER (PARTITION BY member_id, symbol ORDER BY date), 0) AS prev_avg_price
#     FROM cte2
# ),
# final_cte AS (
#     SELECT
#         date,
#         member_id,
#         symbol,
#         action,
#         qty,
#         remaining_qty,
#         close,
#         CASE
#             WHEN action = 'BUY' THEN
#                 ((prev_remain_qty * prev_avg_price) + (qty * close)) / (prev_remain_qty + qty)
#             ELSE
#                 prev_avg_price
#         END AS average_price,
#         (CASE
#             WHEN (remaining_qty + qty) > 0 THEN
#                 ((prev_remain_qty * prev_avg_price) + (qty * close)) / (prev_remain_qty + qty)
#             ELSE
#                 0
#         END) * remaining_qty AS avg_current_investment
#     FROM cte3
# )
# -- Update the trans table with calculated values from final_cte using a join
# UPDATE trans AS tbs
# JOIN final_cte ON
#     tbs.date = final_cte.date
#     AND tbs.member_id = final_cte.member_id
#     AND tbs.symbol = final_cte.symbol
#     AND tbs.action = final_cte.action
# SET
#     tbs.average_price = final_cte.average_price,
#     tbs.avg_current_investment = final_cte.avg_current_investment;
#         """,
#         """
#     set SQL_SAFE_UPDATES = 0;
#         """,
#         """
# UPDATE trans
# SET invested_amt = remaining_qty * average_price;
#         """,
#         """
#     set SQL_SAFE_UPDATES = 0;
#         """,
#         """
#         UPDATE trans
#    SET Nprofit = (remaining_qty * close - average_price);
#         """
#     ]
