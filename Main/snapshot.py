from datetime import timedelta, datetime

from STOCK.ConfigureFiles.config import conn
cursor = conn.cursor()


def gettradingDates():
    cursor.execute("SELECT date FROM trading_days")

    dates = cursor.fetchall()


    tradeDates = [item['date'] for item in dates]

    return tradeDates


def getStartDate():
    cursor.execute("SELECT max(date) FROM member_snapshot")


    date = cursor.fetchall()

    new_dat = date[0]

    return new_dat['max(date)']



def getEndDate():
    cursor.execute("SELECT max(date) from member_close_buy_sell")

    date = cursor.fetchall()
    new_dat = date[0]

    return new_dat['max(date)']


def memberSnapshot():
    data = cursor.execute("""

insert into member_snapshot
WITH cte1 AS
(
	(SELECT
		mbs.date AS mbs_date,
		mbs.member_id AS mbs_member_id,
		mbs.symbol AS mbs_symbol,
		mbs.action AS mbs_action,
		mbs.close AS mbs_close,
		mbs.qty AS mbs_qty,
		ms.date,
		ms.member_id,
		ms.symbol,
		ms.action,
		ms.close,
		ms.qty,
		ms.invested_deducted,
		ms.remaining_qty,
        ms.total_buy_qty AS prev_total_buy_qty,
        ms.total_sell_qty AS prev_total_sell_qty,
        ms.current_price,
		ms.average_price,
		ms.current_invested,
		ms.total_investment AS prev_total_investment,
		ms.total_sell AS prev_total_sell,
		ms.profit,
		ms.net_profit
	FROM member_close_buy_sell mbs
	LEFT JOIN member_snapshot ms
	ON COALESCE((SELECT next_day FROM temp_day WHERE current_day = ms.date),ms.date + INTERVAL 1 day) = mbs.date
	AND ms.member_id = mbs.member_id
	AND ms.symbol = mbs.symbol
	WHERE mbs.date =  COALESCE((SELECT next_day FROM temp_day WHERE current_day =  (SELECT max(date) FROM member_snapshot)) ,'2023-01-02')
)
	UNION
(	
	SELECT
		mbs.date AS mbs_date,
		mbs.member_id AS mbs_member_id,
		mbs.symbol AS mbs_symbol,
		mbs.action AS mbs_action,
		mbs.close AS mbs_close,
		mbs.qty AS mbs_qty,
		ms.date,
		ms.member_id,
		ms.symbol,
		ms.action,
		ms.close,
		ms.qty,
		ms.invested_deducted,
		ms.remaining_qty,
        ms.total_buy_qty AS prev_total_buy_qty,
        ms.total_sell_qty AS prev_total_sell_qty,
        ms.current_price,
		ms.average_price,
		ms.current_invested,
		ms.total_investment AS prev_total_investment,
		ms.total_sell AS prev_total_sell,
		ms.profit,
		ms.net_profit
	FROM member_close_buy_sell mbs
	RIGHT JOIN member_snapshot ms
	ON COALESCE((SELECT next_day FROM temp_day WHERE current_day = ms.date),ms.date + INTERVAL 1 day)  = mbs.date
	AND  ms.member_id = mbs.member_id
	AND  ms.symbol = mbs.symbol
	WHERE ms.date = COALESCE ((SELECT current_day FROM temp_day WHERE current_day = (SELECT max(date) FROM member_snapshot) ),"2023-01-01")
)
)
, cte2 AS (
	SELECT
		COALESCE(cte1.mbs_date, COALESCE((SELECT next_day FROM temp_day WHERE current_day = cte1.date),cte1.date + INTERVAL 1 DAY) ) AS date,
		COALESCE(cte1.mbs_member_id, cte1.member_id) AS member_id,
		COALESCE(cte1.mbs_symbol, cte1.symbol) AS symbol,
		cte1.mbs_action AS action,
		COALESCE(cte1.mbs_close,st.close) AS close,
		COALESCE(cte1.mbs_qty,0) AS qty,
		CASE
			WHEN mbs_action = "BUY" THEN (cte1.mbs_qty * cte1.mbs_close)
			WHEN mbs_action = "SELL" THEN -(cte1.mbs_qty * cte1.mbs_close)
			ELSE 0
		END AS invested_deducted,
		COALESCE(cte1.remaining_qty ,0) AS prev_remaining_qty,
        coalesce(cte1.prev_total_buy_qty,0) AS prev_total_buy_qty,
        coalesce(cte1.prev_total_sell_qty,0) AS prev_total_sell_qty,
        cte1.current_price as current_price,
		cte1.average_price AS average_price,
		cte1.current_invested AS current_invested,
		COALESCE(cte1.prev_total_investment,0) AS prev_total_investment,
		COALESCE(cte1.prev_total_sell,0) AS prev_total_sell,
		COALESCE(cte1.profit,0) AS profit,
		COALESCE(cte1.net_profit,0) AS net_profit
	FROM cte1
	LEFT JOIN stock_daily_staging st
	ON st.date  =  COALESCE((SELECT next_day FROM temp_day WHERE current_day = cte1.date),cte1.date + INTERVAL 1 DAY)
	AND st.symbol = cte1.symbol
	)
, cte3 AS (	
	SELECT
		date,
		member_id,
		symbol,
		qty,
		action,
		close,
        prev_remaining_qty,
        invested_deducted,
		CASE
			WHEN action = "BUY" THEN (prev_remaining_qty + qty)
			WHEN action = "SELL" THEN (prev_remaining_qty - qty)
			ELSE prev_remaining_qty
		END AS remaining_qty,
        prev_total_buy_qty,
        prev_total_sell_qty,
        current_price,
		CASE
    WHEN action = "BUY" AND (prev_remaining_qty + qty) > 0 THEN  
        (((prev_remaining_qty * COALESCE(average_price,0)) + (qty * close)) / (prev_remaining_qty + qty))
    ELSE COALESCE(average_price, 0)
END AS average_price,
prev_total_investment,
		prev_total_sell,
		profit,
		net_profit
	FROM cte2)
SELECT
    cte3.date,                  -- 1: date
    member_id,                  -- 2: member_id
    symbol,                     -- 3: symbol
    qty,                        -- 4: qty
    action,                     -- 5: action
    close,                      -- 6: close
    prev_remaining_qty,          -- 7: prev_remaining_qty
    -- ADD THIS MISSING COLUMN
    remaining_qty,               -- 8: remaining_qty
    invested_deducted,           -- 9: invested_deducted
    CASE                        
        WHEN action = "BUY" THEN (qty + prev_total_buy_qty)
        ELSE prev_total_buy_qty
    END AS total_buy_qty,        -- 10: total_buy_qty
    CASE                        
        WHEN action = "SELL" THEN (qty + prev_total_sell_qty)
        ELSE prev_total_sell_qty
    END AS total_sell_qty,       -- 11: total_sell_qty
    (remaining_qty * close) AS current_price,  -- 12: current_price
    average_price,                              -- 13: average_price
    COALESCE((average_price * remaining_qty),0) AS current_invested,  -- 14: current_invested
    CASE                                        
        WHEN action = "BUY" THEN (qty * close) + prev_total_investment
        ELSE prev_total_investment
    END AS total_investment,     -- 15: total_investment
    CASE                                        
        WHEN action = "SELL" THEN (qty * close) + prev_total_sell
        ELSE prev_total_sell
    END AS total_sell,           -- 16: total_sell
    (remaining_qty * (close - average_price)) AS profit,  -- 17: profit
    ((CASE                                       
        WHEN action = "SELL" THEN (qty * close) + prev_total_sell
        ELSE prev_total_sell
    END) + (average_price * remaining_qty) + (remaining_qty * (close - average_price)) 
    - (CASE
        WHEN action = "BUY" THEN (qty * close) + prev_total_investment
        ELSE prev_total_investment
    END)) AS net_profit,          -- 18: net_profit
    CURRENT_TIMESTAMP AS current_timestamp_col   -- 19: current_timestamp_col
FROM cte3;


    """)

    conn.commit()


def main():
    startDate = getStartDate()
    endDate = getEndDate()

    startDate += timedelta(days=1)

    dates = gettradingDates()

    print(dates)
    print(startDate, endDate)

    while startDate < endDate:
        if startDate in dates:
            print("inserting data for --- ", startDate)
            memberSnapshot()
        startDate += timedelta(days=1)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
