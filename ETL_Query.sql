-- -------ETL------- --
DROP FUNCTION IF EXISTS exctract_period(text, text);
CREATE OR REPLACE FUNCTION exctract_period(_months text, _weeks text DEFAULT 'DEFAULT')
-- Функция для получения периодов продаж по месяцам и неделям
  RETURNS TABLE (
    period_type TEXT,
    start_date TEXT,
    end_date TEXT,
  	sale_date DATE,
	salesman_id INT,
	item_id VARCHAR)
AS
$$
BEGIN
  IF $1 = 'month' AND $2 = 'week' THEN
    RETURN QUERY (
	    SELECT
		$2,
	    TO_CHAR((MIN(s.sale_date) OVER(PARTITION BY 
	    	CONCAT(EXTRACT(year FROM s.sale_date), EXTRACT(month FROM s.sale_date),
	    	EXTRACT(week FROM s.sale_date))))::date,'YYYY-MM-DD'),
	    TO_CHAR((MAX(s.sale_date) OVER(PARTITION BY 
		    CONCAT(EXTRACT(year FROM s.sale_date), EXTRACT(month FROM s.sale_date), 
		    EXTRACT(week FROM s.sale_date))))::date,'YYYY-MM-DD'),
		s.sale_date,
		s.salesman_id,
		s.item_id
	    FROM sales s ORDER BY s.sale_date, s.salesman_id);
  END IF;
  IF $1 = 'month' THEN
    RETURN QUERY (
		SELECT
		$1,
		TO_CHAR((MIN(s.sale_date) OVER(PARTITION BY 
			CONCAT(EXTRACT(year FROM s.sale_date), 
			EXTRACT(month FROM s.sale_date))))::date, 'YYYY-MM-DD'),
		TO_CHAR((MAX(s.sale_date) OVER(PARTITION BY 
			CONCAT(EXTRACT(year FROM s.sale_date), 
			EXTRACT(month FROM s.sale_date))))::date, 'YYYY-MM-DD'),
		s.sale_date,
		s.salesman_id,
		s.item_id
		FROM sales s ORDER BY s.sale_date, s.salesman_id);
  END IF;
END;
$$
LANGUAGE plpgsql;

WITH salesman_with_department AS(
	-- Добавление данных руководителей к данным продавцов
	SELECT *
	FROM salesman sm
		JOIN department dep ON sm.department_id = dep.department_id
),
all_merch AS(
	-- Объединение товаров и услуг в одну таблицу
	SELECT * FROM product
	UNION
	SELECT * FROM service
),
all_periods AS (
	-- Объединение периодов
	SELECT * FROM exctract_period('month', 'week')
	UNION 
	SELECT * FROM exctract_period('month')
)
-- -------Final Query------- --
SELECT DISTINCT ON (start_date, end_date, swd.fio)
	-- all_periods.sale_date,
	period_type,
	start_date,
	end_date,
	swd.fio,
	-- В Исходных данных нет ФИО руководителя, поэтому выводится id
	swd.dep_chif_id,
	SUM(s.quantity) OVER(PARTITION BY 
		CONCAT(period_type,start_date, end_date, swd.fio)) AS sales_count,
	SUM(s.final_price) OVER(PARTITION BY 
		CONCAT(period_type,start_date, end_date, swd.fio)) AS sales_sum,
	all_periods.item_id,
	all_merch.name,
	MAX((s.final_price / (s.quantity * all_merch.price)::float - 1) * 100
		)::numeric(10, 2) AS max_overcharge_percent,
	MAX(s.final_price - s.quantity * all_merch.price) AS max_overcharge
FROM all_periods
	JOIN all_merch ON all_merch.id = all_periods.item_id
	JOIN salesman_with_department swd ON all_periods.salesman_id = swd.id
	JOIN sales s ON all_periods.sale_date = s.sale_date
		AND all_periods.salesman_id = s.salesman_id 
		AND all_periods.item_id  = s.item_id
WHERE s.sale_date BETWEEN all_merch.sdate AND all_merch.edate
GROUP BY period_type,
	start_date,
	end_date,
	swd.fio,
	all_periods.item_id,
	all_merch.name,
	s.quantity,
	s.final_price,
	swd.dep_chif_id
ORDER BY swd.fio, start_date 