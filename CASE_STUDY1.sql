SELECT calendar_dt, cust_id, MVM_STATUS
FROM
        (
            SELECT calendar_dt, cust_id, MVM_STATUS,
                        row_number()OVER(PARTITION BY calendar_dt, cust_id, MVM_STATUS ORDER BY cust_id) AS SELECT_ONE_ROW
            FROM 
                    (
                        SELECT calendar_dt,cust_id,
                                    CASE
                                            WHEN last_value = avg THEN 'N'
                                            WHEN (STDD+avg) >last_value AND last_value > avg THEN 'U'
                                            WHEN last_value > (STDD+avg) THEN 'HU'
                                            WHEN (avg-STDD) <last_value AND last_value < avg THEN 'R'
                                            WHEN last_value <(avg-STDD) THEN 'HR'
                                    END AS MVM_STATUS

                        FROM
                                (
                                     SELECT
                                            calendar_dt,cust_id,recharge_dt,NEXT_DATE,N_date, recharge_amt_num  , AMT_PER_DAY,AMT_perday_withlast,
                                            ROUND(AVG(AMT_PER_DAY)OVER(PARTITION BY cust_id) ,2)AS avg,
                                            ROUND(STDDEV(AMT_PER_DAY)OVER(PARTITION BY cust_id) ,2)AS STDD,
                                            LAST_VALUE(AMT_perday_withlast)
                                            OVER(PARTITION BY cust_id ORDER BY cust_id,recharge_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED  FOLLOWING  ) as last_value
                                      FROM
                                                        (
                                                                SELECT
                                                                             calendar_dt,cust_id,recharge_dt,NEXT_DATE,N_date, recharge_amt_num  ,
                                                                             ROUND(recharge_amt_num/ (NEXT_DATE-recharge_dt),2) AS AMT_PER_DAY,
                                                                             ROUND(recharge_amt_num/ (N_date-recharge_dt),2) AS AMT_perday_withlast
                                                                FROM
                                                                         (  SELECT 
                                                                                    calendar_dt,cust_id,recharge_dt,
                                                                                    lead(recharge_dt,1)OVER(PARTITION BY cust_id ORDER BY cust_id,recharge_dt ) as NEXT_DATE,
                                                                                     lead(recharge_dt,1,calendar_dt)OVER(PARTITION BY cust_id ORDER BY cust_id,recharge_dt ) as N_date,
                                                                                            recharge_amt_num  
                                                                            FROM customer_case1
                                                                            ORDER BY cust_id,recharge_dt
                                                                            )
                                                        )
                                )
                    )
        )
WHERE SELECT_ONE_ROW=1