SELECT
                cust_id,Max_Consecutive_Days
FROM  (

            SELECT 
                        cust_id,Max_Consecutive_Days,
                        ROW_NUMBER()OVER(PARTITION BY cust_id, Max_Consecutive_Days ORDER BY cust_id) AS SELECT_ONE_ROW
            FROM 
                    (
                        SELECT 
                            cust_id,MAX(c)OVER (PARTITION BY cust_id ORDER BY cust_id  ) as Max_Consecutive_Days 
                        FROM (
                                       SELECT 
                                                    cust_id,calendar_dt,
                                                    COUNT(b)OVER (PARTITION BY cust_id,b ORDER BY cust_id,calendar_dt  ) as c
                                    
                                        FROM
                                                 (
                                                        SELECT 
                                                                     cust_id,calendar_dt,
                                                                   (calendar_dt- a) as b
                                                        FROM (
                                                                        SELECT 
                                                                                     cust_id,calendar_dt,
                                                                                      ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cust_id,calendar_dt  ) as a
                                                                        FROM customer_case3
                                                                        ORDER BY cust_id,calendar_dt
                                                                  )
                                                  )
                                  )
                    )

        )
WHERE SELECT_ONE_ROW=1