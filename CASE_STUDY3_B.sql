SELECT
            cust_id,calendar_dt,AMT_LE,AVG_transactions
FROM
        (
                SELECT 
                             cust_id,calendar_dt,AMT_LE,AVG_transactions,
                             ROW_NUMBER()OVER(PARTITION BY cust_id,AVG_transactions ORDER BY  cust_id) AS SELECT_ONE_ROW2
                FROM 
                        (
                                    SELECT
                                                 cust_id,calendar_dt,AMT_LE,C,B,
                                                 ROUND(AVG(B)OVER(PARTITION BY cust_id ),0) AS AVG_transactions
                                    FROM
                                            (
                                                    SELECT 
                                                                 cust_id,calendar_dt,AMT_LE,C,B,
                                                                 ROW_NUMBER()OVER(PARTITION BY cust_id,C ORDER BY  cust_id) AS SELECT_ONE_ROW
                                                    FROM
                                                            (
                                                                    SELECT
                                                                                cust_id,calendar_dt,AMT_LE,C,
                                                                                COUNT(C)OVER(PARTITION BY cust_id,C ORDER BY  cust_id) AS B
                                                                    FROM
                                                                            (
                                                                                    SELECT 
                                                                                               cust_id,calendar_dt,AMT_LE
                                                                                               --,SUM(AMT_LE) OVER(PARTITION BY cust_id ORDER BY  cust_id,calendar_dt ) AS b
                                                                                               ,TRUNC(SUM(AMT_LE) OVER(PARTITION BY cust_id ORDER BY  cust_id,calendar_dt )/250,0) AS C
                                                                                    FROM customer_case3
                                                                                    ORDER BY cust_id,calendar_dt
                                                                              )
                                                            )
                                            )
                                    WHERE SELECT_ONE_ROW=1
                        )
        )
 WHERE SELECT_ONE_ROW2=1