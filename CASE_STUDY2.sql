SELECT cust_id, month_start_dt ,month_base_flag,
                CASE
                         WHEN c=1 AND  LAG(c,1) over(PARTITION BY cust_id ORDER BY cust_id)=1 THEN 'B'
                         
                         WHEN SUM(c) OVER(PARTITION BY cust_id ORDER BY cust_id ROWS BETWEEN UNBOUNDED  PRECEDING AND CURRENT ROW ) =1 AND
                         SUM(c) OVER(PARTITION BY cust_id ORDER BY cust_id ROWS BETWEEN CURRENT ROW AND CURRENT ROW  ) =1  THEN 'I'
                         
                          WHEN LAG(c,1) OVER(PARTITION BY cust_id ORDER BY cust_id)=0 AND 
                                    SUM(c) OVER(PARTITION BY cust_id ORDER BY cust_id ROWS BETWEEN UNBOUNDED  PRECEDING AND CURRENT ROW  ) >=2 AND 
                                    SUM(c) OVER(PARTITION BY cust_id ORDER BY cust_id ROWS BETWEEN CURRENT ROW AND CURRENT ROW ) =1 THEN 'R'
                                    
                          WHEN SUM(c) OVER(PARTITION BY cust_id ORDER BY cust_id ROWS BETWEEN 1  PRECEDING AND CURRENT ROW  ) >=1 AND 
                                    SUM(c) OVER(PARTITION BY cust_id ORDER BY cust_id ROWS BETWEEN CURRENT ROW AND CURRENT ROW  ) =0 THEN 'O'
                END AS IBRO_SEGMENT
FROM (
                 SELECT
                        cust_id, month_start_dt ,month_base_flag,
                        COUNT(month_base_flag)
                         OVER (PARTITION BY cust_id,month_start_dt ORDER BY cust_id,month_start_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED  FOLLOWING ) as c
                 FROM customer_case2 
                 ORDER BY cust_id,month_start_dt
               
          )

