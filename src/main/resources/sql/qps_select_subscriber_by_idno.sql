SELECT COUNT(*)
FROM SUBSCRIBER sub
WHERE CUST_ID IN (SELECT DISTINCT ci.cust_id
                  FROM CUST_IDENTITY ci,
                       CUSTOMER cus
                  WHERE ci.CUST_ID = cus.CUST_ID
                    AND ci.ID_NO = ?
                    AND ci.STATUS = '1'
                    AND cus.STATUS = '1')
