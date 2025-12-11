INSERT INTO CUST_IDENTITY (CUST_IDENTITY_ID,
                           CUST_ID,
                           ID_TYPE,
                           ID_NO,
                           ID_ISSUE_PLACE,
                           ID_ISSUE_DATE,
                           ID_EXPIRE_DATE,
                           CREATE_USER,
                           CREATE_DATETIME,
                           UPDATE_USER,
                           UPDATE_DATETIME,
                           STATUS)
VALUES (?, -- CUST_IDENTITY_ID
        ?, -- CUST_ID
        'IDC',
        LPAD(TRUNC(DBMS_RANDOM.VALUE(0, 999999999999)), 12, '0'),
        'Hà Nội',
        TO_DATE('2013-10-23', 'YYYY-MM-DD HH24:MI:SS'),
        NULL,
        'VTT1',
        TO_DATE('2025-06-12 14:02:28', 'YYYY-MM-DD HH24:MI:SS'),
        'VTT1',
        TO_DATE('2025-12-11 09:22:51', 'YYYY-MM-DD HH24:MI:SS'),
        '1')
