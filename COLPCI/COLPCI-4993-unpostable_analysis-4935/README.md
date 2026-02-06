# ANALYSIS

* *analysis confluence-link*: https://athenaconfluence.athenahealth.com/spaces/CPDE/pages/794289196/Stuck+Payment+Batch+Payment+Batch+stuck+in+PREPOSTING
* `payment-batch-id` provided in JIRA- 2882A31775
* `context-id`- 31775
* `era-batch-id`- 363

## `ERA-BATCH` data in DB

    athena31775@dtest59 > select * from erabatch where id = 363;
    Truncated from 73 to 11 columns.
    +---+------------+---------+------------+-------------+----------------+-----------+--------------+--------------+--------------+----------------+
    |ID |RPOERAFILEID|STATUS   |TOTALCHARGES|TOTALPAYMENTS|TOTALADJUSTMENTS|TOTALCOPAYS|TOTALTRANSFERS|RECEIVERNUMBER|PROVIDERNUMBER|PAYORBATCHNUMBER|
    +---+------------+---------+------------+-------------+----------------+-----------+--------------+--------------+--------------+----------------+
    |363|293434716   |PREPOSTED|2479        |0            |                |           |              |OFFICEALLY    |1275586877    |                |
    +---+------------+---------+------------+-------------+----------------+-----------+--------------+--------------+--------------+----------------+
    Truncated from 73 to 11 columns.
    1 row selected in 0 seconds.

## `PAYMENT-BATCH` data in DB

    athena31775@dtest59 > select * from paymentbatch where id = 2882;
    Truncated from 55 to 13 columns.
    +----+-------------------+----------+---------+----------+----------+------+-----+-------------------+--------------+-----------+---------------+--------------+
    |ID  |CREATED            |CREATEDBY |TOTALCASH|TOTALCHECK|TOTALOTHER|CLOSED|NOTES|LASTMODIFIED       |LASTMODIFIEDBY|CHECKNUMBER|CHECKIDENTIFIER|PROVIDERNUMBER|
    +----+-------------------+----------+---------+----------+----------+------+-----+-------------------+--------------+-----------+---------------+--------------+
    |2882|2025-07-16 21:02:21|UNASSIGNED|         |          |          |      |     |2025-07-16 22:03:39|ATHENA        |3267745    |               |              |
    +----+-------------------+----------+---------+----------+----------+------+-----+-------------------+--------------+-----------+---------------+--------------+
    Truncated from 55 to 13 columns.
    1 row selected in 0 seconds.

## `UNPOSTABLE` data in DB

### all `unpostable` records without filter

    athena31775@dtest59 > select * from unpostable where erarecordid = 363;
    Truncated from 63 to 12 columns.
    +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
    |ID  |PAYMENTBATCHID|ERARECORDID|AMOUNT|TAXRATE|UNPOSTABLETYPEID|UNPOSTABLESTATUSID|ASSIGNMENTGROUPCLASSID|UNPOSTABLEROUTEID|PENDALARMDATE|NOTE|VOIDPAYMENTBATCHID|
    +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
    |1153|              |363        |0     |       |ROUTETOOTHER    |INPROCESS         |                      |0                |             |    |930               |
    |1097|930           |363        |0     |       |ROUTETOOTHER    |CLOSED            |                      |0                |             |    |                  |
    |1098|930           |363        |0     |       |ROUTETOOTHER    |INPROCESS         |                      |0                |             |    |                  |
    +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
    Truncated from 63 to 12 columns.
    3 rows selected in 0 seconds.

### all `unpostable` with filter

1. `VOIDPAYMENTBATCHID` *IS NULL*

        athena31775@dtest59 > select * from unpostable where erarecordid = 363 AND VOIDPAYMENTBATCHID IS NULL;
        Truncated from 63 to 12 columns.
        +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
        |ID  |PAYMENTBATCHID|ERARECORDID|AMOUNT|TAXRATE|UNPOSTABLETYPEID|UNPOSTABLESTATUSID|ASSIGNMENTGROUPCLASSID|UNPOSTABLEROUTEID|PENDALARMDATE|NOTE|VOIDPAYMENTBATCHID|
        +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
        |1097|930           |363        |0     |       |ROUTETOOTHER    |CLOSED            |                      |0                |             |    |                  |
        |1098|930           |363        |0     |       |ROUTETOOTHER    |INPROCESS         |                      |0                |             |    |                  |
        +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
        Truncated from 63 to 12 columns.
        2 rows selected in 0 seconds.


2. `VOIDPAYMENTBATCHID` *IS NULL* && `VOIDEDBY` *IS NULL*


        athena31775@dtest59 > select * from unpostable where erarecordid = 363 AND VOIDEDBY IS NULL AND VOIDPAYMENTBATCHID IS NULL;
        Truncated from 63 to 12 columns.
        +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
        |ID  |PAYMENTBATCHID|ERARECORDID|AMOUNT|TAXRATE|UNPOSTABLETYPEID|UNPOSTABLESTATUSID|ASSIGNMENTGROUPCLASSID|UNPOSTABLEROUTEID|PENDALARMDATE|NOTE|VOIDPAYMENTBATCHID|
        +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
        |1097|930           |363        |0     |       |ROUTETOOTHER    |CLOSED            |                      |0                |             |    |                  |
        |1098|930           |363        |0     |       |ROUTETOOTHER    |INPROCESS         |                      |0                |             |    |                  |
        +----+--------------+-----------+------+-------+----------------+------------------+----------------------+-----------------+-------------+----+------------------+
        Truncated from 63 to 12 columns.
        2 rows selected in 0 seconds.

* **\t;\0;** *payment-batch* 

        athena31775@dtest59 > \t;\0;select * from paymentbatch where id = 2882;
        \0: 0 columns and 38 rows removed.
        +-------------------+-------------------+
        |ID                 |2882               |
        +-------------------+-------------------+
        |CREATED            |2025-07-16 21:02:21|
        |CREATEDBY          |UNASSIGNED         |
        |LASTMODIFIED       |2025-07-16 22:03:39|
        |LASTMODIFIEDBY     |ATHENA             |
        |CHECKNUMBER        |3267745            |
        |TOTALSUMAMOUNT     |      0            |
        |TOTALUNPOSTEDAMOUNT|   -110.72         |
        |DEPOSITDATE        |2025-07-17 00:00:00|
        |DEPOSITBATCHID     |    952            |
        |ERABATCHID         |    363            |
        |PREPAREDBY         |ATHENA             |
        |PAYMENTBATCHROUTEID|     88            |
        |ASSOCIATED         |2025-07-16 22:03:39|
        |ASSOCIATEDBY       |ATHENA             |
        |PAPERLESSFLAG      |Y                  |
        |STATUSID           |     10            |
        +-------------------+-------------------+
        1 row selected in 0 seconds.

