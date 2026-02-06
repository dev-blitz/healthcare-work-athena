# pre-process-script

## steps

### 1. clear the claim

```bash
perl bin/clear_claim.pl --claimid=<claim-id>V<context-id> --stack PTEST1
```

```bash
dabhinab@preprod512511:~/dabhinab_streams$ perl bin/clear_claim.pl --claimid=238140v6901 --stack PTEST1
claimid: 238140v6901 at bin/clear_claim.pl line 42.
stack :PTEST1 at bin/clear_claim.pl line 43.

--before--->PBS--->$VAR1 = {
    ERABATCHID => '94683',
    PAYMENTBATCHID => '134761',
    POSTED => '04/26/2025',
    TRANSFERTYPE => '2'
};

----->PBS--->$VAR1 = {
    ERABATCHID => '94683',
    PAYMENTBATCHID => '134761',
    POSTED => '04/26/2025',
    TRANSFERTYPE => '2'
};
```

### 2. *voiding* the transactions

```bash
perl bin/postera.pl --masterroot PTEST1 --erabatchid <era-batch-id> --claimid <claim-id> --contextid <context-id> --voidonly
```

```bash
dabhinab@preprod512511:~/dabhinab_streams$ perl bin/postera.pl --masterroot PTEST1 --erabatchid 94683 --claimid 238140 --contextid 6901 --voidonly
voiding transactions upto paymentbatchid 134761
```

### 3. *update the batches* in the `DB`

```sql
dabhinab@preprod512511:~/dabhinab_streams$ sql++
sql++ version 18p1.  \h for help, for more search 'sql++' in the wiki.  \q or ^D to exit.
athena1@ptest1 > \u 6901

athena6901@ptest18 > 
update erabatch set status = 'CREATED' ,POSTED ='',POSTEDBY = '' where id = 94683;
1 row updated in 0 seconds.

athena6901@ptest18 > 
update erarecord set matched = '' ,matchedby = '',applied= '' ,appliedby = '',CHARGEID = '' ,TRANSFERTYPE= '',PATIENTINSURANCEIDNUMBER = '' ,SUBSCRIBERINSURANCEIDNUMBER = '' ,CORRECTEDINSURANCEIDNUMBER = '',claimid = '' where erabatchid = 94683 and claimid = 238140;
1 row updated in 0 seconds.

athena6901@ptest18 > 
update paymentbatch set statusid = 1 ,closed = '' ,ASSOCIATED = '' ,ASSOCIATEDBY = '' ,erabatchid = '' where erabatchid = 94683;
1 row updated in 0 seconds.

athena6901@ptest18 > commit;
Commit
```

* *queries*:

```sql
update erabatch set status = 'CREATED' ,POSTED ='',POSTEDBY = '' where id = <era-batch-id>;
    
update erarecord set matched = '' ,matchedby = '',applied= '' ,appliedby = '',CHARGEID = '' ,TRANSFERTYPE= '',PATIENTINSURANCEIDNUMBER = '' ,SUBSCRIBERINSURANCEIDNUMBER = '' ,CORRECTEDINSURANCEIDNUMBER     = '',claimid = '' where erabatchid = <era-batch-id> and claimid = <claim-id>;
    
update paymentbatch set statusid = 1 ,closed = '' ,ASSOCIATED = '' ,ASSOCIATEDBY = '' ,erabatchid = '' where erabatchid = <era-batch-id>;
```

### 4. *post the batches* in order

```bash
perl prod/scripts/app/practice/preprocess_erabatches.pl PTEST18 ATHENA6901 --erabatchids <era-batch-id>;
```

```bash
dabhinab@preprod512511:~/dabhinab_streams$ perl prod/scripts/app/practice/preprocess_erabatches.pl PTEST18 ATHENA6901 --erabatchids 94683;
Starting prod/scripts/app/practice/preprocess_erabatches.pl; practice time is 2025/09/01 09:24:04
Starting garbage collection.
Starting prematching.
Match ERA Batch 94683R6901:
pre-matching... done.  Marking as pre-matched... done.
Starting preposting.
Post ERA Batch 94683R6901: Found paymentbatch with id 134761 for erabatch 94683R6901...
check for new apply queue...
matching again, for good measure... pre-posting... done.  Marking as pre-posted... done.
```
