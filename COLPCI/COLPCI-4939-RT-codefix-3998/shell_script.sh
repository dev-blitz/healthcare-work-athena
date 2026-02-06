#!/bin/bash

LOGFILE="output.log"
: > "$LOGFILE"   # Truncate log file if it exists

# 1. Run clear_claim.pl
echo "+ perl ~/dabhinab_streams/bin/clear_claim.pl --claimid=6438045V7654 --stack PTEST1" | tee -a "$LOGFILE"
perl ~/dabhinab_streams/bin/clear_claim.pl --claimid=6438045V7654 --stack PTEST1 2>&1 | tee -a "$LOGFILE"

# 2. postera.pl
echo "+ perl ~/dabhinab_streams/bin/postera.pl --masterroot PTEST1 --erabatchid 1174798 --claimid 6438045 --contextid 7654 --voidonly" | tee -a "$LOGFILE"
perl ~/dabhinab_streams/bin/postera.pl --masterroot PTEST1 --erabatchid 1174798 --claimid 6438045 --contextid 7654 --voidonly 2>&1 | tee -a "$LOGFILE"

# 3. sql++ session 1
echo "+ sql++" | tee -a "$LOGFILE"
sql++ 2>&1 <<'EOF' | tee -a "$LOGFILE"
\u 7654
UPDATE erabatch SET status = 'CREATED', POSTED ='', POSTEDBY = '' WHERE id IN (1174798);
UPDATE erarecord SET matched = '', matchedby = '', applied= '', appliedby = '', CHARGEID = '', TRANSFERTYPE= '', PATIENTINSURANCEIDNUMBER = '', SUBSCRIBERINSURANCEIDNUMBER = '', CORRECTEDINSURANCEIDNUMBER = '', claimid = '' WHERE erabatchid IN (1174798) AND claimid = 6438045;
UPDATE paymentbatch SET statusid = 1, closed = '', ASSOCIATED = '', ASSOCIATEDBY = '', erabatchid = '' WHERE erabatchid IN (1174798);
commit;
EOF

echo "+ (sql++ session ended)" | tee -a "$LOGFILE"

# 4. preprocess_erabatches.pl
echo "+ perl ~/dabhinab_streams/prod/scripts/app/practice/preprocess_erabatches.pl PTEST8 ATHENA7654 --erabatchids 1174798" | tee -a "$LOGFILE"
perl ~/dabhinab_streams/prod/scripts/app/practice/preprocess_erabatches.pl PTEST8 ATHENA7654 --erabatchids 1174798 2>&1 | tee -a "$LOGFILE"

# 5. sql++ session 2
echo "+ sql++" | tee -a "$LOGFILE"
sql++ 2>&1 <<'EOF' | tee -a "$LOGFILE"
select k.kickcode, k.kickedamount, kr.athenakickcode, r.id, r.erabatchid, kr.balancetransactiontype, kr.balancetransactionreason, kr.nextclaimstatus from erakick k, erarecord r, kickreason kr where k.kickreasonid = kr.id and k.erarecordid = r.id and r.controlnumber = '6438045V7654' and r.erabatchid = 1174798 order by r.created;
EOF

echo "+ (sql++ session ended)" | tee -a "$LOGFILE"

echo "=== All commands completed. Output in $LOGFILE ==="

