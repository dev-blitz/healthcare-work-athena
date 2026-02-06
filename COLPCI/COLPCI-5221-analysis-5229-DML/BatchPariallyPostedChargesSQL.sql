SELECT
    transaction.id,
    transaction.parentchargeid,
    transaction.transfertype,
    transaction.fromdate,
    transaction.todate,
    transaction.days,
    transaction.procedurecode,
    transaction.amount,
    transaction.outstanding,
    transaction.adjustments,
    transaction.transfers,
    transaction.payments,
    transaction.type,
    transaction.transactionreasonid,
    erarecord.transfertype AS erarecordtransfertype,
    erarecord.id erarecordid,
    erarecord.imagefilepagenumber,
    NVL(erarecord.PAYMENT,0) AS PAYMENT,
    NVL(erarecord.<record_adjustment_col1>,0), -- replace placeholders
    NVL(erarecord.<record_transfer_col1>,0), -- replace placeholders
    erarecord.failurereason,
    erarecord.manualflag,
    erarecord.matched,
    erarecord.action,
    erarecord.kickreasoncategoryid,
    client.lastname AS patient_lastname,
    client.firstname AS patient_firstname,
    kickreason.name AS kickreason,
    procedurecode.description,
    kickreasoncategory.name AS kickreasoncategory_name,
    patientinsurance.id AS insurancepackageid,
    patientinsurancepackage_name(patientinsurance.id) AS insurancepackage_name,
    erakick.kickcode,
    erakick.balancetransactionreason,
    erakick.kickedamount,
    erakick.kickreasonid,
    NULL AS partialposttype,
    1 AS partialposttypeorder
FROM
    erarecord
    JOIN transaction ON erarecord.chargeid = transaction.parentchargeid
    LEFT JOIN client ON transaction.patientid = client.id
    LEFT JOIN procedurecode ON transaction.procedurecode = procedurecode.procedurecode
    LEFT JOIN kickreasoncategory ON erarecord.kickreasoncategoryid = kickreasoncategory.id
    LEFT JOIN kickreason ON erakick.kickreasonid = kickreason.id
    LEFT JOIN claim ON transaction.claimid = claim.id
    LEFT JOIN patientinsurance ON transaction.patientinsuranceid = patientinsurance.id
    LEFT JOIN erakick ON erarecord.id = erakick.erarecordid
WHERE
    erarecord.erabatchid = :ERABATCHID
    AND erarecord.applied IS NOT NULL
    AND erarecord.partialpostsignedoff IS NULL
    AND transaction.voided IS NULL
    AND transaction.outstanding > 0
    AND transaction.type IN ('CHARGE', 'TRANSFERIN')
    AND NVL(erarecord.action, 'NONE') <> 'CLAIMLEVELREMIT'
    AND NOT EXISTS (
        SELECT 1
        FROM claimnote cn
        JOIN unpostable u ON cn.unpostableid = u.id
        JOIN patientinsurance pi ON cn.patientinsuranceid = pi.id
        JOIN insurancepackage ip ON pi.insurancepackageid = ip.id
        WHERE
            cn.claimid = transaction.claimid
            AND cn.transfertype = transaction.transfertype
            AND u.unpostabletypeid IN ('READJNOTICE','BALANCEFORWARD')
            AND ip.adjudicationprogramid = 'C'
    )
    AND (
        erarecord.transfertype <> '2'
        OR NOT EXISTS (
            SELECT 1
            FROM patientinsurance c2pi
            JOIN patientinsurance erpi ON erarecord.patientinsuranceid = erpi.id
            JOIN claimaudit ca ON ca.claimid = claim.id
            WHERE
                claim.secondarypatientinsuranceid = c2pi.id
                AND c2pi.insurancepackageid <> erpi.insurancepackageid
                AND ca.fieldname = 'SECONDARYPATIENTINSURANCEID'
                AND ca.oldvalue = erarecord.patientinsuranceid
                AND ca.newvalue = claim.secondarypatientinsuranceid
                AND ca.created > erarecord.applied
        )
    ) order by erabatch.id;

