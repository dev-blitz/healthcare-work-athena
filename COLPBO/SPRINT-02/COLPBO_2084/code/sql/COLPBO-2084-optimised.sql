WITH FilteredClaims AS (
    SELECT DISTINCT t.claimid
    FROM tcharge t
    JOIN claimnote c ON t.claimid = c.claimid
    WHERE c.kickreasonid = 20938
    AND t.type = 'CHARGE'
    GROUP BY t.claimid
    HAVING SUM(t.amount) < 5000
) SELECT 
    batch.id || 'R' || batch._context_id_ AS batch_key,
    batch.id || 'R' || batch._context_id_ || 'V' || record.claimid AS claim_key,
    batch._context_id_,
    batch.paymentbatchrouteid,
    record.discarded,
    record.applied,
    record.appliedby,
    record.partialpostsignedoff,
    record.transfertype,
    record.failurereason,
    batch.postedby,
    CASE WHEN EXISTS (
        SELECT 1 FROM FilteredClaims WHERE record.claimid = FilteredClaims.claimid
    ) THEN 1 ELSE 0 END AS nextclaimstatus,
    COUNT(record.id) OVER (PARTITION BY batch._context_id_, batch.id) AS totalrecordcount,
    COUNT(DISTINCT record.parenterarecordid) OVER (PARTITION BY record.claimid, record.erabatchid) AS distinctparentidcount,
    CASE WHEN EXISTS (
        SELECT 1 FROM erabatchcopymap 
        WHERE batch.id = erabatchcopymap.originalerabatchid 
        AND batch._context_id_ = erabatchcopymap._context_id_
    ) THEN 1 ELSE 0 END AS RARBatch,
    COUNT(batch.id) OVER (PARTITION BY record.claimid) AS earlierbatchcount,
    CASE
        WHEN batch._context_id_ IN (
            23840, 17792, 25248, 18112, 29472, 6401, 11553, 16450, 14530, 18626,
            13826, 17986, 16098, 19266, 17250, 16258, 11331, 27715, 7267, 7363,
            14627, 8515, 27043, 11747, 16899, 19043, 28291, 4803, 22211, 2819,
            16291, 25507, 12324, 15524, 7492, 10564, 16708, 10628, 19844, 14756,
            30244, 22116, 30308, 13956, 28516, 26532, 11397, 16517, 23845, 13669,
            18277, 17349, 29669, 15462, 26726, 25734, 15558, 25862, 8710, 27238,
            27270, 13126, 13158, 28518, 16647, 15655, 25927, 16775, 12871, 13095,
            17223, 12328, 17704, 12616, 17832, 9864, 20104, 7912, 23272, 14088,
            17288, 16425, 7241, 20553, 25737, 21673, 11497, 12521, 11721, 28169,
            19209, 18313, 7274, 16522, 11658, 19850, 27018, 11818, 13866, 21034,
            13547, 15691, 11915, 18091, 15051, 21516, 28780, 7340, 15532, 8492,
            25900, 14668, 11628, 11692, 14764, 29100, 14860, 8748, 21036, 15948,
            16972, 20108, 22252, 19212, 12525, 13581, 22829, 21901, 14861, 24622,
            14446, 16494, 11502, 8526, 27086, 13902, 28334, 17326, 17390, 15375,
            10575, 9679, 9743, 6703, 22063, 13071, 20239, 13199, 23600, 12400,
            4368, 28976, 16720, 432, 27056, 20976, 27280, 3792, 16465, 7377,
            23857, 7601, 14865, 10865, 15985, 13265, 28818, 24754, 23954, 12946,
            17106, 11347, 11411, 15507, 20691, 15635, 30195, 28179, 21075, 8883,
            27315, 28435, 22355, 10099, 17363, 14868, 18068, 11988, 15156, 27572,
            16437, 12373, 8309, 17621, 20853, 10709, 15989, 28309, 24341, 12149,
            10133, 14261, 14422, 20726, 29974, 12694, 12822, 15958, 24182, 14006,
            14134, 19414, 15479, 8439, 9559, 26199, 20119, 28343, 10039, 11352,
            19800, 12888, 20248, 12120, 18424, 12313, 30137, 30169, 13305, 9434
        ) THEN 'FQHC_CONTEXT'
        ELSE ''
    END AS FQHC_CONTEXT
FROM ALLCONTEXTS.erabatch batch
JOIN ALLCONTEXTS.erarecord record 
    ON batch.id = record.erabatchid 
    AND batch._context_id_ = record._context_id_
LEFT JOIN FilteredClaims c ON record.claimid = c.claimid
WHERE 
    batch.created >= '2022-01-01'
    AND batch.status <> ('DISCARDED')  -- Optimized filtering
    AND batch.ansiversionid = 3
    AND batch.created >= DATEADD(DAY, -7, CURRENT_DATE())
LIMIT 50500;

