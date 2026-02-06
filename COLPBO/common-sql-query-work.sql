SELECT

    batch.id || 'R' || batch._context_id_,

    batch.id || 'R' || batch._context_id_ || 'V' || record.claimid,

    batch._context_id_,

    batch.paymentbatchrouteid,

    -- DISCARDED IF NULL THEN VALID ELSE NOT

    record.discarded,

    record.applied,

    record.appliedby,

    -- partialpostsignedoff IF NULL THEN VALID ELSE NOT

    record.partialpostsignedoff,

    --TRANSFERTYPE IF 1 THEN PRIMARY ELSE NOT

    record.transfertype,

    -- FAILUREREASON IF NULL THEN VALID ELSE NOT

    record.failurereason,

    batch.postedby,

    -- DENIAL CHECK (IF 1 THEN 'DENIAL' ELSE NOT)

    CASE

        WHEN EXISTS( 

            SELECT 1 

            FROM ALLCONTEXTS.erakick erakick

            JOIN ALLCONTEXTS.kickreason kickreason

                ON kickreason.id = erakick.kickreasonid

                AND record._context_id_ = erakick._context_id_

            JOIN ATHENANET_RAW_COMBINED.ALLCONTEXTS.kickreason athenakickreason

                ON kickreason.athenakickcode = athenakickreason.kickcode 

            WHERE record.id = erakick.erarecordid

                AND athenakickreason.nextclaimstatus IN ('HOLD', 'MGRHOLD', 'ATHENAHOLD', 'CBOHOLD')

                AND athenakickreason.kickreasoncategoryid = 0

        ) THEN 1

        ELSE 0

     END

    AS nextclaimstatus,

    -- TOTAL RECORD COUNT to sort it

    (

        SELECT COUNT(erarecord.id)

        FROM ATHENANET_RAW_COMBINED.ALLCONTEXTS.erarecord erarecord

        WHERE erarecord._context_id_ = batch._context_id_

            AND erarecord.erabatchid = batch.id

    ) AS totalrecordcount,

    -- MULTIPLE CLAIM IN CLAIMSET (IF THIS IS 1 THEN NO DISTINCT PARENTID ELSE IT HAS MULTIPLE CLAIMS WITH DIFF PARENTERARECORDID)

    (

        SELECT COUNT(DISTINCT erarecord1.parenterarecordid)

        FROM ALLCONTEXTS.erarecord erarecord1

        WHERE erarecord1.claimid = record.claimid

            AND erarecord1.erabatchid = record.erabatchid

            AND erarecord1._context_id_ = record._context_id_

    ) AS distinctparentidcount,

    -- erabatchcopymap

    CASE 

        WHEN EXISTS(

            SELECT 1

            FROM erabatchcopymap

            WHERE batch.id = erabatchcopymap.originalerabatchid

                AND batch._context_id_ = erabatchcopymap._context_id_

        ) THEN 1

        ELSE 0

    END AS RARBatch,

    -- CHARGE COUNT MISMATCH (IF 0 THEN FIRST BATCH ELSE NOT)

    (

        SELECT COUNT(erabatch.id)

        FROM erabatch erabatch

        JOIN erarecord erarecord2

            ON erabatch.id = erarecord2.erabatchid

            AND erabatch._context_id_ = erarecord2._context_id_

        WHERE erabatch.id < batch.id

            AND erabatch._context_id_ = batch._context_id_

            AND erarecord2.claimid = record.claimid

    ) AS earlierbatchcount,

    --IF 'FQHC CONTEXT' THEN SKIP ELSE VALID 

    CASE 

        WHEN (batch._context_id_ IN (23840, 17792, 25248, 18112, 29472, 6401, 11553, 16450, 14530, 18626,

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

            19800, 12888, 20248, 12120, 18424, 12313, 30137, 30169, 13305, 9434,

            8506, 9754, 20090, 20250, 11194, 14266, 18426, 27771, 10395, 12891,

            18107, 21275, 26491, 12412, 15548, 16636, 21756, 9596, 21884, 23004,

            10844, 13116, 12349, 14429, 29821, 15549, 21725, 17661, 13661, 10781,

            7869, 10077, 12125, 28541, 29597, 14333, 26782, 17886, 20094, 25278,

            28542, 8094, 17406, 28735, 3167, 6719, 11903, 1823, 3871, 12063)) 

        THEN 'FQHC_CONTEXT' 

        ELSE '' 

    END AS FQHC_CONTEXT

FROM

    ALLCONTEXTS.erabatch batch

    JOIN ALLCONTEXTS.erarecord record 

        ON batch.id = record.erabatchid

        AND batch._context_id_ = record._context_id_

WHERE

    batch.created > '2022-01-01'

    AND batch.status <> 'DISCARDED'

    AND batch.ansiversionid = 3

    -- AND batch.paymentbatchrouteid IN (37)

    -- FAKEKICK

    AND EXISTS (

        SELECT 1

        FROM ALLCONTEXTS.erakick fakekick    

        WHERE fakekick.erarecordid = record.id

            AND fakekick._context_id_ = batch._context_id_

            AND fakekick.kickcode = 'PROVSIGINFORM'

    )
