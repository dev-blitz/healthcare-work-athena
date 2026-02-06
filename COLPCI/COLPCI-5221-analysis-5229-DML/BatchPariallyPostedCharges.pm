#########################################################################################
# BatchPartiallyPostedChargesSQL
#
# Description:
#       -returns the SQL::Select object used to find the partially posted charges of particular era
#        batch.
#
# Parameters:
#       $dbh
#       $args - hashref of args
#               ERABATCHID => (required) id of era batch
#
# Return Value:
#       SQL::Select object
#########################################################################################
sub BatchPartiallyPostedChargesSQL {
        my ($dbh,$args) = @_;
        AssertRequiredFields($args,['ERABATCHID']);

        my $paymentbatchid = SQLFirstRow("select id from paymentbatch where erabatchid = ?", $dbh, $args->{ERABATCHID});
        my $mcrreadjoverpaidkickreasonid = SQLValues("select id from kickreason where kickcode = 'MCRREADJOVERPAID' and kickreasoncategoryid = 1705", $dbh);
        my $toggle = BusCall::PracticeStructure::GetTablespaceValueWithDefault($dbh, { CONTEXTID => 1, KEY => 'CRZS_11428_QUERY_SPLIT' }) eq 'ON';
        # We are going to name the CHARGE/TRANSFERIN transaction for the non-zero-outstanding charge
        # "transaction" and the one for the current batch's erarecords "incomingcharge."  Those
        # will only be different in the case where erarecords for one transfertype cause the other
        # transfertype to be mispaid (e.g. secondary posts before primary, secondary reopens
        # primary).  Only in that case will we join on incomingcharge.  All we want from
        # incomingcharge is patientinsurance info.

        # We start with a base query and make copies of it, then alter the copies, then union all
        # the resulting queries.  The first alteration will be to handle both erarecords with their
        # kick data included separate from erarecords with their kick data in erakick rows.

        # The second changes will be to cover the different types of mispayment scenarios we expect
        # to see: simple underpaid-or-overpaid primary-or-secondary (the erarecords have one
        # transfertype and the charges that are mispaid are of the same transfertype),
        # secondary-reopens-primary (secondary erarecords, primary overpaid),
        # secondary-posted-before-primary (primary erarecords, secondary underpaid), and
        # secondary-denial-before-primary (primary erarecords, secondary underpaid and had an old
        # kick).

        # So we end up with 8 queries, and we take their union.

        # base query
        my %sql;
        $sql{ROOTBASE} = SQL::Select->new()->Select(
                "transaction.id",
                "transaction.parentchargeid",
                "transaction.transfertype",
                "transaction.fromdate",
                "transaction.todate",
                "transaction.days",
                "transaction.procedurecode",
                "transaction.amount",
                "transaction.outstanding",
                # these are used in regressions:
                "transaction.claimid",
                "transaction.adjustments",
                "transaction.transfers",
                "transaction.payments",
                "transaction.type",
                "transaction.transactionreasonid",
                # erarecord.transfertype is usually the same as transaction.transfertype, except for
                # special cases below.
                "erarecord.transfertype ERARECORDTRANSFERTYPE",
                "erarecord.id erarecordid",
                "erarecord.imagefilepagenumber",
                (map {"nvl(erarecord.$_,0) $_"} ('PAYMENT',@ERA::RecordAdjustmentCols,@ERA::RecordTransferCols)),
                "erarecord.failurereason failurereason",
                "erarecord.manualflag manualflag",
                "erarecord.matched matched",
                "erarecord.action action",
                "erarecord.kickreasoncategoryid kickreasoncategoryid",
                "client.lastname patient_lastname",
                "client.firstname patient_firstname",
                "kickreason.name kickreason",
                "procedurecode.description description",
                "kickreasoncategory.name kickreasoncategory_name",
                "patientinsurance.id as insurancepackageid",
                "patientinsurancepackage_name(patientinsurance.id) insurancepackage_name",
                "erakick.kickcode kickcode",
                "erakick.balancetransactionreason balancetransactionreason",
                "erakick.kickedamount kickedamount",
                "erakick.kickreasonid kickreasonid",
        );
        # Note: one of the below queries contains this From clause in its entirety
        # with one additional table and a slightly-different ordering to accomodate
        # an "ordered" hint.  If you change the tables here, change them there as
        # well, and verify that you didn't kill the performance of the query.
        $sql{ROOTBASE}->From(
                'erarecord',
                'transaction',
                'client',
                'procedurecode',
                'kickreasoncategory',
                'kickreason',
                'claim',
                'patientinsurance',
                'erakick',
        )->Where(
                ["erarecord.erabatchid = ?", $args->{ERABATCHID}],
                "erarecord.applied is not null",
                "erarecord.partialpostsignedoff is null",
                "transaction.voided is null",
        )->Joins(
                "transaction.claimid = claim.id",
                "transaction.patientid = client.id (+)",
                "transaction.procedurecode = procedurecode.procedurecode (+)",
                "erarecord.kickreasoncategoryid = kickreasoncategory.id (+)",
                "erarecord.id = erakick.erarecordid (+)",
                "erakick.kickreasonid = kickreason.id (+)",
        );
        if ($args->{CLAIMID}) {
                $sql{ROOTBASE}->Where(["erarecord.claimid = ?", $args->{CLAIMID}]);
        }

        $sql{BASE} = DeepCopyStructure($sql{ROOTBASE});
        $sql{BASE}->Joins(
                "erarecord.chargeid = transaction.parentchargeid",
        );

        # Final subqueries to be unioned together - they adds conditions for each
        # of the four major ways an erarecord can be partially posted:
        #  - The transfertype of our erarecord is overpaid or underpaid.
        #  - Secondary posted before primary: this erarecord is for primary but the secondary posted
        #    first and we had undertransferred so now secondary is underpaid.
        #  - Secondary reopens primary: this erarecord is for secondary but it triggered a transfer
        #    from primary - which is now overpaid.
        #  - Extra patient transfer: we wind up with more money in patient transfers for a given charge
        #    for the payer who is remitting than was specified in their remittance

        # simple overpaid or underpaid

        $sql{UNDERPAID} = DeepCopyStructure($sql{BASE});
        $sql{UNDERPAID}->Hints("ordered use_nl(erarecord transaction) index(transaction transaction_parentchargeid)");
        $sql{UNDERPAID}->Select(
                "null partialposttype",
                "1 partialposttypeorder",
        )->Joins(
                # Incoming erarecord from primary mispays primary, or from secondary mispays
                # secondary (the mispaid charge is the incoming charge):
                "erarecord.transfertype = transaction.transfertype",
                "transaction.patientinsuranceid = patientinsurance.id",
        )->Where(
                "transaction.outstanding > 0",
                "transaction.type in ('CHARGE','TRANSFERIN')",
                "nvl(erarecord.action,'NONE') <> 'CLAIMLEVELREMIT'",
                # Hydra 92715: If the claim is underpaid in the same transfer type as Medicare
                # balance forward unpostable, do not display on partial post lists.
                "not exists (
                        select 1 from claimnote, unpostable, insurancepackage
                        where
                                claimnote.claimid = transaction.claimid
                                and claimnote.transfertype = transaction.transfertype
                                and claimnote.unpostableid = unpostable.id
                                and unpostable.unpostabletypeid in ('READJNOTICE', 'BALANCEFORWARD')
                                and claimnote.patientinsuranceid = patientinsurance.id
                                and patientinsurance.insurancepackageid = insurancepackage.id
                                and insurancepackage.adjudicationprogramid = 'C')",
                # If this is secondary and we have just automatically flipped 2 to a tertiary, we
                # expect secondary will still be open.  No need to send this to partially-posted.
                q{
                        erarecord.transfertype <> '2' or not exists (
                                select 1 from
                                        patientinsurance claim2patientinsurance,
                                        patientinsurance erarecordpatientinsurance,
                                        claimaudit
                                where
                                        claim.secondarypatientinsuranceid = claim2patientinsurance.id
                                        and erarecord.patientinsuranceid = erarecordpatientinsurance.id
                                        and claim2patientinsurance.insurancepackageid <> erarecordpatientinsurance.insurancepackageid
                                        and claimaudit.claimid = claim.id
                                        and claimaudit.fieldname = 'SECONDARYPATIENTINSURANCEID'
                                        and claimaudit.oldvalue = erarecord.patientinsuranceid
                                        and claimaudit.newvalue = claim.secondarypatientinsuranceid
                                        and claimaudit.created > erarecord.applied
                        )
                },
        );

        $sql{OVERPAID} = DeepCopyStructure($sql{BASE});
        $sql{OVERPAID}->Select(
                "null partialposttype",
                "2 partialposttypeorder",
        )->Joins(
                # Incoming erarecord from primary mispays primary, or from secondary mispays
                # secondary (the mispaid charge is the incoming charge):
                "erarecord.transfertype = transaction.transfertype",
                "transaction.patientinsuranceid = patientinsurance.id",
        )->Where(
                "transaction.outstanding < 0",
                "transaction.type in ('CHARGE','TRANSFERIN')",
                "nvl(erarecord.action,'NONE') <> 'CLAIMLEVELREMIT'",
        );

        # secondary reopens primary #

        # We need to use the "ordered" hint here because otherwise it takes half a
        # second for oracle to determine the optimal table join order.  If you need
        # to add a new table to this query, be very careful that you don't kill the
        # performance of this query.
        $sql{SECONDARYREOPENSPRIMARY} = DeepCopyStructure($sql{BASE});
        $sql{SECONDARYREOPENSPRIMARY}->Hints(
                "ordered",
        )->Select(
                "'secondary reopened primary' partialposttype",
                "3 partialposttypeorder",
        # We use ReplaceFrom so we can fully-specify the table join ordering here.
        )->ReplaceFrom(
                'transaction',
                'erarecord',
                'procedurecode',
                'kickreasoncategory',
                'client',
                'kickreason',
                'claim',
                'erakick',
                # We join on transaction incomingcharge only to get patientinsurance.id.
                'transaction incomingcharge',
                'patientinsurance',
        )->Joins(
                "incomingcharge.parentchargeid = erarecord.chargeid",
                ["incomingcharge.paymentbatchid = ?", $paymentbatchid],
                "incomingcharge.patientinsuranceid = patientinsurance.id",
        )->Where(
                # Incoming erarecord from secondary mispays primary:
                "erarecord.transfertype = '2'",
                "incomingcharge.transfertype = '2'",
                "incomingcharge.type = 'TRANSFERIN'",
                "transaction.transfertype = '1'",
                "transaction.type = 'CHARGE'",
                "transaction.outstanding < 0",
                # Transferins created in the course of autoposting do have a paymentbatchid.
                ["transaction.outstanding = (
                        select -sum(amount) from transaction t
                        where
                                t.type = 'TRANSFERIN'
                                and t.voided is null
                                and t.parentchargeid = transaction.id
                                and t.transfertype = '2'
                                and t.paymentbatchid = ?
                )", $paymentbatchid],
        );

        # secondary posted before primary #

        $sql{SECONDARYBEFOREPRIMARY} = DeepCopyStructure($sql{BASE});
        $sql{SECONDARYBEFOREPRIMARY}->Select(
                "'secondary before primary' partialposttype",
                "4 partialposttypeorder",
        )->From(
                # We join on transaction incomingcharge only to get patientinsurance.id.
                "transaction incomingcharge",
        )->Joins(
                "incomingcharge.parentchargeid = erarecord.chargeid",
                "incomingcharge.patientinsuranceid = patientinsurance.id",
        )->Where(
                # Incoming erarecord from primary mispays secondary:
                "erarecord.transfertype = '1'",
                "incomingcharge.transfertype = '1'",
                "incomingcharge.type = 'CHARGE'",
                "transaction.transfertype = '2'",
                "transaction.type = 'TRANSFERIN'",
                # The original TRANSFERIN to 2 is voided, but our transaction is the current
                # nonvoided one, because we need the current tcharge.outstanding.
                "transaction.outstanding <> 0",
                # Can't check for t.transfertype = '2' because PAYMENTS and ADJUSTMENTS don't have
                # transfertype set.
                # Can't check for t.chargeid = transaction.id because t.chargeid is the voided
                # TRANSFERIN.
                # So we check that its charge's transfertype is 2.
                # t.paymentbatchid may be null if t isn't a PAYMENT.
                # t.voidpaymentbatchid is not null if it was voidedby ATHENA in the course of
                # autoposting.
                ["exists (
                        select /*+ no_index(t transaction_voidpaymentbatchid) */ 1 from transaction t
                        where
                                t.parentchargeid = incomingcharge.id
                                and (
                                        t.transfertype = '2'
                                        or ( select transfertype from transaction where id = t.chargeid ) = '2'
                                )
                                and t.type in ('PAYMENT','ADJUSTMENT','TRANSFERIN')
                                and t.voided is not null
                                and t.voidpaymentbatchid = ?
                                and ( t.paymentbatchid <> t.voidpaymentbatchid or t.paymentbatchid is null )
                )", $paymentbatchid],
                # don't add it to the partial post list:
                # if this was a primary readjudication that resulted in an outstanding secondary balance
                # if this was a primary readjudication by Medicare that left secondary overpaid (kick 91949 MCRREADJOVERPAID)
                ["transaction.outstanding < 0 and not exists (
                        select
                                1
                        from
                                claimnote,
                                paymentbatch
                        where
                                claimnote.paymentbatchid = paymentbatch.id
                                and claimnote.claimid = claim.id
                                and claimnote.transfertype = '2'
                                and claimnote.kickreasonid = $mcrreadjoverpaidkickreasonid
                                and paymentbatch.erabatchid = ?
                )
                or transaction.outstanding > 0 and not exists (
                        select
                                /*+ no_index(readjudication erarecord_batchid) no_index(reversal erarecord_batchid) */
                                1
                        from
                                erarecord readjudication,
                                erarecord reversal
                        where
                                readjudication.chargeid = transaction.parentchargeid
                                and reversal.chargeid = transaction.parentchargeid
                                and readjudication.reversalflag is null
                                and reversal.transfertype = '1'
                                and readjudication.transfertype = '1'
                                and reversal.reversalflag = 'Y'
                                and readjudication.erabatchid = reversal.erabatchid
                                and readjudication.erabatchid = ?
                )", ($args->{ERABATCHID}) x 2],

        );

        # secondary denial before primary #

        $sql{SECONDARYDENIALBEFOREPRIMARY} = DeepCopyStructure($sql{BASE});
        $sql{SECONDARYDENIALBEFOREPRIMARY}->Select(
                "'secondary denial before primary' partialposttype",
                "5 partialposttypeorder",
        )->From(
                # We join on transaction incomingcharge only to get patientinsurance.id.
                "transaction incomingcharge",
        )->Joins(
                "incomingcharge.parentchargeid = erarecord.chargeid",
                "incomingcharge.patientinsuranceid = patientinsurance.id",
        )->Where(
                # Incoming erarecord from primary mispays secondary:
                "erarecord.transfertype = '1'",
                # Incoming erarecord is for primary charge.
                "incomingcharge.transfertype = '1'",
                "incomingcharge.type = 'CHARGE'",
                # Secondary charge is mispaid, i.e. possibly already denied,
                # probably should not be rebilled, so the poster may wish to take
                # anything transferred just now to 2 and pass it on to p.
                "transaction.transfertype = '2'",
                "transaction.type = 'TRANSFERIN'",
                "transaction.outstanding > 0",
                # Condition: there is an old claimnote
                # for this claim,
                # with a kick,
                # on secondary,
                # that occurred in the context of a payment batch,
                # and whose insurance package is the same as the current claim.secondarypatientinsuranceid ins pkg;
                # and there were no transfers to secondary before the one just now in the current PB;
                # and the claim was in CLOSED status2 before and after the claimnote.
                ["exists (
                        select 1
                        from
                                claimnote,
                                claim,
                                patientinsurance claimnotepatientinsurance,
                                patientinsurance claimsecondarypatientinsurance
                        where
                                claimnote.claimid = erarecord.claimid
                                and claimnote.kickreasonid is not null
                                and claimnote.transfertype = '2'
                                and claimnote.paymentbatchid is not null
                                and claimnote.patientinsuranceid = claimnotepatientinsurance.id
                                and claim.id = claimnote.claimid
                                and claim.secondarypatientinsuranceid = claimsecondarypatientinsurance.id
                                and claimnotepatientinsurance.insurancepackageid = claimsecondarypatientinsurance.insurancepackageid
                                and not exists (
                                        select 1 from tcharge
                                        where claimid = erarecord.claimid
                                        and transfertype = '2'
                                        and paymentbatchid <> ?
                                )
                                and claimnote.claimstatus = 'CLOSED'
                                and (
                                        not exists (
                                                select /*+ index(cn claimnote_claimid) */ 1 from claimnote cn
                                                where cn.claimid = claimnote.claimid
                                                and cn.transfertype = '2'
                                                and cn.id < claimnote.id
                                        )
                                        or exists (
                                                select /*+ index(cn claimnote_claimid) */ 1 from claimnote cn
                                                where cn.claimid = claimnote.claimid
                                                and cn.transfertype = '2'
                                                and cn.id < claimnote.id
                                                and cn.claimstatus = 'CLOSED'
                                                and not exists (
                                                        select /*+ index(cn2 claimnote_claimid) */ 1 from claimnote cn2
                                                        where cn2.claimid = claimnote.claimid
                                                        and cn2.transfertype = '2'
                                                        and cn2.id < claimnote.id
                                                        and cn.id < cn2.id
                                                )
                                        )
                                )
                )", $paymentbatchid],
                # don't add it to the partial post list:
                # if this was a primary readjudication that resulted in an outstanding secondary balance
                # if this was a primary readjudication by Medicare that left secondary overpaid
                ["transaction.outstanding < 0 and not exists (
                        select
                                1
                        from
                                claimnote,
                                paymentbatch
                        where
                                claimnote.paymentbatchid = paymentbatch.id
                                and claimnote.claimid = claim.id
                                and claimnote.transfertype = '2'
                                and claimnote.kickreasonid = $mcrreadjoverpaidkickreasonid
                                and paymentbatch.erabatchid = ?
                )
                or transaction.outstanding > 0 and not exists (
                        select
                                /*+ no_index(readjudication erarecord_batchid) no_index(reversal erarecord_batchid) */
                                1
                        from
                                erarecord readjudication,
                                erarecord reversal
                        where
                                readjudication.chargeid = transaction.parentchargeid
                                and reversal.chargeid = transaction.parentchargeid
                                and readjudication.reversalflag is null
                                and reversal.reversalflag = 'Y'
                                and reversal.transfertype = '1'
                                and readjudication.transfertype = '1'
                                and readjudication.erabatchid = reversal.erabatchid
                                and readjudication.erabatchid = ?
                )", ($args->{ERABATCHID}) x 2],
        );

        ####################################
        # Claim Level Partial Post Reasons #
        ####################################
        $sql{CLAIMLEVELBASE} = DeepCopyStructure($sql{ROOTBASE});
        $sql{CLAIMLEVELBASE}->Joins(
                "erarecord.claimid = claim.id",
        )->Where(
                "erarecord.action = 'CLAIMLEVELREMIT'",
        );

        # CLAIM LEVEL DISCREPANCY #
        $sql{CLAIMLEVELREMIT} = DeepCopyStructure($sql{CLAIMLEVELBASE});
        $sql{CLAIMLEVELREMIT}->Select(
                # We can sort these into overpaid/underpaid primary/secondary in the display if we
                # want.
                "'Claim Level Discrepancy' partialposttype",
                "7 partialposttypeorder",
        )->Joins(
                # Incoming erarecord from primary mispays primary, or from secondary mispays
                # secondary (the mispaid charge is the incoming charge):
                "erarecord.transfertype = transaction.transfertype",
                "transaction.patientinsuranceid = patientinsurance.id",
        )->Where(
                "transaction.outstanding <> 0",
                "transaction.type in ('CHARGE','TRANSFERIN')",
                # Hydra 92715: If the claim is underpaid in the same transfer type as Medicare
                # balance forward unpostable, do not display on partial post lists.
                "transaction.outstanding < 0
                or not exists (
                        select 1 from claimnote, unpostable, insurancepackage
                        where
                                claimnote.claimid = transaction.claimid
                                and claimnote.transfertype = transaction.transfertype
                                and claimnote.unpostableid = unpostable.id
                                and unpostable.unpostabletypeid in ('READJNOTICE', 'BALANCEFORWARD')
                                and claimnote.patientinsuranceid = patientinsurance.id
                                and patientinsurance.insurancepackageid = insurancepackage.id
                                and insurancepackage.adjudicationprogramid = 'C')",
        );

        # delete unneeded entries
        delete $sql{ROOTBASE};
        delete $sql{BASE};
        delete $sql{CLAIMLEVELBASE};

        # For most partial post types, we require that there be some remittance in order for a
        # charge to be partially posted.  Extra patient transfers, however, can, and usually will,
        # appear in the cases of zero-dollar remittance.
        #
        # Multiple nonzero-$ erakicks for one erarecord will produce nonidentical "duplicate" rows:
        # one for each kick and one for the erarecord.  Also, erarecords that meet the nonzero-$
        # requirement without recourse to kicks will show up in both lists.  Since we group by
        # parentchargeid and count the money by distinct erarecordid and transactionid, and, um,
        # don't the kick data, we can get away with it, for now.
        #
        # Hydra #268560 - Records with clear denials showing on the partial post list
        # While checking for denials check the totalkickedamount of each balancetransactiontype.
        my @clauses;
        foreach my $type (sort keys %sql) {
                $sql{$type}->Where(
                        "(
                                " . join(' or ',map {"nvl(erarecord.$_,0) <> 0"} ('PAYMENT',@ERA::RecordAdjustmentCols,@ERA::RecordTransferCols)). "
                                or (
                                        erakick.balancetransactiontype is not null
                                        and nvl( (select
                                                        sum(ek2.kickedamount)
                                                from
                                                        erakick ek2
                                                where
                                                        ek2.erarecordid = erakick.erarecordid
                                                        and ek2.balancetransactiontype = erakick.balancetransactiontype), 0) <> 0
                                )
                        )"
                );

                #767241 - If this is a clonned batch and claim is unpaid, transactions would have posted earlier in RARbatch.
                #so dont show up in partial post list
                if (InList($type, qw(OVERPAID UNDERPAID))) {
                        $sql{$type}->Where(
                                "not exists (
                                        select
                                                1
                                        from
                                                erabatchcopymap
                                        where
                                                erabatchcopymap.erabatchid = erarecord.erabatchid
                                                and erabatchcopymap.copyreason = 'ERCM'
                                                and 0 = nvl((   select
                                                                        sum(rarrecord.payment)
                                                                from
                                                                        erarecord rarrecord
                                                                where
                                                                        rarrecord.erabatchid = erarecord.erabatchid
                                                                        and rarrecord.claimid = erarecord.claimid
                                                                        and rarrecord.transfertype = erarecord.transfertype
                                                ),0)
                                )",
                        );
                }
                push(@clauses, $sql{$type});
        }

        if ($toggle) {
                return @clauses;
        } else {
                # Combine all the partiallypostedtypes together:
                my $sqlunion = SQL::Select->Union(@clauses);

                return $sqlunion;
        }
}

