sub _DoMoreKickTransformations {
        my ($self, $dbh, $args) = @_;
        AssertValidFields($args,[qw(ERARECORDS USERNAME ERAFORMAT PAYMENTBATCHID)]);
        AssertRequiredFields($args,[qw(ERARECORDS USERNAME)]);
 
        #Hydra 928147 - Medicaid OH Wrap - Transfer Primary Contractual Amount to the Secondary
        my $pbrid = $self->GetPaymentBatchRouteID($dbh);
        if(InList($pbrid, qw(31 115 148 178 304 709 852 976))) {
                $self->MedicaidOHWrapBilling($dbh, $args);
        }
        #Hydra 192258 - MCRRHCCPT posting override
        #                When Medicare A is primary payer and the claim has a department value of type of bill 71 (i.e., RHC),
        #                We receive a secondary remit with kickreason.athenakickcode = CPT with action EOB or ERA(informational codes can be returned, but only CPT)
        #                and CROVR is present on 2ry along with no athena billing batch created for secondary
        #                then fire MCRRHCCPT
        my @kickcodeids = map { $_->{KICKREASONID} } map { @{$_->{KICKS}}} @{$args->{ERARECORDS}};
        my $cptkicksql = SQL::Select->new(
                )->Select(
                        "id",
                )->From(
                        "kickreason",
                )->Flags(
                        {TEMPTABLEOPTIN => 1}
                )->Where(
                        ["id in ( ?? )", \@kickcodeids],
                        "athenakickcode like 'CPT'",
                        ["not exists (select 1 from kickreason where id in ( ?? ) and athenakickcode not in ('CPT', 'INFORM', 'CONTRACT'))", \@kickcodeids],
        );
        my @onlycptcodeids = $cptkicksql->ColumnValues($dbh);
 
        if (($args->{ERARECORDS}[0]->{TRANSFERTYPE} eq '2') && scalar(@onlycptcodeids)) {
                my $claimid = $args->{ERARECORDS}[0]->{CLAIMID};
 
                my $sql = SQL::Select->new(
                        )->Select(
                                1
                        )->From(
                                "claim",
                                "erarecord",
                                "claimnote",
                                "department",
                                "typeofbill",
                                "patientinsurance",
                                "insurancepackage",
                                "insurancereportingcategory",
                        )->Joins(
                                "erarecord.claimid=claimnote.claimid",
                                "erarecord.claimid=claim.id",
                                "claim.departmentid=department.id",
                                "department.typeofbillid=typeofbill.id",
                                "claimnote.patientinsuranceid = patientinsurance.id",
                                "patientinsurance.insurancepackageid = insurancepackage.id",
                                "insurancepackage.insurancereportingcategoryid = insurancereportingcategory.id",
                        )->Where(
                                "claimnote.athenakickreasonid = 28",
                                "claimnote.transfertype = '1'",
                                "lower(insurancereportingcategory.name) like 'medicare a%'",
                                "typeofbill.code='71'",
                                ["claimnote.claimid = ?",$claimid],
                );
                my $medicareaconditionflag = $sql->Values($dbh);
 
                my $billingbatchsql= SQL::Select->new(
                        )->Select(
                                1
                        )->From(
                                "claimnote",
                        )->Where(
                                ["claimnote.claimid= ?",$claimid],
                                "claimnote.transfertype='2'",
                                "claimnote.action='BILL'",
                                "claimnote.billingbatchid is not null",
                );
                my $billingbatch = $billingbatchsql->Values($dbh);
 
                if($medicareaconditionflag && !($billingbatch)) {
                        foreach my $erarecord (@{$args->{ERARECORDS}}) {
                                my $kick = $erarecord->{KICKS} || [];
                                my @cptcodes = grep { InList($_->{KICKREASONID}, @onlycptcodeids) } @$kick;
                                foreach my $thiskick (@cptcodes) {
                                        $self->_ReplaceWithFakeKick($dbh, {
                                                ERARECORD      => $erarecord,
                                                ERAKICK        => $thiskick,
                                                USERNAME       => $args->{USERNAME},
                                                FAKEKICKCODE   => 'MCRRHCCPT',
                                                REMOVEORIGINAL => 1,
                                        });
                                }
                        }
                }
        }
 
        #Hydra 249108: FWRFILINGLIMIT automation
        $self->FWRFilingLimitOverride($dbh, {
                ERARECORDS => $args->{ERARECORDS},
                PAYMENTBATCHID => $args->{PAYMENTBATCHID},
                USERNAME => $args->{USERNAME},
        });
 
        foreach my $erarecord (@{$args->{ERARECORDS}}) {
                my $kick = $erarecord->{KICKS} || [];
                
                # Hydra #637732 - OA23:INFORM if primary
                
                my $transfertype = $erarecord->{TRANSFERTYPE};
 
                if ($transfertype eq '1') {
                        # get all the OA23 primary kicks and fire a fake kick as INFORM
                        my @oa23primarykicks = grep {$_->{KICKCODE} eq 'OA23'} @$kick;
                        foreach my $oa23kick (@oa23primarykicks) {
                                $self->_ReplaceWithFakeKick($dbh, {
                                                ERARECORD    => $erarecord,
                                                USERNAME    => $args->{USERNAME},
                                                ERAKICK        => $oa23kick,
                                                FAKEKICKCODE    => 'OA23PRIMARY',
                                });
                        }
                }
        
                # 492612 - PROVSIG Posting Override
                if (any { $_->{KICKCODE} eq 'PROVSIG' || $_->{ATHENAKICKCODE} eq 'PROVSIG' } @$kick) {
                        $self->TryApplyPROVSIGINFORM($dbh, {
                                ERARECORD    => $erarecord,
                                USERNAME    => $args->{USERNAME},
                        });
                }
 
                #Hydra 436583: If charge is 0.01, erarecord.payment = 0
                #And payercode BALANCETRANSACTIONREASON <> An adjustment or transfer then CONTRACT the penny balance
                my $tchargeamount = SQLValues("select 1 from tcharge where id = ? and voided is null and amount = 0.01 and transfertype =
                        ?",$dbh,$erarecord->{CHARGEID},$erarecord->{TRANSFERTYPE});
                #Hydra 965341: remove sum of payments/transfers condition from parents
                if ($tchargeamount == 1 && $erarecord->{PAYMENT} == 0) {
 
                        my @contractkicks;
                        @contractkicks = grep { $_->{BALANCETRANSACTIONTYPE} ne 'ADJUSTMENT' && $_->{KICKEDAMOUNT} == 0.01} @$kick;
                        foreach my $contractkick (@contractkicks) {
                                $self->_ReplaceWithFakeKick($dbh, {
                                                ERARECORD    => $erarecord,
                                                USERNAME    => $args->{USERNAME},
                                                ERAKICK        => $contractkick,
                                                FAKEKICKCODE    => 'PENNYADJCONTRACT',
                                });
                        }
                }
 
                # Hydra 506385: Create Generic Penny INFORM rule
                my $zerocharge = SQL::Select->new(
                        )->Select(
                                "1",
                        )->From(
                                "tcharge",
                        )->Where(
                                ["id = ?", $erarecord->{CHARGEID}],
                                "amount = 0",
                        )->Values($dbh);
 
                if ($zerocharge) {
 
                        my @kicks = grep { $_->{KICKEDAMOUNT} == 0.01 } @$kick;
                        my @athenakickcodes = UniqueElements(map { $_->{ATHENAKICKCODE} } @kicks);
 
                        my @adjustbalancereasoncodes = SQL::Select->new(
                                )->Select(
                                        "athenakickcode",
                                )->From(
                                        "kickreason",
                                )->Flags(
                                        {TEMPTABLEOPTIN => 1}
                                )->Where(
                                        ["kickcode in (??)", \@athenakickcodes],
                                        "balancetransactiontype = 'ADJUSTMENT'",
                                        "deleted is null",
                                )->ColumnValues($dbh);
 
                        my %adjustbalancereasoncodeshash = ();
                        foreach my $kickcode (@adjustbalancereasoncodes) {
                                $adjustbalancereasoncodeshash{$kickcode} = 1;
                        }
 
                        foreach my $thiskick (@kicks) {
                                $self->_ReplaceWithFakeKick($dbh, {
                                        ERARECORD => $erarecord,
                                        USERNAME => $args->{USERNAME},
                                        ERAKICK => $thiskick,
                                        FAKEKICKCODE => 'PENNYADJINFORM',
                                }) if ($adjustbalancereasoncodeshash{$thiskick->{ATHENAKICKCODE}});
                        }
                }
 
                # Hydra 185158,213216 - When tcharge.billed=0, SVC02=0 and SVC03=0, make sure the remark codes does not change
                #               the claim status and kick the charge with ZEROCHARGEINFORM.
 
                if (($erarecord->{AMOUNT} == 0) && ($erarecord->{PAYMENT} == 0)) {
                        my @recordkickreasonids = map {$_->{KICKREASONID}} @{$erarecord->{KICKS}};
                        my @informormanualclosekicks = SQL::Select->new(
                                )->Select(
                                        "distinct id",
                                )->From(
                                        "kickreason",
                                )->Flags(
                                        {TEMPTABLEOPTIN => 1}
                                )->Where(
                                        ["id in (??)",\@recordkickreasonids],
                                        "(requiresmanualcloseyn = 'Y' or athenakickcode = 'INFORM') ",
                        )->ColumnValues($dbh);
                        my $billedzeroflag = SQLValues ("select 1 from tcharge where id = ? and voided is null and amount = 0",$dbh, $erarecord->{CHARGEID});
                        if ($billedzeroflag == 1) {
                                my @firekicks = grep { !InList($_->{KICKREASONID},@informormanualclosekicks) } @{$erarecord->{KICKS}};
                                if (@firekicks) {
                                        foreach my $kick (@firekicks) {
                                                $kick->{INFORMONLY} = 1;
                                        }
                                        $self->_AddFakeKick($dbh, {
                                                ERARECORD    => $erarecord,
                                                USERNAME     => $args->{USERNAME},
                                                FAKEKICKCODE => 'ZEROCHARGEINFORM',
                                        });
                                }
                        }
                }
 
                # Hydra 161676: Skip calling NORC override for ABP
                unless ($args->{ERAFORMAT} eq 'ABP') {
 
                        # Hydra 82294: PR16, CO16, OA16 and PI16 must be changed into PR16NORC, CO16NORC etc.,
                        # unless they are returned with a remark code which is not mapped to INFORM,
                        # The same is true about PR17, CO17, OA17 or PI17.
                        # Hydra 92789: CO125 must be changed to CO125NORC unless they are returned with remarkcodes which is not mapped to INFORM.
                        # If remark codes are not present, claim should kick with CO125NORC
                        # Hydra 97285: Extend CO125 global override to OA125, PI125, and PR125
                        # Hydra 101939: CO226NORC override (also OA226, PI226, and PR226) . When SVC03=0, if it has the adjustment codes (CO226,
                        # OA226, PR226 or PI226) informational remark codes are remark codes whose athenakickcode has a null
                        # nextclaimstatus, balancetransactiontype, and balancetransactionreason. This excludeds athenakickcode DUPSUP, then
                        # kick the codes with NORC
                        # Hydra 103455: When SVC03 =0, a certain adjustment code (currently %16, %17, %125, and %226) is present in
                        # the SVC alone or with another adjustment code(s)/remark code(s) that has a balancetransactiontype of null
                        # or whose athenakickcode has null for nextclaimstatus, balancetransactiontype, and is not DUPSUP then change
                        # that adjustment code into NORC kickcode.
 
                        # Hydra 105840: Added another set of Kickcodes (COA1, PRA1, OAA1 and PIA1) along with Hydra 103455
 
                        # Hydra 114568: UPDATED NORC Spec: When SVC03 =0, a certain adjustment code (currently %16, %17, %125, %226 and %A1)
                        # is present in the SVC alone or with another adjustment code(s)/remark code(s) whose athenakickcode has null for
                        # nextclaimstatus, and is not DUPSUP then change that adjustment code into NORC kickcode.
 
                        # Hydra 118547: Added another set of Kickcodes (CO227, PR227, OA227 and PI227) along with Hydra 114568
                        # Hydra 190327: Added another set of kickcodes (CO129, PR129, OA129 and PI129) to NORC override
                        # Hydra 223173: Do not consider REMITRECEIVED as a remarkcode with nextclaim status for the purpose of the NORC overrides.
                        # Hydra 337445: Added %252 claim adjustment codes to NORC
                        # Hydra 407408: NORC logic for payer code OA96
 
                        my @payercode = ('OA96');
                        my @othercodes   = grep { ($_->{KICKCODE} ne 'REMITRECEIVED') && ($_->{KICKCODE} !~ /^(?:CO|PR|OA|PI)(?:1(?:6|7|25|29)|226|227|252|A1)$/) && (!InList($_->{KICKCODE}, @payercode))} @$kick;
                        my @othercodeids = map { $_->{KICKREASONID} } @othercodes;
                        my @athenakickcodes;
                        my $nextclaimstatus;
                        my @codes1617 = grep { (InList($_->{KICKCODE}, @payercode) || ($_->{KICKCODE} =~ /^(CO|PR|OA|PI)(1(6|7|25|29)|226|227|252|A1)$/)) && ($_->{KICKEDAMOUNT} != 0) } @$kick;
                        my @carccodeids = map { $_->{KICKREASONID} } @codes1617;
 
                        if (($erarecord->{PAYMENT} == 0) && @codes1617) {
                                my $kickcodesql = SQL::Select->new(
                                        )->Select(
                                                "distinct athenakickcode",
                                        )->From(
                                                "kickreason",
                                        )->Flags(
                                                {TEMPTABLEOPTIN => 1}
                                        )->Where(
                                                ["id in ( ?? )", \@carccodeids],
                                );
                                @athenakickcodes = $kickcodesql->ColumnValues($dbh);
 
                                my $kickcodesql2 = SQL::Select->new(
                                        )->Select(
                                                "distinct id"
                                        )->From(
                                                "kickreason",
                                        )->Flags(
                                                {TEMPTABLEOPTIN => 1}
                                        )->Where(
                                                ["kickcode in ( ?? )", \@athenakickcodes],
                                                ["kickreasoncategoryid = 0"],
                                );
                                my @athenakickcodeids = $kickcodesql2->ColumnValues($dbh);
                                push @carccodeids, @athenakickcodeids;
 
                                my $kickcodesql3 = SQL::Select->new(
                                        )->Select(
                                                "count (*)",
                                        )->From(
                                                "kickreason",
                                        )->Flags(
                                                {TEMPTABLEOPTIN => 1}
                                        )->Where(
                                                "nextclaimstatus is not null",
                                                ["id in ( ?? )", \@carccodeids],
                                        );
 
                                my $carcnextclaimstatus = $kickcodesql3->Values($dbh);
                                if (@othercodes) {
                                        my $sql = SQL::Select->new(
                                                )->Select(
                                                        "distinct athenakickcode",
                                                )->From(
                                                        "kickreason",
                                                )->Flags(
                                                        {TEMPTABLEOPTIN => 1}
                                                )->Where(
                                                        ["id in ( ?? )", \@othercodeids],
                                        );
                                        @athenakickcodes=$sql->ColumnValues($dbh);
 
                                        my $sql2 = SQL::Select->new(
                                                )->Select(
                                                        "distinct id"
                                                )->From(
                                                        "kickreason",
                                                )->Flags(
                                                        {TEMPTABLEOPTIN => 1}
                                                )->Where(
                                                        ["kickcode in ( ?? )", \@athenakickcodes],
                                                        ["kickreasoncategoryid = 0"],
                                        );
                                        my @athenacodeids = $sql2->ColumnValues($dbh);
                                        push @othercodeids, @athenacodeids;
 
                                        my $sql3 = SQL::Select->new(
                                                )->Select(
                                                        "count (*)",
                                                )->From(
                                                        "kickreason",
                                                )->Flags(
                                                        {TEMPTABLEOPTIN => 1}
                                                )->Where(
                                                        "athenakickcode = 'DUPSUP' or
                                                        nextclaimstatus is not null",
                                                        ["id in ( ?? )", \@othercodeids],
                                                );
 
                                        $nextclaimstatus = $sql3->Values($dbh);
                                }
 
                                unless ($nextclaimstatus || $carcnextclaimstatus) {
                                        foreach my $thiskick (@codes1617) {
                                                $self->_ReplaceWithFakeKick($dbh, {
                                                        ERARECORD    => $erarecord,
                                                        USERNAME     => $args->{USERNAME},
                                                        ERAKICK      => $thiskick,
                                                        FAKEKICKCODE => ($thiskick->{KICKCODE} . 'NORC'),
                                                });
                                        }
                                }
                        }
                }
 
                # Hydra 134891: Updated Balance and Non-Balance determination Logic to consider Athenakick
                # Hydra 80969: When a kick with balance behavior and another kick with no balance behavior
                # are returned on a denied charge, the kick that has balance behavior should be replaced
                # with the fake kick ADJTRANSINFORM, to prevent the claim from falling to partial post.
                # Update: only map balance kicks to ADJTRANSINFORM if at least one non-balance kick
                # is not mapped to either INFORM or PASSTHRU.
                # Hydra 894345 - ADJTRANSINFORM should not fire on records with MCRDEMONSTRATIONCIP
                if ($erarecord->{PAYMENT} == 0) {
                        my @kickcodeids = map { $_->{KICKREASONID} } @$kick;
                        my (@athenakickcodes, @nonbalanceathenakickcodes, @nonbalancekickids);
 
                        my $sql = SQL::Select->new(
                                )->Select(
                                        "distinct athenakickcode",
                                )->From(
                                        "kickreason",
                                )->Flags(
                                        {TEMPTABLEOPTIN => 1}
                                )->Where(
                                        ["id in ( ?? )", \@kickcodeids],
                                        "balancetransactiontype is null",
                                        "balancetransactionreason is null",
                        );
                        @athenakickcodes = $sql->ColumnValues($dbh);
 
                        my $sql2 = SQL::Select->new(
                                )->Select(
                                        "distinct kickcode",
                                )->From(
                                        "kickreason",
                                )->Flags(
                                        {TEMPTABLEOPTIN => 1}
                                )->Where(
                                        ["kickcode in ( ?? )", \@athenakickcodes],
                                        "balancetransactiontype is null",
                                        "balancetransactionreason is null",
                                        ["kickreasoncategoryid = 0"],
                        );
                        @nonbalanceathenakickcodes = $sql2->ColumnValues($dbh);
 
                        my $sql3 = SQL::Select->new(
                                )->Select(
                                        "distinct id",
                                )->From(
                                        "kickreason",
                                )->Flags(
                                        {TEMPTABLEOPTIN => 1}
                                )->Where(
                                        ["athenakickcode in ( ?? )", \@nonbalanceathenakickcodes],
                                        ["id in ( ?? )", \@kickcodeids],
                                );
                        @nonbalancekickids = $sql3->ColumnValues($dbh);
 
                        my (@balancekicks, @nonbalancekicks);
 
                        my $ptx_rule_denials_setting = BusCall::PracticeStructure::GetTablespaceValueWithDefault($dbh, { CONTEXTID => 1, KEY => 'POSTING_RULES_DENIAL_UPDATE', }) eq 'ON';
 
                        if ($args->{ERAFORMAT} eq 'ABP') {
                                @balancekicks     = grep { ! InList($_->{KICKREASONID}, @nonbalancekickids) } @$kick;
                                @nonbalancekicks  = grep { InList($_->{KICKREASONID}, @nonbalancekickids) } @$kick;
                        } else {
                                @balancekicks     = grep { ! InList($_->{KICKREASONID}, @nonbalancekickids) && ($_->{KICKEDAMOUNT} != 0) } @$kick;
                                @nonbalancekicks  = grep { InList($_->{KICKREASONID}, @nonbalancekickids) && ($_->{KICKEDAMOUNT} != 0) } @$kick;
                        }
 
                        if ($ptx_rule_denials_setting) {
                                @balancekicks = grep {
                                                $_->{KICKCODE}
                                                && $_->{KICKCODE} ne 'INCIDENTALADJUST'
                                        } @balancekicks;
                        }
                        @balancekicks = grep {
                                $_->{KICKCODE} && !InList($_->{KICKCODE}, qw(MVAPIPNEXTPAYOR PTRESP))
                        } @balancekicks;
                        if (@balancekicks && @nonbalancekicks) {
                                my @nbkickids = map { $_->{KICKREASONID} } @nonbalancekicks;
                                my $kick0218 = grep { $_->{KICKCODE} eq '0218' } @$kick;
                                my $kick0260 = grep { $_->{KICKCODE} eq '0260' } @$kick;
                                my $nbsql = SQL::Select->new(
                                        )->Select(
                                                "count(*)",
                                        )->From(
                                                "kickreason",
                                        )->Flags(
                                                {TEMPTABLEOPTIN => 1}
                                        )->Where(
                                                "athenakickcode <> 'INFORM'",
                                                "athenakickcode <> 'PASSTHRU'",
                                                "athenakickcode <> 'EOBRCVD'",
                                                "athenakickcode <> 'REMITRECEIVED'",
                                                "kickcode <> 'MCRDEMONSTRATIONCIP'",
                                                ["id in ( ?? )", \@nbkickids],
                                        );
                                my $trigger = $nbsql->Values($dbh);
 
                                # CRZS-13362 - payer returns two adjustment codes out of which one is general for example CO16 which is mapped to INFORM,
                                # and other adjustment code is mapped to Balance behavioral Athena Kickcode.
                                # But the general adjustment code is returned with remark code which is mapped to NEXT action Athena kick code,
                                # Then, Fire ADJTRANSINFORM to suppress the balance behaviour.
                                my $toggle = Athena::RolloutToggle::GetEnabledVersion($dbh, {KEY => 'CRZS_13362_ADDITIONAL_LOGIC'}) eq 'ON';;
                                my ($informkicks, @remarkkicks, $nextclaimstatus);
                                if ($toggle) {
                                        $informkicks = SQL::Select->new(
                                                )->Select(
                                                        "count(*)",
                                                )->From(
                                                        "kickreason",
                                                )->Flags(
                                                        {TEMPTABLEOPTIN => 1}
                                                )->Where(
                                                        "athenakickcode = 'INFORM'",
                                                        ["id in ( ?? )", \@nbkickids],
                                                )->Values($dbh);
                                        @remarkkicks = map {$_->{KICKREASONID}} grep {$_->{KICKCODE} =~/^[MN]/ } @$kick;
                                        if(@remarkkicks) {
                                                $nextclaimstatus = ERA::Utils::DoesKicksHaveNextClaimStatus($dbh,{
                                                        KICKREASONIDS => \@remarkkicks,
                                                });
                                        }
                                }
 
                                if (($trigger || ( $toggle && $informkicks && $nextclaimstatus )) && !$kick0218 && !$kick0260) {
                                        foreach my $balancekick (@balancekicks) {
                                                $self->_ReplaceWithFakeKick($dbh, {
                                                        ERARECORD    => $erarecord,
                                                        USERNAME     => $args->{USERNAME},
                                                        ERAKICK      => $balancekick,
                                                        FAKEKICKCODE => 'ADJTRANSINFORM',
                                                });
                                        }
                                }
                        }
                }
        }
        # Hydra 167163 - For context ID 2474, when kickreason.athenakickcode is in (NMN, ADNPRAC, ADNREVIEW, SERVICEMAX)
        #         or (OON and AUTH returned on the same charge) with action EOB or ERA, an attachment placeholder
        #         does not exist on the claim, and there are no attachments on a claim, then fire Kickcode MEDREC.
 
        if ($self->GetPracticeID() == 2474) {
                $self->PostingOverridesForVohraHealth($dbh, HashSlice($args, [qw(ERARECORDS USERNAME ERAFORMAT)]));
        }
        # Hydra 622682 - [MexExpress] Suppress copay transfer for George's Chicken employees
        if ($self->GetPracticeID() == 3908 && $self->GetFileFormat() eq 'ANSI835') {
                $self->SuppressCopayFor3908($dbh,$args);
        }
 
        # Hydra 224230 - [GENERIC] Treat DUPSUP denials as DUPCHG during posting when returned after single billing event
        foreach my $erarecord (@{$args->{ERARECORDS}}) {
 
                my $kicks = $erarecord->{KICKS} || [];
                my @krids = map { $_->{KICKREASONID} } @$kicks;
                my $sql = SQL::Select->new(
                        )->Select(
                                "id",
                                "athenakickcode",
                        )->From(
                                "kickreason",
                        )->Flags(
                                {TEMPTABLEOPTIN => 1}
                        )->Where(
                                ["id in ( ?? )", \@krids],
                                "athenakickcode in ('DUPSUP', 'DUPCHG')",
                );
                my @dupkicks = $sql->TableHash($dbh);
 
                my (@dupsupkicks, @dupchgkicks);
                # Collecting DUPSUP and DUPCHG kicks separately.
                foreach my $dupkick (@dupkicks) {
                        if ($dupkick->{ATHENAKICKCODE} eq 'DUPSUP') {
                                push @dupsupkicks, (grep {$_->{KICKREASONID} eq $dupkick->{ID}} @$kicks);
                        }
                        elsif ($dupkick->{ATHENAKICKCODE} eq 'DUPCHG') {
                                push @dupchgkicks, (grep {$_->{KICKREASONID} eq $dupkick->{ID}} @$kicks);
                        }
                }
 
                if (@dupsupkicks || @dupchgkicks) {
                        my @samechargerecords = grep {$_->{CHARGEID} eq $erarecord->{CHARGEID} && $_->{ID} ne $erarecord->{ID}} @{$args->{ERARECORDS}};
                        my $remittanceonmatchedcharge = 0;
                        foreach my $samechargerecord (@samechargerecords) {
                                my $chargekicks = $samechargerecord->{KICKS} || [];
                                foreach my $chargekick (@$chargekicks) {
                                        if ($chargekick->{BALANCETRANSACTIONTYPE} ne '' && ($args->{ERAFORMAT} ne 'ANSI835' || $chargekick->{KICKEDAMOUNT} > 0)) {
                                                $remittanceonmatchedcharge = 1;
                                                last;
                                        }
                                }
                                $remittanceonmatchedcharge = 1 if ($samechargerecord->{PAYMENT} > 0);
                                last if ($remittanceonmatchedcharge);
                        }
 
                        my @sameclaimrecords = grep { $_->{CLAIMID} eq $erarecord->{CLAIMID} && $_->{ID} ne $erarecord->{ID} } @{$args->{ERARECORDS}};
                        my $denialkicksonsameclaim = 0;
                        foreach my $sameclaimrecord (@sameclaimrecords) {
                                my $claimkicks = $sameclaimrecord->{KICKS} || [];
                                my @claimkrids = map { $_->{KICKREASONID} } @$claimkicks;
                                my $denialsql = SQL::Select->new(
                                        )->Select(
                                                "kr.id",
                                        )->From(
                                                "kickreason kr",
                                                "kickreason akr"
                                        )->Flags(
                                                {TEMPTABLEOPTIN => 1}
                                        )->Where(
                                                ["kr.id in ( ?? )", \@claimkrids],
                                                "kr.athenakickcode = akr.kickcode",
                                                "akr.kickcode != 'REMITRECEIVED'",
                                                "akr.nextclaimstatus is not null"
                                );
                                my @denialkicks = $denialsql->ColumnValues($dbh);
                                # Checking if denial kicks are there.
                                if (@denialkicks) {
                                        $denialkicksonsameclaim = 1;
                                        last;
                                }
                        }
                        my ($querystatus, $currentstatus);
                        if ($erarecord->{TRANSFERTYPE} eq '1' || $erarecord->{TRANSFERTYPE} eq '2') {
                                $querystatus = 'status' . $erarecord->{TRANSFERTYPE};
                                $currentstatus = SQLValues("select $querystatus from claim where id = ?", $dbh, $erarecord->{CLAIMID});
                        }
 
                        my $patientinsurancepackageid = BusCall::Claim::ClaimGetInsurancePackage($dbh,{
                                CLAIMID         => $erarecord->{CLAIMID},
                                TRANSFERTYPE    => $erarecord->{TRANSFERTYPE},
                                IDONLY          => 1,
                        });
                        my @billingevents = SQLColumnValues("
                                select
                                        id
                                from
                                        claimnote
                                where
                                        claimid = ?
                                        and transfertype = ?
                                        and patientinsuranceid in (
                                                select
                                                        id
                                                from
                                                        patientinsurance
                                                where
                                                        insurancepackageid = ?
                                        )
                                        and (
                                                (claimnote.action = 'BILL' and claimnote.transfertype <> 'p')
                                                or (claimnote.action = 'APPEAL' and claimnote.claimstatus = 'BILLED')
                                                or (claimnote.transfertype = 'p' and claimnote.kickreasonid = 17137)
                                                or (claimnote.kickreasonid in (27177, 17293, 51712, 24221, 29039, 24824, 67870))
                                        )
                        ", $dbh, $erarecord->{CLAIMID}, $erarecord->{TRANSFERTYPE}, $patientinsurancepackageid);
 
                        if (!$remittanceonmatchedcharge && (!$denialkicksonsameclaim || @dupchgkicks) && $currentstatus eq 'BILLED' && @billingevents == 1 && $erarecord->{REVERSALFLAG} ne 'Y') {
                                my $rules2_0_migration_toggle = Athena::Conf::AthenaNet::AthenaXConf()->get("rollout.CRZS_13266_RULES2_0_MIGRATION");
                                my $bridmappings = ($rules2_0_migration_toggle) ? BusCall::Claim::GetGlobalBizReqInfoUsingLegacyId($dbh, { LEGACYRULEIDS => [ '4820' ]}) : [];
                                my $claimrulefired;
                                # Make sure claim rule 4820 fired.
                                if ($rules2_0_migration_toggle && $bridmappings->[0]->{ID} ne '') {
                                        $claimrulefired = SQL::Select->new(
                                                )->Select(
                                                        1
                                                )->From(
                                                        'claimnote',
                                                )->Where(
                                                        ["claimid = ?", $erarecord->{CLAIMID}],
                                                        ["transfertype = ?", $erarecord->{TRANSFERTYPE}],
                                                        ["patientinsuranceid = ?", $erarecord->{PATIENTINSURANCEID}],
                                                        ["id < ?", $billingevents[0]],
                                                        SQL->Or(
                                                                "claimruleid = 4820",
                                                                ["bizrequirementid = ?", $bridmappings->[0]->{ID}],
                                                        ),
                                                        "pendingflag = 'O'",
                                                )->Values($dbh);
                                }
                                else {
                                        $claimrulefired = SQLValues("
                                                select
                                                        1
                                                from
                                                        claimnote
                                                where
                                                        claimid = ?
                                                        and transfertype = ?
                                                        and patientinsuranceid = ?
                                                        and id < ?
                                                        and claimruleid = 4820
                                                        and pendingflag = 'O'
                                        ", $dbh, $erarecord->{CLAIMID}, $erarecord->{TRANSFERTYPE}, $erarecord->{PATIENTINSURANCEID}, $billingevents[0]);
                                }
 
                                if ($claimrulefired) {
                                        if (@dupsupkicks && !$denialkicksonsameclaim) {
                                                foreach my $thiskick (@dupsupkicks) {
                                                        $self->_ReplaceWithFakeKick($dbh, {
                                                                ERARECORD      => $erarecord,
                                                                ERAKICK        => $thiskick,
                                                                USERNAME       => $args->{USERNAME},
                                                                FAKEKICKCODE   => 'DUPSUPCLMDUP',
                                                        });
                                                }
 
                                        }
                                        elsif (@dupchgkicks) {
                                                foreach my $thiskick (@dupchgkicks) {
                                                        $self->_ReplaceWithFakeKick($dbh, {
                                                                ERARECORD      => $erarecord,
                                                                ERAKICK        => $thiskick,
                                                                USERNAME       => $args->{USERNAME},
                                                                FAKEKICKCODE   => 'DUPCHGCLMDUP',
                                                                REMOVEORIGINAL => 1,
                                                        });
                                                }
                                        }
                                }
                                elsif (@dupsupkicks && !$denialkicksonsameclaim) {
                                        foreach my $thiskick (@dupsupkicks) {
                                                $self->_ReplaceWithFakeKick($dbh, {
                                                        ERARECORD      => $erarecord,
                                                        ERAKICK        => $thiskick,
                                                        USERNAME       => $args->{USERNAME},
                                                        FAKEKICKCODE   => 'DUPSUPDUPCHG',
                                                });
                                        }
                                }
                        }
 
                }
        }
 
        # Hydra 366457: Rules has developed a generic suite to stamp the savedscrubdata table to indicate we should wrap the charge
        # to a second secondary on the claim.
        # During posting to a claim, we should check savedscrubdata to confirm if
        # We should wrap bill or Which patientinsurance should be used to bill any remaining balance.
        my $erarecords = $args->{ERARECORDS};
        my $claimid = $erarecords->[0]->{CLAIMID};
        my $transfertype = $erarecords->[0]->{TRANSFERTYPE};
        my @erarecordids = map {$_->{ID}} @$erarecords;
        my $pbrid = $self->GetPaymentBatchRouteID($dbh);
         if ($transfertype eq '1') {
                my $wraptopatientins = SQL::Select->new(
                        )->Select(
                                "1",
                        )->From(
                                "erarecord",
                                "claim",
                                "savedscrubdata",
                                "patientinsurance",
                        )->Flags(
                                {TEMPTABLEOPTIN => 1}
                        )->Where(
                                "erarecord.claimid = savedscrubdata.claimid",
                                "savedscrubdata.claimid = claim.id",
                                "savedscrubdata.transfertype = erarecord.transfertype",
                                "savedscrubdata.fieldname = 'WrapToInsurancePackage'",
                                "claim.secondarypatientinsuranceid = patientinsurance.id",
                                "patientinsurance.insurancepackageid = savedscrubdata.value",
                                "savedscrubdata.activeyn = 'Y'",
                                "savedscrubdata.deleted is null",
                                ["erarecord.id in (??)", \@erarecordids],
                                ["erarecord.claimid in (??)", $claimid],
                )->TableHash($dbh);
                if ($wraptopatientins) {
                        my $secondaryinsurancepackageid = BusCall::Claim::ClaimGetInsurancePackage($dbh, {
                                CLAIMID    => $claimid,
                                TRANSFERTYPE    => '2',
                                IDONLY    => 1,
                        });
 
                        # no denial adjustment kicks (mapped to HOLD,MGRHOLD,CBOHOLD,ATHENAHOLD). no charges on claim are OVERPAID:
                        my $chargesoverpaid = 0;
                        foreach (@$erarecords) {
                                my $secondarybilledamount = $self->ReturnBilledAmountOnCharge($dbh, $_->{CHARGEID}, '2');
                                my $secondarypayment = $self->ReturnPaymentOnCharge($dbh, $_->{CHARGEID}, '2');
                                if ($secondarypayment > $secondarybilledamount) {
                                        $chargesoverpaid = 1;
                                        last;
                                }
                        }
                        if ($secondaryinsurancepackageid && ($chargesoverpaid == 0)) {
                                foreach my $erarecord (@$erarecords) {
                                        my $existingpayment = $self->ReturnPaymentOnCharge($dbh, $erarecord->{CHARGEID}, '2');
                                        # If there are no payments on the charge already,Move amount that is CONTRACT (kick or canonical) to fakekick WRAPPOSTINGNEXTPAYOR
                                        next if ($existingpayment);
                                        
                                        my $kicks = $erarecord->{KICKS} || [];
                                        my @krids = map {$_->{KICKREASONID}} @$kicks;
                                        my $denialkickcount = $self->ReturnDenialKickCount($dbh, @krids);
                                        # We need to fire WRAPPOSTINGNEXTPAYOR only if there is no denial on the current charge
                                        next if ($denialkickcount);
 
                                        # Collecting all contract and other adjustment kicks
                                        my @contractandotherkicks = grep {
                                                (InList($_->{BALANCETRANSACTIONREASON},qw(CONTRACTUAL OTHER)) && ($_->{BALANCETRANSACTIONTYPE} eq 'ADJUSTMENT'))
                                                        && ($_->{KICKEDAMOUNT} > 0) } @$kicks;
                                        # Move amount that is CONTRACT or OTHERADJ (kick or canonical) to fakekick WRAPPOSTINGNEXTPAYOR:NEXTPAYOR
                                        foreach my $thiskick (@contractandotherkicks) {
                                                $self->_ReplaceWithFakeKick($dbh, {
                                                        ERARECORD    => $erarecord,
                                                        ERAKICK        => $thiskick,
                                                        USERNAME    => $args->{USERNAME},
                                                        FAKEKICKCODE    => 'WRAPPOSTINGNEXTPAYOR',
                                                });
                                        }
                                        # For ABP files we may have CONTRACTUAL or OTHERADJUSTMENT instead of kickcodes
                                        if (!(@contractandotherkicks) && ($erarecord->{CONTRACTUAL} || $erarecord->{OTHERADJUSTMENT}) && $pbrid == 1) {
                                                my $contractandotheradj = SumCurrency($erarecord->{CONTRACTUAL},$erarecord->{OTHERADJUSTMENT});
                                                $erarecord->{CONTRACTUAL} = '';
                                                $erarecord->{OTHERADJUSTMENT} = '';
                                                $erarecord->{operation} = 'Update';
                                                ProcessForm('ERARECORD',$dbh, $args->{USERNAME}, $erarecord, [ qw(CONTRACTUAL OTHERADJUSTMENT) ]);
                                                $self->_AddFakeKick($dbh, {
                                                        ERARECORD    => $erarecord,
                                                        USERNAME    => $args->{USERNAME},
                                                        KICKEDAMOUNT    => $contractandotheradj,
                                                        FAKEKICKCODE     => 'WRAPPOSTINGNEXTPAYOR',
                                                });
                                        }
                                }
                        }
                }
        }
        elsif (($transfertype eq '2')) {
                # If matching remittance to transfertype = '2', there exists savedscrubdata.field = WrapToPatientInsurance and
                # value <> the erarecord's patientinsuranceID and savedscrubdata.ACTIVEYN equals Y
                my $wraptopatientins = SQL::Select->new(
                        )->Select(
                                "1",
                        )->From(
                                "erarecord",
                                "claim",
                                "savedscrubdata",
                        )->Flags(
                                {TEMPTABLEOPTIN => 1}
                        )->Where(
                                "erarecord.claimid = savedscrubdata.claimid",
                                "savedscrubdata.claimid = claim.id",
                                "erarecord.transfertype = savedscrubdata.transfertype",
                                "savedscrubdata.fieldname = 'WrapToPatientInsurance'",
                                "erarecord.patientinsuranceid <> savedscrubdata.value",
                                "savedscrubdata.activeyn = 'Y'",
                                "savedscrubdata.deleted is null",
                                ["erarecord.id in (??)", \@erarecordids],
                                ["erarecord.claimid in (??)", $claimid],
                )->TableHash($dbh);
                if ($wraptopatientins) {
                        #no denial adjustment kicks (mapped to HOLD,MGRHOLD,CBOHOLD,ATHENAHOLD).no charges on claim are OVERPAID.
                        my $chargesoverpaid = 0;
                        my $claimdenialkickcount = 0;
                        foreach (@$erarecords) {
                                my $secondarybilledamount = $self->ReturnBilledAmountOnCharge($dbh, $_->{CHARGEID}, '2');
                                my $secondarypayment = $self->ReturnPaymentOnCharge($dbh, $_->{CHARGEID}, '2');
                                my $totalsecondarypayment = $secondarypayment + $_->{PAYMENT};
                                if ($totalsecondarypayment > $secondarybilledamount) {
                                        $chargesoverpaid = 1;
                                        last;
                                }
                        }
                        if ($chargesoverpaid == 0) {
                                foreach my $erarecord (@$erarecords) {
                                        my $kicks = $erarecord->{KICKS} || [];
                                        my @krids = map {$_->{KICKREASONID}} @$kicks;
                                        my $denialkickcount = $self->ReturnDenialKickCount($dbh, @krids);
                                        # We need to fire WRAPPOSTINGINFORM only if there is no denial on the current charge
                                        next if ($denialkickcount);
 
                                        my @transfercontractkicks = grep {
                                                (InList($_->{BALANCETRANSACTIONREASON},qw(CONTRACTUAL OTHER))
                                                        || (InList($_->{BALANCETRANSACTIONTYPE},qw(TRANSFER TRANSFERP)))) && ($_->{KICKEDAMOUNT} > 0)
                                        } @$kicks;
                                        # Move amount that is CONTRACT or OTHERADJ or any type of TRANSFER (kick or canonical) to fakekick WRAPPOSTINGINFORM:INFORM
                                        foreach my $thiskick (@transfercontractkicks) {
                                                $self->_ReplaceWithFakeKick($dbh, {
                                                        ERARECORD    => $erarecord,
                                                        ERAKICK        => $thiskick,
                                                        USERNAME    => $args->{USERNAME},
                                                        FAKEKICKCODE    => 'WRAPPOSTINGINFORM',
                                                });
                                        }
                                        # For ABP files we may have CONTRACTUAL or OTHERADJUSTMENT instead of kickcodes
                                        if (!(@transfercontractkicks) && ($erarecord->{CONTRACTUAL} || $erarecord->{OTHERADJUSTMENT}) && $pbrid == 1) {
                                                my $contractandotheradj = SumCurrency($erarecord->{CONTRACTUAL},$erarecord->{OTHERADJUSTMENT});
                                                $erarecord->{CONTRACTUAL} = '';
                                                ProcessForm('ERARECORD',$dbh, $args->{USERNAME}, $erarecord, [ qw(CONTRACTUAL OTHERADJUSTMENT) ]);
                                                $self->_AddFakeKick($dbh, {
                                                        ERARECORD    => $erarecord,
                                                        USERNAME    => $args->{USERNAME},
                                                        KICKEDAMOUNT    => $contractandotheradj,
                                                        FAKEKICKCODE    => 'WRAPPOSTINGINFORM',
                                                });
                                        }
                                }
                        }
                }
        }
 
        #h1009476 - As a Rite Aid user I want all claims that would be automatically transferred to the patient to hold for review for other insuracnes
        if ($self->GetPracticeID() == 11977) {
                my @krids =  map { $_->{KICKREASONID} } map { @{$_->{KICKS}} } @$erarecords;
                my $denialkickcount = $self->ReturnDenialKickCount($dbh, @krids);
                my $nextpayor;
 
                if ($claimid && $transfertype && !$denialkickcount) {
                        $nextpayor = BusCall::Claim::NextPayor($dbh, { CHARGE=>{
                                CLAIMID => $claimid,
                                TRANSFERTYPE => $transfertype,
                        }});
 
                        my @kicktypestolookup = ('TRANSFERP');
                        push @kicktypestolookup, 'TRANSFER' if $nextpayor eq 'p';
 
                        foreach my $erarecord (@$erarecords) {
                                my @kicks = @{$erarecord->{KICKS} || [] };
                                foreach my $kick (grep { InList($_->{BALANCETRANSACTIONTYPE}, @kicktypestolookup) } @kicks) {
                                        $self->_ReplaceWithFakeKick($dbh, {
                                                ERARECORD       => $erarecord,
                                                ERAKICK         => $kick,
                                                USERNAME        => $args->{USERNAME},
                                                FAKEKICKCODE    => 'PTRESPRVW',
                                        });
                                }
                        }
                }
 
                # hydra 1009499 - As a Rite Aid User I do not want to close claims in athena that are being worked in our other system
                my $denialkickids = SQL::Select->new(
                                )->Select(
                                        "count(kickreason.id)",
                                )->From(
                                        "kickreason",
                                        "kickreason athenakick",
                                )->Flags(
                                        {TEMPTABLEOPTIN => 1}
                                )->Where(
                                        "athenakick.kickreasoncategoryid = 0",
                                        "kickreason.athenakickcode = athenakick.kickcode",
                                        "athenakick.nextclaimstatus in ('HOLD','MGRHOLD')",
                                        ["kickreason.id in (??)",\@krids],
                                )->Values($dbh);
 
                my $kickfired = SQL::Select->new(
                                )->Select(
                                        "1",
                                )->From(
                                        "claimnote",
                                        "kickreason",
                                )->Where(
                                        "kickreason.id = claimnote.kickreasonid",
                                        "kickreason.kickcode = 'PTRESPRVW'",
                                        ["claimnote.claimid = ?", $claimid],
                                )->Values($dbh);
 
                my @exceptionalcase = grep { $_->{ATHENAKICKCODE} eq 'ADNEOB' } map { @{$_->{KICKS}} } @$erarecords;
 
                if (($denialkickids || (@exceptionalcase && $transfertype eq '1')) && !$kickfired) {
                        foreach my $erarecord (@$erarecords) {
                                my @kicks = @{$erarecord->{KICKS} || [] };
                                my $outstanding = SQL::Select->new(
                                                )->Select(
                                                        "sum(outstanding)",
                                                )->From(
                                                        "tcharge",
                                                )->Where(
                                                        ["parentchargeid =?", $erarecord->{CHARGEID}],
                                                        ["transfertype = ?", $erarecord->{TRANSFERTYPE}],
                                                )->Values($dbh);
 
                                $self->_AddFakeKick($dbh, {
                                        ERARECORD       => $erarecord,
                                        USERNAME        => 'ATHENA',
                                        KICKEDAMOUNT    => $outstanding,
                                        FAKEKICKCODE    => 'RITEAIDADJ',
                                });
                        }
                }
        }
 
        # Hydra - 992738 - [HRA] PR187 and OA187 Global CSA Payment Automation, Same ERA Batch and CLP Loop
        my @updatekicks;
        my @records;
        my @previousrecordcharges = SQL::Select->new()->Distinct(
                                )->Select(
                                        "erarecord.chargeid",
                                        "erarecord.claimstatuscode",
                                        "erakick.kickcode",
                                )->From(
                                        "erarecord",
                                        "erakick",
                                )->Where(
                                        "erarecord.id = erakick.erarecordid",
                                        ["erarecord.claimid = ?",$args->{ERARECORDS}[0]->{CLAIMID}],
                                )->TableHash($dbh);
 
        my @oldchargeids = map { $_->{CHARGEID} } @previousrecordcharges;
        my @oldkick187 = grep { (InList($_->{KICKCODE}, qw(OA187 PR187))) } @previousrecordcharges;
        my @oldclaimsatuscode = grep { $_->{CLAIMSTATUSCODE} =~ m/(19|20)/ } @previousrecordcharges;
        my $payment = SumCurrency(map { $_->{PAYMENT} } @{$args->{ERARECORDS}});
        my %defaultconf = (
                enabled => 0,
                contexts => [],
        );
        my $practiceid = Athena::Util::Database::SessionInfo($dbh)->{context};
        my $confdata = Athena::Conf::AthenaNet::AthenaXConf()->get("rollout.colpci.COLPCI_3334_GLOBAL_ADJUSTMENT") // \%defaultconf;
        my $confcontexts = (defined $confdata->{contexts}) ? $confdata->{contexts} : $defaultconf{contexts};
        my $globaladjenabled = (defined $confdata->{enabled}) ? $confdata->{enabled} : $defaultconf{enabled};
 
        foreach my $erarecord (@{$args->{ERARECORDS}}) {
                my $kick = $erarecord->{KICKS} || [];
                $erarecord->{INITIALADJUSTMENTAMOUNT} = SumCurrency(map { $_->{KICKEDAMOUNT} } @$kick);
 
                my @kicks187 = grep { (InList($_->{KICKCODE}, qw(PR187 OA187))) && ($_->{KICKEDAMOUNT} < 0) } @$kick;
                my @otherposprkicks = grep { ($_->{KICKCODE} =~ /^PR(?!(187$))/) && ($_->{BALANCETRANSACTIONTYPE} eq 'TRANSFER') && ($_->{KICKEDAMOUNT} > 0) } @$kick;
                @otherposprkicks = sort { $a->{KICKCODE} cmp $b->{KICKCODE} } @otherposprkicks;
                my $overridefired = 0;
 
                unless ((InList($erarecord->{CLAIMSTATUSCODE},'19','20') && InList($erarecord->{CHARGEID},@oldchargeids) && @oldkick187)
                        || @oldclaimsatuscode) {
 
                        if (@kicks187 && $erarecord->{REVERSALFLAG} ne 'Y'
                                && (($kicks187[0]->{DISTRIBUTEDYN} eq 'N' && abs($kicks187[0]->{KICKEDAMOUNT}) <= $payment)
                                || ($kicks187[0]->{DISTRIBUTEDYN} ne 'N' && abs($kicks187[0]->{KICKEDAMOUNT}) <= $erarecord->{PAYMENT}))) {
 
                                foreach my $otherprkick (@otherposprkicks) {
                                        $overridefired = 1;
                                        if (abs($kicks187[0]->{KICKEDAMOUNT}) <= $otherprkick->{KICKEDAMOUNT}) {
                                                $otherprkick->{KICKEDAMOUNT} = SumCurrency($kicks187[0]->{KICKEDAMOUNT}, $otherprkick->{KICKEDAMOUNT});
                                                $kicks187[0]->{KICKEDAMOUNT} = 0;
                                        } else {
                                                $kicks187[0]->{KICKEDAMOUNT} = SumCurrency($kicks187[0]->{KICKEDAMOUNT}, $otherprkick->{KICKEDAMOUNT});
                                                $otherprkick->{KICKEDAMOUNT} = 0;
                                        }
 
                                        push(@updatekicks, $otherprkick);
                                        push(@updatekicks, $kicks187[0]);
                                }
 
                                if ($overridefired == 1) {
                                        my $finaladjustmentamount = SumCurrency(map { $_->{KICKEDAMOUNT} } @$kick);
                                        if ($finaladjustmentamount != $erarecord->{INITIALADJUSTMENTAMOUNT}) {
                                                $erarecord->{PAYMENT} = $erarecord->{PAYMENT} - ($finaladjustmentamount - $erarecord->{INITIALADJUSTMENTAMOUNT});
                                                push(@records, $erarecord);
                                        }
 
                                        $self->_AddFakeKick($dbh, {
                                                ERARECORD    => $erarecord,
                                                USERNAME     => $args->{USERNAME},
                                                KICKEDAMOUNT => 0,
                                                FAKEKICKCODE => 'GLOBAL187CSAPMT',
                                        });
                                }
                        }
                }
 
                if ($globaladjenabled && InList($practiceid, @{$confcontexts})) {
                        # CRZS-4483 To hold Global posting and instead treat as Denials
                        my @globaladjkicks = grep { $_->{ATHENAKICKCODE} eq 'GLOBAL' && $_->{KICKEDAMOUNT} > 0 } @$kick;
 
                        foreach my $kick (@globaladjkicks) {
                                my $globalkickreasonref = BusCall::Claim::KickReasonLookup($dbh, {
                                        KICKREASONID => $kick->{KICKREASONID},
                                });
 
                                $self->_ReplaceWithFakeKick($dbh, {
                                        ERARECORD       => $erarecord,
                                        ERAKICK         => $kick,
                                        USERNAME        => $args->{USERNAME},
                                        FAKEKICKCODE    => 'GLOBALRVW',
                                }) if $globalkickreasonref->{POSTINGOVERRIDEYN} ne 'Y';
                        }
                }
        }
 
        my $credentialingfixtoggle = Athena::RolloutToggle::GetEnabledVersion($dbh, {KEY => 'COLPCI_4165_CREDENTIALING_POSTING_FIX'});
        if ($credentialingfixtoggle && $self->GetPracticeID() == 8042 && $self->GetType() ne 'REAPPLY' && $erarecords->[0]->{TRANSFERTYPE} eq '1') {
                my $newrule;
                my $credentialingsql = SQL::Select->new(
                )->Select(
                        "to_char(claimnote.created, 'YYYY/MM/DD HH24:MI:SS') created",
                )->From(
                        "claimnote",
                )->Where(
                        ["claimnote.claimid= ?",$args->{ERARECORDS}[0]->{CLAIMID}],
                        "claimnote.note like '%Privia Credentialing PQ Project%'",
                );
                my $credentialingdate = $credentialingsql->Values($dbh);
 
                if($credentialingdate) {
                        my $claimnotesql = SQL::Select->new(
                                )->Select(
                                        "to_char(claimnote.created, 'YYYY/MM/DD HH24:MI:SS') created",
                                )->From(
                                        "claimnote",
                                )->Where(
                                        ["claimnote.claimid= ?",$erarecords->[0]->{CLAIMID}],
                                        ["claimnote.transfertype= ?", $erarecords->[0]->{TRANSFERTYPE}],
                                        "claimnote.action='BILL'",
                        );
                        my @claimnotes = $claimnotesql->TableHash($dbh);
 
                        my @laterbilledevent = grep { $_->{CREATED} ge $credentialingdate } @claimnotes;
                        my @beforebilledevent = grep { $_->{CREATED} lt $credentialingdate } @claimnotes;
 
                        $newrule = scalar @laterbilledevent ? 0 : scalar @beforebilledevent ? 1 : $newrule;
                }
 
                if ($newrule) {
                        foreach my $erarecord (@{$args->{ERARECORDS}}) {
                                my $kick = $erarecord->{KICKS} || [];
                                my $ispaymentpresent = $erarecord->{PAYMENT} ? 1 : 0;
                                my $loccodefired;
                                if (@$kick) {
                                        foreach my $lockick (@$kick) {
                                                 if (!$ispaymentpresent || ($ispaymentpresent && $lockick->{BALANCETRANSACTIONTYPE} ne 'ADJUSTMENT' ) ) {
                                                        $self->_ReplaceWithFakeKick($dbh, {
                                                                ERARECORD    => $erarecord,
                                                                USERNAME    => $args->{USERNAME},
                                                                ERAKICK        => $lockick,
                                                                FAKEKICKCODE    => 'LOCCODE',
                                                                REMOVEORIGINAL  => 1,
                                                        });
                                                        $loccodefired = 1;
                                            }
                                        }
                                }
                                unless ($loccodefired) {
                                        $self->_AddFakeKick($dbh, {
                                                ERARECORD    => $erarecord,
                                                USERNAME     => $args->{USERNAME},
                                                FAKEKICKCODE => 'LOCCODE',
                                        });
                                }
                        }
                }
        }
 
        $self->EngineProcessTable($dbh, {
                OPERATION => 'Update',
                TABLENAME => 'ERAKICK',
                TABLEROWS => \@updatekicks,
                COLUMNNAMES => ['KICKEDAMOUNT'],
                USERNAME => $args->{USERNAME},
        });
        $self->EngineProcessTable($dbh, {
                OPERATION => 'Update',
                TABLENAME => 'ERARECORD',
                TABLEROWS => \@records,
                COLUMNNAMES => ['PAYMENT'],
                USERNAME => $args->{USERNAME},
        });
 
        # If there is a $0 remittance for the secondary $0 charge which was created for MedicareB,
                # mark the record so that the $0 charge can be closed after this claim posts. (see H271914, 288730)
        $self->MaybeMarkRecordForClosingSecondary($dbh, $args);
 
        # Hydra#320006 - [Health Safety Net] Non-Medicare primary copay HSN secondary
        $self->TransferNonMediCareHSNCopayToPatient($dbh, $args);
 
        $self->TryReduceNegativeTransfers($dbh, $args);
 
        #988090 - Create MCRABNNEXTPAYOR Kick
        $self->TransferToNextPayerIfABNSigned($dbh, $args);
}

