package ERA::Engine::TXMedicaid;
 
use strict;
use warnings;
no warnings qw(uninitialized);
use base qw(ERA::Engine);
 
use AthenaUtils;
use RemitMatch;
use ERA::Engine;
use ERA::Utils;
use List::MoreUtils qw(any);
use ERA;
use SQL::Select;
use Athena::ServiceBus::Dispatcher;
 
#########################################################################################
# _DoMoreKickTransformations
#
# Description:
#
# Hydra 125426 - If CLP04 >0,and SVC03 for CPTs in the code range 90465-90468, 90471-90474 is greater than 0,
#         SVC03 for codes in the code range 90632-90749 is 0 and OA16, N362, and M54 are present in the SVC
#         and patient's age <= 18 on date of service  
#         then change OA16 into MCDTXOA16CONTRACT
#
# Parameters:
#       $dbh: the database handle.
#       $args:
#               USERNAME   => the username that will be saved in the database
#               ERARECORDS => arrayref of the records that were in one and the same CLP loop.
#
# Return Value:
#       none - the ERARECORDS hash is manipulated directly, as is the database.
#########################################################################################
 
sub _DoMoreKickTransformations {
        my ($self,$dbh,$args) = @_;
 
        AssertRequiredFields($args,[qw(ERARECORDS USERNAME)]);
 
        # Hydra 172501 - When PBR = Medicaid-TX; the claim has a department value of type of bill 71; MA125 is returned on
        #              a paid charge within the CLP; other charges are billed and denied with M54 and/or CO125; then move
        #              the balance from CO125 to the fake kick MCDTXRHCMP    
        my @otherrecords = grep {!(grep {$_->{KICKCODE} eq 'MA125'} @{$_->{KICKS}})} @{$args->{ERARECORDS}};
        my @ma125records = grep {$_->{PAYMENT} > 0} grep {(grep {$_->{KICKCODE} eq 'MA125'} @{$_->{KICKS}})} @{$args->{ERARECORDS}};
        my @othercount = grep {!(grep {$_->{KICKCODE} eq 'M54' || $_->{KICKCODE} eq 'CO125'} @{$_->{KICKS}})} @otherrecords;
        if (@ma125records && !@othercount && !grep { $_->{PAYMENT} != 0 } @otherrecords) {
                my $bill71flag = SQLValues("select 1 from claim, department, typeofbill where claim.id =? and claim.departmentid = department.id and department.typeofbillid = typeofbill.id and typeofbill.code = '71'",$dbh, $args->{ERARECORDS}->[0]->{CLAIMID});
                if ($bill71flag) {
                        foreach my $erarecord (@otherrecords) {
                                my $kick = $erarecord->{KICKS};
                                my @co125 = grep { $_->{KICKCODE} eq 'CO125' } @$kick;
                                foreach my $thiskick (@co125) {
                                        $self->_ReplaceWithFakeKick($dbh, {
                                                ERARECORD    => $erarecord,
                                                USERNAME     => $args->{USERNAME},
                                                ERAKICK      => $thiskick,
                                                FAKEKICKCODE => 'MCDTXRHCMP',
                                        });
                                }
                        }
                }
        }
 
        my (@proccodezero,@proccodepos);
        foreach my $rec (@{$args->{ERARECORDS}}) {
 
                my ($proccode) = $rec->{PROCEDURECODE} =~ /^(\d*)/;
                $proccode ||= 0;
                if ( ($proccode >= 90632) && ($proccode <= 90749) && ($rec->{PAYMENT} == 0) ) {
                        push @proccodezero,$rec;
                }
                elsif ( ((($proccode >= 90465) && ( $proccode <=90468 )) || (($proccode >= 90471) && ($proccode <= 90474))) && ($rec->{PAYMENT} > 0)  ) {
                        push @proccodepos,$rec;
                }
                #Hydra 202176,224662 - When PBR is Medicaid TX and COA2 is returned on an unpaid charge with claimstatuscode 4,
                #            reduce COA2 to 0 and move the balance to MCDTXCOA2CPT.
                #Hydra 233786 - If the procedure code has a modifier when parent override conditions are true, fire MCDTXCOA2CPT.
                #               If procedure code does not have a modifier when parent override conditons are true, fire MCDTXCOA2DENIED.
                if (($rec->{PAYMENT} == 0) && ($rec->{CLAIMSTATUSCODE} == 4)) {
                        my $kick = $rec->{KICKS};
                        my @coa2 = grep { $_->{KICKCODE} eq 'COA2' } @$kick;
                        my $fakekickcode;
                        $fakekickcode = ($rec->{PROCEDURECODE} =~ /,/) ? 'MCDTXCOA2CPT' : 'MCDTXCOA2DENIED';
                        if (@coa2) {
                                foreach my $thiskick (@coa2) {
                                        $self->_ReplaceWithFakeKick($dbh, {
                                                ERARECORD    => $rec,
                                                USERNAME     => $args->{USERNAME},
                                                ERAKICK      => $thiskick,
                                                FAKEKICKCODE => $fakekickcode,
                                        });
                                }
                        }
                }
        }
        foreach my $erarecord (@proccodezero) {
                my $kick = $erarecord->{KICKS};
                my @oa16 = grep { $_->{KICKCODE} eq 'OA16' } @$kick;
                my @m54  = grep { $_->{KICKCODE} eq 'M54' } @$kick;
                my @n362 = grep { $_->{KICKCODE} eq 'N362' } @$kick;
                my $age  = SQLValues("select trunc((months_between(servicedate,dob))/12) from claim,client where claim.id =? and claim.patientid=client.id",$dbh, $erarecord->{CLAIMID});
 
                if(@proccodezero && @proccodepos && @oa16 && @m54 && @n362 && ($age <= 18)){
                        foreach my $thiskick (@oa16) {
                                $self->_ReplaceWithFakeKick($dbh, {
                                        ERARECORD    => $erarecord,
                                        USERNAME     => $args->{USERNAME},
                                        ERAKICK      => $thiskick,
                                        FAKEKICKCODE => 'MCDTXOA16CONTRACT',
                                });
                        }
                }
        }
 
        #Hydra 177714 : When PBR = Medicaid-TX, SVC03 = 0 and CO22 is with a remark code mapped to a next claim status, turn CO22 into MCDTXCO22INFORM.
 
        foreach my $erarecord (@{$args->{ERARECORDS}}) {
                my $kick = $erarecord->{KICKS};
                my @co22 = grep { $_->{KICKCODE} eq 'CO22' } @$kick;
                my @remarkcodes = map { $_->{KICKREASONID} } grep { $_->{KICKCODE} =~ /^(MA?|N)\d+$/ } @$kick;
                if (@co22 && @remarkcodes && ($erarecord->{PAYMENT} == 0)) {
                        my $nextclaimstatus = ERA::Utils::DoesKicksHaveNextClaimStatus($dbh, {
                                KICKREASONIDS => \@remarkcodes
                        });
 
                        if ($nextclaimstatus) {
                        #Hydra 243297: When PBR = Medicaid-TX, SVC03 = 0 and CO22 is returned with N192 (alone or with other remark
                        #codes), do not fire MCDTXCO22INFORM when a Medicare Replacement Plan is primary indicated by either:
                        #insuranceproducttype.id in (14,15,16,22) OR insuranceproducttype.id in (1,2,3) AND
                        #insurancepackage.name like %Medicare%
                                my @n192 = grep { $_->{KICKCODE} eq 'N192' } @$kick;
                                my $practiceid = GetPracticeID();
                                my %primaryinsurancepackage = BusCall::Claim::ClaimGetInsurancePackage($dbh, {
                                        CLAIMID         => $erarecord->{CLAIMID},
                                        TRANSFERTYPE    => 1,
                                        CONTEXTID       => $practiceid,
                                });
                                #Hydra 285912 : MCDTXCO22INFORM issue. Bug fix of h#243297.
                                if (
                                        !(
                                                @n192 &&
                                                (
                                                        InList($primaryinsurancepackage{INSURANCEPRODUCTTYPEID},(14,15,16,22)) || ## no critic (ProhibitMagicNumbers)
                                                        (
                                                                InList($primaryinsurancepackage{INSURANCEPRODUCTTYPEID}, (1,2,3)) && ## no critic (ProhibitMagicNumbers)
                                                                $primaryinsurancepackage{NAME} =~ /medicare/i
                                                        )
                                                )
                                        ) &&
                                        !(@n192 && $erarecord->{TRANSFERTYPE} eq '2')
                                ) {
                                        foreach my $thiskick (@co22) {
                                                $self->_ReplaceWithFakeKick($dbh, {
                                                        ERARECORD    => $erarecord,
                                                        USERNAME     => $args->{USERNAME},
                                                        ERAKICK      => $thiskick,
                                                        FAKEKICKCODE => 'MCDTXCO22INFORM',
                                                });
                                        }
                                }
                        }
                }
        }
 
        #Hydra 283028: If
        #    * Medicaid-TX sends a denial (SVC03 =0) with N55 on the charge that matches to secondary
        #    * A Medicare is registered as Primary
        #    * There exists a CROVR1 kick in claimnotes
        #    If MCDTXN55DROP does not already exist in claimnotes (we only ever want this to fire once on the claim)
        #    if all conditions are met, then fire fakekick MCDTXN55DROP and throw away N55.
        my $practiceid = GetPracticeID();
        my @erarecords = @{$args->{ERARECORDS}};
        my %primaryinsurancepackage = BusCall::Claim::ClaimGetInsurancePackage($dbh, {
                CLAIMID => $erarecords[0]->{CLAIMID},
                TRANSFERTYPE => 1,
                CONTEXTID => $practiceid,
        });
        if(($erarecords[0]->{TRANSFERTYPE} eq '2') && ($primaryinsurancepackage{NAME} =~ /medicare/i)) {
                my @unpaidrecordswithn55 = grep { ($_->{PAYMENT} == 0) && (List::MoreUtils::any { $_->{KICKCODE} eq 'N55' } @{$_->{KICKS} }) } @erarecords;
                my @unpaidrecordswithm56 = grep { ($_->{PAYMENT} == 0) && (List::MoreUtils::any { $_->{KICKCODE} eq 'M56' } @{$_->{KICKS} }) } @erarecords;
                if (@unpaidrecordswithn55 || @unpaidrecordswithm56) {
                        my $crovr1 = SQLValues("
                                select
                                        1
                                from
                                        claimnote,
                                        kickreason
                                where
                                        kickreason.id = claimnote.kickreasonid
                                        and kickreason.athenakickcode like 'CROVR1'
                                        and claimnote.transfertype = '1'
                                        and claimnote.claimid = ?
                        ", $dbh, $erarecords[0]->{CLAIMID});
 
                        if ($crovr1 && @unpaidrecordswithn55) {
                                my $mcdtxn55drop = SQLValues("
                                        select
                                                1
                                        from
                                                claimnote,
                                                kickreason
                                        where
                                                kickreason.kickcode = 'MCDTXN55DROP'
                                                and kickreason.id = claimnote.kickreasonid
                                                and claimnote.claimid = ?
                                ",$dbh,$erarecords[0]->{CLAIMID});
                                if(!$mcdtxn55drop) {
                                        #Disable nextclaimstatus of all other kicks on the claim.
                                        my @allclaimkicks = map { @{$_->{KICKS}} } @erarecords;
                                        foreach (@allclaimkicks) {
                                                $_->{INFORMONLY} = 1;
                                        }
                                        foreach my $erarecord (@unpaidrecordswithn55) {
                                                my $kick = $erarecord->{KICKS} || [];
                                                my @n55 = grep { $_->{KICKCODE} eq 'N55' } @$kick;
                                                foreach my $thiskick (@n55) {
                                                        $thiskick->{INFORMONLY} = undef; #MCDTXN55DROP should not inherit INFORMONLY behaviour from N55
                                                        $self->_ReplaceWithFakeKick($dbh, {
                                                                ERARECORD      => $erarecord,
                                                                USERNAME       => $args->{USERNAME},
                                                                ERAKICK        => $thiskick,
                                                                FAKEKICKCODE   => 'MCDTXN55DROP',
                                                                REMOVEORIGINAL => 1,
                                                        });
                                                }
                                        }
                                }
                        }
 
                        # Hydra 517644 - [Medicaid-TX] MCDTXM56DROP
                        if ($crovr1 && @unpaidrecordswithm56) {
                                my $alreadyfired = SQLValues("
                                        select
                                                1
                                        from
                                                claimnote,
                                                kickreason
                                        where
                                                kickreason.kickcode in ('MCDTXM56DROP', 'MCDTXN55DROP')
                                                and kickreason.id = claimnote.kickreasonid
                                                and claimnote.claimid = ?
                                ",$dbh,$erarecords[0]->{CLAIMID});
                                if(!$alreadyfired) {
                                        #Disable nextclaimstatus of all other kicks on the claim.
                                        my @allclaimkicks = map { @{$_->{KICKS}} } @erarecords;
                                        foreach (@allclaimkicks) {
                                                $_->{INFORMONLY} = 1;
                                        }
                                        foreach my $erarecord (@unpaidrecordswithm56) {
                                                my $kick = $erarecord->{KICKS} || [];
                                                my @m56 = grep { $_->{KICKCODE} eq 'M56' } @$kick;
                                                foreach my $thiskick (@m56) {
                                                        $thiskick->{INFORMONLY} = undef; #MCDTXM56DROP should not inherit INFORMONLY behaviour from M56
                                                        $self->_ReplaceWithFakeKick($dbh, {
                                                                ERARECORD      => $erarecord,
                                                                USERNAME       => $args->{USERNAME},
                                                                ERAKICK        => $thiskick,
                                                                FAKEKICKCODE   => 'MCDTXM56DROP',
                                                                REMOVEORIGINAL => 1,
                                                        });
                                                }
                                        }
                                }
                        }
                }
        }
 
        $self->SUPER::_DoMoreKickTransformations($dbh, $args);
 
        # Hydra 207492 : When claimstatuscode <>4, and CO125NORC fires on claim, change it to MCDTXCO125CONTRACT.
 
        foreach my $erarecord (@{$args->{ERARECORDS}}) {
                my $kick = $erarecord->{KICKS};    
                my @co125norc = grep { $_->{KICKCODE} eq 'CO125NORC' } @$kick;
 
                if (@co125norc && ($erarecord->{CLAIMSTATUSCODE} != 4)) {
                        foreach my $thiskick (@co125norc) {
                                $self->_ReplaceWithFakeKick($dbh, {
                                        ERARECORD        => $erarecord,
                                        USERNAME         => $args->{USERNAME},
                                        ERAKICK          => $thiskick,
                                        FAKEKICKCODE     => 'MCDTXCO125CONTRACT',
                                        REMOVEORIGINAL     => 1,
                                });
                        }
                }
        }
}
 
#########################################################################################
# ApplyBatchPractice
#
# Description:
#       -Applies all the erarecords for an entire erabatch in a particular practice
#       -calls through to $self->ApplyClaim and ApplyRecord to do posting work
#       -Cannot be called from within another DBTransaction
#
# Parameters:
#       $dbh:  application databse handle object
#       $args: hashref of arguments:
#               ERABATCHID              (required)
#               USERNAME                (required)
#               PAYMENTBATCHID  (required)
#               PAYMENTMETHOD   (required)
#               CHECKCCNUMBER
#               POSTDATE
#
#               SHOWPERCENTAGEBAR => (boolean), print an HTML percentage-bar to STDOUT
#
# Return Value:
#       true if batch posts completely, false otherwise
#########################################################################################
 
sub ApplyBatchPractice {
        my $self = shift;
        my ($dbh,$args) = @_;
        return $self->SUPER::ApplyBatchPractice($dbh, $args) if ($self->GetType() eq 'REAPPLY');
 
        AssertRequiredFields($args,[qw(ERABATCHID USERNAME PAYMENTBATCHID PAYMENTMETHOD)]);
 
        #Hydra: 115102 - When the payer is Medicaid TX and PLB03-1 is returned as WU for $0,
        #        Unpostable should be discarded as a Provider Takeback and Closed.
 
        #Hydra: 140088 - When payer is Medicaid TX and BPR02 has an amount greater than zero
        #         and BPR04 equals NON, create one offset unpostable for the BPR02 amount
        #         and attach note to unpostable.
 
        my @exceptions = SQLTableHash("
                select * from erarecord where
                action = 'BATCHEXCEPTION'
                and eradiscardstatusreasonid = 'UNKNOWN'
                and plbreasoncode = 'WU'
                and applied is null
                and payment = 0
                and erabatchid = ?
                ", $dbh, $args->{ERABATCHID});
 
        my $practiceid = GetPracticeID();
 
        foreach my $exception (@exceptions) {
                $exception->{PAYMENTBATCHID} = SQLValues("select max(id) from paymentbatch where erabatchid = ?", $dbh, $args->{ERABATCHID});
                next unless ($exception->{PAYMENTBATCHID}); # Should never happen, but if it does, the unpostable will be created manually.
                $exception->{APPLIED}   = 'SYSDATE';
                $exception->{APPLIEDBY} = $args->{USERNAME};
                $exception->{ERADISCARDSTATUSREASONID} = 'PROVIDERTAKEBACK';
                $self->DiscardRecord($dbh, {
                        USERNAME => $args->{USERNAME},
                        GENERATESTATUS => 1,
                        ERARECORD => $exception,
                });
 
                my $unpostableeventid = SQLValues("select id from unpostableevent where name = 'Posted to athenaNet' and unpostabletypeid='PROVIDERTAKEBACK' and deleted is null", $dbh);
                my $unpostableid = SQLValues("select id from unpostable where voided is null and erarecordid = ?", $dbh, $exception->{ID});
 
                # If our ERA batch does not yet have a payment (the ERA received - check missing scenario)
                # then DiscardRecord will not create any unpostables from paid records.
                # In that event, we do not need to do anything else.
                if ($unpostableid) {
                        Unpostable::Update($dbh, {
                                ID                 => $unpostableid,
                                POSTDATE           => Today(),
                                UNPOSTABLEEVENTID  => $unpostableeventid,
                                UNPOSTABLESTATUSID => 'CLOSED',
                                CONTEXTID          => $practiceid,
                        });
                }
        }
 
        @exceptions = SQLTableHash("
                select * from erarecord where
                action = 'BATCHEXCEPTION'
                and eradiscardstatusreasonid = 'UNKNOWN'
                and note like 'Please contact Medicaid-TX as the money for this batch%'
                and erabatchid = ?
                and applied is null
                ", $dbh, $args->{ERABATCHID});
 
        foreach my $exception (@exceptions) {
                $exception->{PAYMENTBATCHID} = SQLValues("select max(id) from paymentbatch where erabatchid = ?", $dbh, $args->{ERABATCHID});
                next unless ($exception->{PAYMENTBATCHID}); # Should never happen, but if it does, the unpostable will be created manually.
                $exception->{APPLIED}   = 'SYSDATE';
                $exception->{APPLIEDBY} = $args->{USERNAME};
                $exception->{ERADISCARDSTATUSREASONID} = 'OFFSET';
                $self->DiscardRecord($dbh, {
                        USERNAME => $args->{USERNAME},
                        GENERATESTATUS => 1,
                        ERARECORD => $exception,
                });
 
                my $unpostableeventid = SQLValues("select id from unpostableevent where name = 'Created: Offset' and unpostabletypeid='OFFSET' and deleted is null", $dbh);
 
                my $unpostableid = SQLValues("select id from unpostable where voided is null and erarecordid = ?", $dbh, $exception->{ID});
 
                # If our ERA batch does not yet have a payment (the ERA received - check missing scenario)
                # then DiscardRecord will not create any unpostables from paid records.
                # In that event, we do not need to do anything else.
                if ($unpostableid) {
                        Unpostable::Update($dbh, {
                                ID                 => $unpostableid,
                                POSTDATE           => Today(),
                                UNPOSTABLEEVENTID  => $unpostableeventid,
                                UNPOSTABLESTATUSID => 'CLOSED',
                                CONTEXTID          => $practiceid,
                        });
                        Unpostable::AddNote($dbh, {
                                UNPOSTABLEID => $unpostableid,
                                CONTEXTID    => $practiceid,
                                NOTE         => "Please contact Medicaid-TX as the money for this batch is on hold and will remain so until an oustanding issue is resolved.",
                                USERNAME     => 'ATHENA',
                        });
                }
        }
 
        return $self->SUPER::ApplyBatchPractice($dbh,$args);
}
 
1;
