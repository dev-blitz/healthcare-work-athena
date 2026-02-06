#########################################################################################
# _AppliedClaimCheckClaimStatus
#
# Description
#    - In some cases check the new claimnotes and maybe make alterations.
#
# Parameters
#    $dbh: the database handle.
#    $args:
#        ERARECORDS
#        PAYMENTBATCHID
#        POSTDATE
#        USERNAME
#
# Return Value:
#    none
#########################################################################################
sub _AppliedClaimCheckClaimStatus {
    my ($self, $dbh, $args) = @_;

    # 579903 - Put Denied OTHERSYSBILLED claims into a Manager Hold
    $self->MoveOtherSysBilledClaims($dbh, $args);

    $self->_MedicareReadjudication1Overpaid2TrumpDenial2($dbh, $args);

    # Closes the Secondary medicare $0 charge if the record is matched to a $0
    # secondary charge, which has MCRDEDUCTINFORM kick fired in its ClaimNote.
    # see H288730
    foreach my $group (GroupBy(['PARENTERARECORDID'], @{$args->{ERARECORDS}})) {
        if ($group->[0]->{CLOSESECONDARYZERODOLLARCHARGE}) {
            my $medicarezerotransferclose = SQLValues("
                select
                    id
                from
                    kickreason
                where
                    kickreason.kickcode = ?
                    and kickreason.deleted is null
                    and kickreason.kickreasoncategoryid = 1705
            ", $dbh, 'MEDICAREZEROTRANSFERCLOSE');

            # Check if there is no outstanding on the claim
            my $outstanding = SQLValues("select outstanding2 from claim where id = ?", $dbh, $args->{ERARECORDS}[0]->{CLAIMID});
            $self->ApplyClaimNote($dbh, {
                CLAIMID => $args->{ERARECORDS}[0]->{CLAIMID},
                TRANSFERTYPE => '2',
                KICKREASONID => $medicarezerotransferclose,
                PAYMENTBATCHID => $args->{PAYMENTBATCHID},
            }) unless ($outstanding);
        }
    }

    my @erarecords = @{$args->{ERARECORDS}};
    my $claimid = $erarecords[0]->{CLAIMID};
    my $transfertype = $erarecords[0]->{TRANSFERTYPE};

    foreach my $erarecord (@erarecords) {
        my @kickreasonids = map { $_->{KICKREASONID} } @{ $erarecord->{KICKS} || [] };
        my @athenakickcodes = SQL::Select->new()->Select(
            "athenakickcode",
            "id",
        )->From(
            "kickreason",
        )->Flags(
            {TEMPTABLEOPTIN => 1}
        )->Where(
            ["kickreason.id in (??)", \@kickreasonids],
        )->TableHash($dbh);

        my @bdekickreaonids = map { $_->{ID} } grep { $_->{ATHENAKICKCODE} eq 'BDE' } @athenakickcodes;
        my @bdekicks = grep { InList($_->{KICKREASONID}, @bdekickreaonids) } @{ $erarecord->{KICKS} || [] };
        if (@bdekicks) {
            my %insurancepackage = BusCall::Claim::ClaimGetInsurancePackage($dbh, {
                CLAIMID => $claimid,
                TRANSFERTYPE => $transfertype,
                CONTEXTID => GetPracticeID(),
            });

            my $insurancereportingcategoryid = $insurancepackage{INSURANCEREPORTINGCATEGORYID};
            # If the claim received a prior BDE denial for the current IRC,
            # An appeal was submitted since the prior BDE,
            # and current BDE occurred at least seven days since the appeal,
            # Total claim charge is less than $5,000.
            # The patient insurance ID on the appeal note is
            # associated with the same insurance reporting category as both BDE notes,
            # add claimnote with BDEFWRAPPEALFAIL.

            my $sqlobj = SQL::Select->new(
            )->Select(
                "distinct appealnote.claimid",
            )->From(
                "claimnote appealnote",
                "claimnote firstbdenote",
                "insurancepackage appealinsurancepackage",
                "insurancepackage firstbdeinsurancepackage",
                "patientinsurance appealpatientinsurance",
                "patientinsurance firstbdepatientinsurance",
            )->Joins(
                "firstbdenote.claimid = appealnote.claimid",
                "firstbdenote.transfertype = appealnote.transfertype",
                "appealnote.patientinsuranceid = appealpatientinsurance.id",
                "appealpatientinsurance.insurancepackageid = appealinsurancepackage.id",
                "firstbdenote.patientinsuranceid = firstbdepatientinsurance.id",
                "firstbdepatientinsurance.insurancepackageid = firstbdeinsurancepackage.id",
            )->Where(
                ["appealinsurancepackage.insurancereportingcategoryid = ?", $insurancereportingcategoryid],
                ["firstbdeinsurancepackage.insurancereportingcategoryid = ?", $insurancereportingcategoryid],
                ["appealnote.claimid = (?)", $claimid],
                ["appealnote.transfertype = ?", $transfertype],
                "appealnote.action = 'APPEAL'",
                "firstbdenote.action in ('EOB', 'ERA')",
                "firstbdenote.athenakickreasonid = 14",
                "firstbdenote.created < appealnote.created",
                "(sysdate - appealnote.created) >= 7",
                "not exists (
                    select 1
                    from tcharge
                    where
                        tcharge.claimid = appealnote.claimid
                        and tcharge.type = 'CHARGE'
                    having sum(tcharge.amount) > 5000
                )",
                ["not exists (
                    select 1
                    from claimnote
                    where
                        claimnote.claimid = appealnote.claimid
                        and claimnote.transfertype = appealnote.transfertype
                        and claimnote.action in ('EOB', 'ERA')
                        and claimnote.athenakickreasonid not in (14, 11218)
                        and (
                            (firstbdenote.created < claimnote.created
                            and claimnote.created < appealnote.created)
                            or
                            (appealnote.created < claimnote.created
                            and claimnote.created < sysdate)
                        )
                        and claimnote.paymentbatchid <> ?
                )", $args->{PAYMENTBATCHID}],
            );

            my @appealfailclaims = $sqlobj->ColumnValues($dbh);
            my $fwrappealfail = SQLValues("
                select
                    id
                from
                    kickreason
                where
                    kickreason.kickcode = ?
                    and kickreason.deleted is null
                    and kickreason.kickreasoncategoryid = 0
            ", $dbh, 'FWRAPPEALFAIL');

            if (@appealfailclaims) {
                $self->ApplyClaimNote($dbh, {
                    CLAIMID => $claimid,
                    TRANSFERTYPE => $transfertype,
                    KICKREASONID => $fwrappealfail,
                    PAYMENTBATCHID => $args->{PAYMENTBATCHID},
                });
            }
            last;
        }
    }

    return;
}
# outstanding is the claim
# medicarezerotransferclose is the kickreason
# bdekicks = kick reason id array

# join tables:
# 1. appealnote
# 2. insurancepackage
# 3. patientinsurance
# 4. claimnote.action = 'APPEAL'
