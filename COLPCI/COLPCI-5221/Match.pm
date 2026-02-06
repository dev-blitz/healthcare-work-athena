package ERA::Match;

#########################################################################################
# ERA::Match
#
# Description:
#	-part of ERA::Engine
# 	-this file is really just a collection of the methods that are used in the 
#	 "matching" of ERA: that is, both the association of an ERA Batch with a
#	 particular context, and the association of individual ERA Records with
#	 specific claims/charges
#
#########################################################################################

use Text::Soundex;
use Time::HiRes qw( gettimeofday tv_interval );

use AthenaUtils;
use AthenaDate;
use PatientInsurance;
use KickReasonCategory;
use Insurance;
use Claim;
use ERA::QA;
use CapPayment;
use ERA::DualPosting;
use SQL;
use SQL::Select;
use Unpostable::Route;
use AthenaCarp qw(confess reconfess);
use Athena::RolloutToggle;
use Athena::Util::Database qw(SessionInfo);
use Global;
use Athena::Conf::AthenaNet;
use Athena::Util::List qw(ListsSubsetOf);
use Try::Tiny;
use AthenaScribe;
use StatsD;
use Athena::Util::Assert qw( AssertValidFields AssertFields);
use Athena::Util::List qw(InList);
use POSIX qw(strftime);

use strict;

use constant MAXPOTENTIALCHARGES => 20_000;
use Athena::ServiceBus::Dispatcher;


#########################################################################################
# GetMatchingPaymentBatch
#
# Description:
#	-Wrapper for GetMatchingPaymentBatchesForERABatch
#	-Finds and returns the paymentbatch with which this erabatch should be posted
#	-Looks for a unique UNASSIGNED/assignedto paymentbatch with matching paymentbatchroute,
#		dollar amount, and checknumber (allowing for a somewhat fuzzy match on
#		checknumber: case-insensitive, and leading zeroes are ignored).
#
# Parameters:
#	$dbh:  application database handle object
#	$args: hashref of arguments:
#		ERABATCH - hashref
#		or
#		ERABATCHID
#
# Return Value:
#	%paymentbatch hash
#########################################################################################
sub GetMatchingPaymentBatch {

	my ($self, $dbh, $args) = @_;

	AssertValidFields($args,[qw( ERABATCHID ERABATCH )]);

	my $erabatch = $args->{ERABATCH} || {SQLHash("select * from erabatch where id=?",$dbh,$args->{ERABATCHID})};
	Assert($erabatch->{ID},"An erabatch must be specified");

	# If batch is associated, return that id
	my @paymentbatches = SQLTableHash("select * from paymentbatch where erabatchid = ?", $dbh, $erabatch->{ID});

	# Try for associated batch first
	#Hydra 154539 : Have general outsorting logic before specific outsorting overrides

	my $genericengine = new ERA::Engine($dbh, {});
	$genericengine->{PAYMENTBATCHROUTE} = DeepCopyStructure($self->{PAYMENTBATCHROUTE});

	# search by route, amount (totalsumamount), and checknumber
	@paymentbatches = $genericengine->GetMatchingBatches($dbh, {
		ERABATCH => $erabatch,
		AMOUNTTEST => 'TOTALSUMAMOUNT',
	}) unless @paymentbatches;

	# If we didn't find a match on totalsumamount, we check for a
	# payment batch matching on unpostedamount
	@paymentbatches = $genericengine->GetMatchingBatches($dbh, {
		ERABATCH => $erabatch,
		AMOUNTTEST => 'TOTALUNPOSTEDAMOUNT',
	}) unless @paymentbatches;

	# match is only good if it is a _unique_ match
	if (@paymentbatches == 1) {
		return %{$paymentbatches[0]};
	}
	# The generic outsorting fails, hence moving to specific outsorting.
	# search by route, amount (totalsumamount), and checknumber
	@paymentbatches = $self->GetMatchingBatches($dbh, {
		ERABATCH => $erabatch,
		AMOUNTTEST => 'TOTALSUMAMOUNT',
	}) unless @paymentbatches;

	# If we didn't find a match on totalsumamount, we check for a
	# payment batch matching on unpostedamount
	@paymentbatches = $self->GetMatchingBatches($dbh, {
		ERABATCH => $erabatch,
		AMOUNTTEST => 'TOTALUNPOSTEDAMOUNT',
	}) unless @paymentbatches;

	# match is only good if it is a _unique_ match
	if (@paymentbatches == 1) {
		return %{$paymentbatches[0]};
	}
	else {
		return ();
	}
}


#########################################################################################
# GetMatchingBatches
#
# Description:
#	- Finds ERA batches that match this payment batch, or payment
#	  batches that match this ERA batch, on pbr, dollar amount, and
#	  checknumber (allowing for a somewhat fuzzy match on
#	  checknumber: case-insensitive, and leading zeroes are
#	  ignored).  (Checknumber matching is the piece most often
#	  overridden.)
#	- If looking for payment batches, only finds those that are unassigned, open, and
#	  either not associated, or associated to this particular ERA batch.
#
# Parameters:
#	$dbh:  application database handle object
#	$args: hashref of arguments:
#		ERABATCH (hashref) or ERABATCHID - have ERA batch, looking for matching payment batches
#		PAYMENTBATCH (hashref) or PAYMENTBATCHID - have PB, looking for matching ERA batches
#	switches:
#		AMOUNTTEST (string) -
#			'TOTALSUMAMOUNT - the usual $ test
#			'TOTALUNPOSTEDAMOUNT' - 
#			'EITHER' - check totalsumamount, then TOTALUNPOSTEDAMOUNT if no match
#			'NONE' (default) - how to match totalpayments
#		CHECKALLPAYERS (boolean) -
#			Look for matching batches from all payers, not just $self->GetPaymentBatchRouteID
#
# Return Value:
#	@batches tablehash
#########################################################################################
sub GetMatchingBatches {
	my ($self, $dbh, $args) = @_;

	AssertValidFields($args, [qw( PAYMENTBATCHID PAYMENTBATCH ERABATCHID ERABATCH AMOUNTTEST CHECKALLPAYERS REPLACEFLAG )]);
	Assert(
		$args->{PAYMENTBATCH} || $args->{ERABATCH} || $args->{PAYMENTBATCHID} || $args->{ERABATCHID},
		'Must identify a target ERA Batch or Payment Batch.'
	);
	Assert(
		!($args->{PAYMENTBATCH} || $args->{PAYMENTBATCHID}) || !($args->{ERABATCH} || $args->{ERABATCHID}),
		'Cannot pass in both PAYMENTBATCH/ID and ERABATCH/ID.'
	);
	my $batchtype = ($args->{PAYMENTBATCH} || $args->{PAYMENTBATCHID}) ? 'PAYMENTBATCH' : 'ERABATCH';
	my $batch;

	# Do we have enough batch hash, or need more?
	if ($args->{PAYMENTBATCH} || $args->{ERABATCH}) {
		my @requiredfields;
		if ($batchtype eq 'PAYMENTBATCH') {
			push @requiredfields, 'TOTALSUMAMOUNT' if InList($args->{AMOUNTTEST}, (
				'TOTALSUMAMOUNT', 'TOTALUNPOSTEDAMOUNT', 'EITHER'
			));
			push @requiredfields, ( 'TOTALUNPOSTEDAMOUNT', 'TOTALAMOUNT' ) if InList($args->{AMOUNTTEST}, (
				'TOTALUNPOSTEDAMOUNT', 'EITHER'
			));
		}
		else {
			push @requiredfields, 'TOTALPAYMENTS' if InList($args->{AMOUNTTEST}, (
				'TOTALSUMAMOUNT', 'TOTALUNPOSTEDAMOUNT', 'EITHER'
			));
		}
		$batch = (!grep { !exists $args->{$batchtype}{$_} } @requiredfields) ?
			$args->{$batchtype} :
			$args->{$batchtype}{ID} ?
			{ SQLHash("select * from $batchtype where id=?", $dbh, $args->{$batchtype}{ID}) } :
			$args->{$batchtype . 'ID'} ?
			{ SQLHash("select * from $batchtype where id=?", $dbh, $args->{$batchtype . 'ID'}) } : ''
		;
		Assert((!grep { !exists ($batch->{$_}) } @requiredfields), 'Some required fields still missing!');
	}
	else {
		$batch = { SQLHash("select * from $batchtype where id=?", $dbh, $args->{$batchtype . 'ID'}) };
	}

	my $wantbatchtype = $batchtype eq 'PAYMENTBATCH' ? 'ERABATCH' : 'PAYMENTBATCH';

	# base query
	my $sql = SQL::Select->new()->Select(
		"$wantbatchtype.*"
	)->From(
		"$wantbatchtype"
	);
	my $paymentbatchrouteid = $self->GetPaymentBatchRouteID($dbh);
	if (($paymentbatchrouteid ne '') && (!$args->{CHECKALLPAYERS})) {
		$sql->Where(
			['paymentbatch.paymentbatchrouteid = ?', $paymentbatchrouteid],
		) if($wantbatchtype eq 'PAYMENTBATCH');
		$sql->Where(
			['erabatch.paymentbatchrouteid = ?', $paymentbatchrouteid],
		) if($wantbatchtype ne 'PAYMENTBATCH');
	}
	if ($batchtype eq 'PAYMENTBATCH') {
		$sql->Select('erabatch.paymentbatchrouteid');
		$sql->Where("erabatch.status <> 'DISCARDED'");
		$sql->Where("not exists (select 1 from erabatchcopymap where erabatchcopymap.erabatchid = erabatch.id)");
		if ($args->{CHECKALLPAYERS}) {
			$sql->From('paymentbatchroute')->Joins('paymentbatchroute.id = erabatch.paymentbatchrouteid');
			$sql->Where("paymentbatchroute.claimactionid = 'ERA'");
		}
	}

	# Additional conditions for case have ERABATCH want PAYMENTBATCH
	if ($batchtype eq 'ERABATCH') {
		$sql->Where(
			"paymentbatch.createdby = 'UNASSIGNED'",
			'paymentbatch.closed is null',
			['( paymentbatch.erabatchid is null or paymentbatch.erabatchid = ? )', $batch->{ID}]
		);
	}

	# AMOUNTTEST
	if ($args->{AMOUNTTEST}) {
		my $totalsumamountwhere;
		my $totalunpostedamountwhere;
		if ($batchtype eq 'PAYMENTBATCH') {
			# Hydra 131447: To help Oracle build a better explain plan,
			# pass in the totalsumamount as a literal value and not a bind when it is zero.
			if (0 == $batch->{TOTALSUMAMOUNT}) {
				$totalsumamountwhere = SQL->new('erabatch.totalpayments = 0');
			}
			else {
				$totalsumamountwhere = SQL->new(['erabatch.totalpayments = ?', $batch->{TOTALSUMAMOUNT}]);
			}
			$totalunpostedamountwhere = SQL->new([q{(
				? = 0
				and ? = 0
				and -erabatch.totalpayments = ?
			)}, $batch->{TOTALSUMAMOUNT}, $batch->{TOTALAMOUNT}, $batch->{TOTALUNPOSTEDAMOUNT}]);
		}
		else {
			$totalsumamountwhere = SQL->new(['paymentbatch.totalsumamount = ?', $batch->{TOTALPAYMENTS}]);
			$totalunpostedamountwhere = SQL->new([q{(
				paymentbatch.totalsumamount = 0
				and paymentbatch.totalamount = 0
				and -paymentbatch.totalunpostedamount = ?
			)}, $batch->{TOTALPAYMENTS}]);
		}
		if ($args->{AMOUNTTEST} eq 'TOTALSUMAMOUNT') {
			$sql->Where($totalsumamountwhere);
		}
		elsif ($args->{AMOUNTTEST} eq 'TOTALUNPOSTEDAMOUNT') {
			$sql->Where($totalunpostedamountwhere);
		}
		elsif ($args->{AMOUNTTEST} eq 'EITHER') {
			$sql->Where(["?? or ??", $totalsumamountwhere, $totalunpostedamountwhere]);
		}
	}
	# If we have a checknumber, then we want to match regularly
	if (($batch->{CHECKNUMBER} =~ /[^0 ]/) && ($batch->{CHECKNUMBER} !~ /^NA$/i) && ($batch->{CHECKNUMBER} !~ /^EFT$/i)) {
	
		my $checknumbersql = $sql->Clone();

		if ($batchtype eq 'ERABATCH') {
			$checknumbersql->Hints( "index(PAYMENTBATCH PAYMENTBATCH_CREATEDBY_CLOSED)" ) if ( $batch->{CHECKNUMBER} =~ /^0*1$/ );
		}

		# CHECKNUMBERTEST
		if (! $args->{CHECKALLPAYERS}) {
			$checknumbersql->Where($self->_GetMatchingBatchesForChecknumberWhere({$batchtype => $batch}));
			$checknumbersql->ReplaceSelect("1") if ($args->{REPLACEFLAG});
		}
		else {
			$checknumbersql->Where($self->_GetGenericMatchingBatchesForChecknumberWhere({$batchtype => $batch}));
		}
		my @possiblebatches = $checknumbersql->TableHash($dbh);
		
		return @possiblebatches if (scalar @possiblebatches > 0);
	
	}
	if ($wantbatchtype eq 'ERABATCH') {
	
		# Look for a match based on the OCRed claim IDs that are stored in pbscanpageclaim.
		
		my $ocredclaimids = SQL::Select->new()->Select(
			"count(distinct pbscanpageclaim.controlnumber)"
		)->From(
			"paymentbatchscanpage",
			"pbscanpageclaim",
		)->Where(
			"pbscanpageclaim.remitscanpageid = paymentbatchscanpage.remitscanpageid",
			["paymentbatchscanpage.paymentbatchid = ?", $args->{PAYMENTBATCH}{ID}],
		)->Values($dbh);
		
		return () unless $ocredclaimids;
		
		my $controlnumbersql = $sql->Clone();
		
		# A similar query has caused performance issues in the past (possibly because erarecord is not indexed on controlnumber).
		# Here, we want to find all ERA batches that have at least one control number in common with the OCRed claim IDs.
		# For each batch, we want to know: one, how many control numbers does it have in common with what was OCRed,
		# and two, how many control numbers does it have in total. No doubt there is a way to answer both questions with one query.
		# But I am very concerned that, if we make the query too complex, then it will break performance again.
		# So I am breaking it into two pieces. In the first piece, we count the number of control numbers in common.
		# Then we grep out the batches that do not have enough CNs in common with what was OCRed.
		# Then, for the remaining batches (if any), we run a second query for the total number of control numbers.
		
		# We cannot group by erabatch.* (which is what $sql is selecting), so we resort to a ReplaceSelect.
		$controlnumbersql->ReplaceSelect(
			"erabatch.id",
			"count(distinct erarecord.controlnumber) overlap",
		)->From(
			"erarecord",
			"pbscanpageclaim",
			"paymentbatchscanpage",
		)->Where(
			"erarecord.erabatchid = erabatch.id",
			"erarecord.controlnumber = pbscanpageclaim.controlnumber",
			"pbscanpageclaim.remitscanpageid = paymentbatchscanpage.remitscanpageid",
			["paymentbatchscanpage.paymentbatchid = ?", $args->{PAYMENTBATCH}{ID}],
		)->GroupBy(
			"erabatch.id"
		);

		my @possiblebatches = $controlnumbersql->TableHash($dbh);

		# To qualify, an ERA batch must contain at least 90% of the OCRed claim IDs.
		@possiblebatches = grep { ($_->{OVERLAP}/$ocredclaimids) >= $self->_MinOCRedControlNumberMatchThreshold() } @possiblebatches;
		return () unless (@possiblebatches);
		
		# Now, check how many control numbers each possible batch has in it.
		
		my $claimcountsql = SQL::Select->new()->Select(
			"erabatchid",
			"count(distinct controlnumber)",
		)->From(
			"erarecord",
		)->Flags({
			TEMPTABLEOPTIN => 1,
		})->Where(
			['erabatchid in ( ?? )', [map {$_->{ID}} @possiblebatches]],
		)->GroupBy(
			"erabatchid",
		);

		my %claimcounts = $claimcountsql->ColumnValues($dbh);

		# At least 51% of the distinct claim IDs in the ERA batch must be present among the OCRed claim IDs.
		@possiblebatches = grep { ($_->{OVERLAP}/$claimcounts{$_->{ID}}) >= $self->_MinERAControlNumberMatchThreshold() } @possiblebatches;
		return () unless (@possiblebatches);
		
		$sql->Flags({
			TEMPTABLEOPTIN => 1,
		})->Where(
			['erabatch.id in ( ?? )', [map {$_->{ID}} @possiblebatches]],
		);
		
		my @matchingbatches = $sql->TableHash($dbh);
		return @matchingbatches;
	}
	# Else, without checknumber and claimids
	else {
		return ();
	}
}

sub _MinOCRedControlNumberMatchThreshold {
	return .9;
}

sub _MinERAControlNumberMatchThreshold {
	return .51;
}



sub _GetMatchingBatchesForChecknumberWhere {
	my $self = shift;
	my $args = shift;
	AssertValidFields($args, [qw( PAYMENTBATCH ERABATCH )]);
	return
		$args->{PAYMENTBATCH} ?
		[q{(
			ltrim(upper(erabatch.checknumber),'0 ') = ltrim(upper(?),'0 ')
		)}, $args->{PAYMENTBATCH}{CHECKNUMBER}] :
		$args->{ERABATCH} ?
		[q{(
			ltrim(upper(paymentbatch.checknumber),'0 ') = ltrim(upper(?),'0 ')
		)}, $args->{ERABATCH}{CHECKNUMBER}] :
		'';
}

#########################################################################################
# _GetGenericMatchingBatchesForChecknumberWhere
#
# Description:
# 	-Works exactly the same as _GetMatchingBatchesForChecknumberWhere,
#	but ignores all payer-specific check matching overrides.
#########################################################################################
sub _GetGenericMatchingBatchesForChecknumberWhere {
	my $self = shift;
	my $args = shift;
	AssertValidFields($args, [qw( PAYMENTBATCH ERABATCH )]);
	return
		$args->{PAYMENTBATCH} ?
		[q{(
			erabatch.checknumber = ?  -- need for all 0's case
			or ltrim(upper(erabatch.checknumber),'0 ') = ltrim(upper(?),'0 ')
		)}, ($args->{PAYMENTBATCH}{CHECKNUMBER}) x 2] :
		$args->{ERABATCH} ?
		[q{(
			checknumber = ?  -- need for all 0's case
			or ltrim(upper(checknumber),'0 ') = ltrim(upper(?),'0 ')
		)}, ($args->{ERABATCH}{CHECKNUMBER}) x 2] :
		''
	;
}

#########################################################################################
# MatchBatch
#
# Description:
# 	-Matches the erarecords to chargeids for an entire erabatch
#	-loops over all network practices and calls MatchBatchPractice to do all the work
#	-records all actions having been done by user 'ATHENA' (the system posting user)
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCHID
#
#		SHOWPERCENTAGEBAR => (boolean), print an HTML percentage-bar to STDOUT
#		ERARECORDSQLWHERE => (SQL object), restricts match to the where clause represented by the object
#
# Return Value:
#	1 if all matches succeeded, otherwise 0
#########################################################################################
sub MatchBatch {
	my ($self, $dbh, $args) = @_;

	# flag to record whether or not all records in the batch get matched
	my $allrecordsmatched = 1;
	$self->{PERCENTAGEBAR_CURRENTRECORD} = 0;

	# since we just re-pass args hash around through sub-subroutines,
	# stick system username 'ATHENA' into a copy of args
	$args = {
		%$args,
		USERNAME => 'ATHENA'
	};

	my $erarecordsqlwhereclauses = $args->{ERARECORDSQLWHERE} || [];

	# since this is called by a fake system user 'ATHENA', need to toggle
	# off security checks in this (and inner) scope.  This is sorta like 
	# being setuid root!
	local $AthenaSecurity::BYPASSCHECK = 1;

	my $recordsql = SQL::Select->new(
		)->Select(
			"count(*)",
		)->From(
			"erarecord",
		)->Where(
			["erabatchid = ?", $args->{ERABATCHID}],
			"applied is null",
		);

	$recordsql->Where($_) for (@$erarecordsqlwhereclauses);

	my $recordcount;
	$recordcount = $recordsql->Values($dbh) if $args->{SHOWPERCENTAGEBAR};

	$args->{SHOWPERCENTAGEBAR} = 0 unless $recordcount;
	PercentageBar({TARGET=>$recordcount}) if $args->{SHOWPERCENTAGEBAR};

	my %erabatch = SQLHash("select * from erabatch where id = ?",$dbh,$args->{ERABATCHID});

	# In an enterprise practice, patient payments should match only
	# to providergroups that have access to the batch's bank account
	# (though we enforce this only for XStat practices).
	my @bankaccountprovidergroupids = SQLColumnValues(q{
		select
			bankstatementaccountentity.providergroupid
		from
			paymentbatch,
			depositbatch,
			providergroup,
			bankstatementaccountentity
		where
			paymentbatch.depositbatchid = depositbatch.id
			and depositbatch.bankstatementaccountid = bankstatementaccountentity.bankstatementaccountid
			and bankstatementaccountentity.providergroupid = providergroup.id
			and nvl(providergroup.restrictedyn,'N') != 'Y'
			and bankstatementaccountentity.providergroupid is not null
			and paymentbatch.erabatchid = ?
	}, $dbh, $args->{ERABATCHID});
	# -2 means "-Any-" provider group, so no restriction.
	if (@bankaccountprovidergroupids && !InList(-2, @bankaccountprovidergroupids)) {
		$erabatch{BANKACCOUNTPROVIDERGROUPIDS} = \@bankaccountprovidergroupids;
	}

	# loop over payment-network practices and match
	# have to be very careful not to leave the $dbh corrupted if we die in here
	eval {
		my $thispracticematched = $self->MatchBatchPractice($dbh,{
			USERNAME => $args->{USERNAME},
			ERABATCH => \%erabatch,
			ERARECORDSQLWHERE => $erarecordsqlwhereclauses,
			SHOWPERCENTAGEBAR => $args->{SHOWPERCENTAGEBAR},
			$args->{MATCHFROMUI} ? (MATCHFROMUI =>  $args->{MATCHFROMUI}) : (),
			$args->{MANUALUI} ? (MANUALUI =>  $args->{MANUALUI}) : (),
		});
		$allrecordsmatched &&= $thispracticematched;
	};
	my $error = $@;
	reconfess $error if $error ne '';

	return $allrecordsmatched;
}

#########################################################################################
# MatchBatchPractice
#
# Description:
# 	-Matches the erarecords to chargeids for an entire erabatch
#	-calls through to MatchClaim to do matching work
#	-sets all of the erarecord.chargeid's in the database
#	-records all actions having been done by user 'ATHENA' (the system posting user)
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCH
#		USERNAME
#
#		SHOWPERCENTAGEBAR => (boolean), print an HTML percentage-bar to STDOUT
#		ERARECORDSQLWHERE => (SQL object), restricts match to the where clause represented by the object
#
# Return Value:
#	1 if all matches succeeded, otherwise 0
#########################################################################################
sub MatchBatchPractice {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args,['ERABATCH','USERNAME']);

	# flag to record whether or not all records in the batch get matched
	my $allrecordsmatched = 1;
	my $erabatch = $args->{ERABATCH};
	unless ($args->{MATCHFROMUI}) {
		# a payor may have some rules for discarding batch exceptions right off
		DBTransaction(sub{$self->DiscardBatchBatchExceptions($dbh, $args)},$dbh);

		my $paymentbatchid = SQLValues("select id from paymentbatch where erabatchid = ? ", $dbh, $erabatch->{ID});

		# Handle Offsetting batchexception pairs to either mark as manually matched
		# or set eradiscardruleid
		if ($paymentbatchid) {
			$self->MarkOffsettingBatchExceptionERARecordPairs($dbh, { ERABATCH => $erabatch });

			#hydra 416096 - Auto-discard parity payments as incentive payments for set of KRCs
			#Since this will reduce posting touches
			$self->DiscardIncentivePaymentsForKRCs($dbh,$args);
		}
	}
	my $erarecordsqlwhereclauses = $args->{ERARECORDSQLWHERE} || [];

	my $recordsql = SQL::Select->new(
		)->Select(
			"*",
		)->From(
			"erarecord",
		)->Where(
			["erabatchid = ?", $erabatch->{ID}],
			"applied is null",
		)->OrderBy(
			"controlnumber",
			"id",
		);

	$recordsql->Where($_) for (@$erarecordsqlwhereclauses);

	my @erarecords = $recordsql->TableHash($dbh);

	@erarecords = $self->GetErakickData($dbh, {
		ERABATCHID=>$erabatch->{ID},
		ERARECORDS=>\@erarecords, 
	});

	# erarecord.action is a little bit of a kluge... it differentiates different
	# classes of erarecord (patient statement, batchexceptions, claim payments),
	# but also differentiates crossover claim payments from non-crossover (though
	# this distinction might some day be removed).
	map {
		$_->{ACTION_CLASS} = 
			($_->{ACTION} eq 'PATIENTPAYMENT') ? 'PATIENT'
				: ($_->{ACTION} eq 'BATCHEXCEPTION') ? 'BATCHEXCEPTION'
					: ($_->{ACTION} eq 'REFUNDREQUESTLINE') ? 'REFUNDREQUESTLINE'
						: 'CLAIM'
	} @erarecords;
	my @claims = GroupBy([['ACTION_CLASS','$','-'],['PARENTERARECORDID']],@erarecords);
	# Batch Exceptions must be processed before Refund Request Lines
	@claims = sort {
		if ($a->[0]->{ACTION} eq 'REFUNDREQUESTLINE' && $b->[0]->{ACTION} eq 'BATCHEXCEPTION') { 
			return 1; 
		} 
		elsif ($a->[0]->{ACTION} eq 'BATCHEXCEPTION' && $b->[0]->{ACTION} eq 'REFUNDREQUESTLINE') { 
			return -1;
		}
		else {
			return 0;
		}
	} @claims;
	
	foreach my $claim (@claims) {
		if ($claim->[0]->{ACTION} eq "PATIENTPAYMENT") {
			my $thispatientpaymentmatched = DBTransaction(sub {
				$self->MatchPatientAccount($dbh,{
					USERNAME => $args->{USERNAME},
					ERABATCH => $erabatch,
					ERARECORDS => $claim
				});
			},$dbh);

			$allrecordsmatched &&= $thispatientpaymentmatched;
		} 
		elsif (!InList($claim->[0]->{ACTION}, qw(BATCHEXCEPTION REFUNDREQUESTLINE))) {
			# When there is primary and secondary for a claim in the same batch with same patientinsuranceid,
			# and with a corrected insuranceidnumber only for the secondary, match the secondary to IP 18500.
			my @primaryinsuranceids = UniqueElements (map { $_->{PATIENTINSURANCEIDNUMBER} } grep { ($_->{CLAIMSTATUSCODE} =~ /^(?:1|19)$/) } @$claim);
			foreach my $insuranceid (@primaryinsuranceids) {
				my @secondaryloop = grep { ($_->{CLAIMSTATUSCODE} =~ /^(?:2|20)$/) && ($insuranceid eq $_->{PATIENTINSURANCEIDNUMBER})
							&& ($_->{CORRECTEDINSURANCEIDNUMBER} ne '')} @$claim;
				foreach my $secondaryrecord (@secondaryloop) {
					$secondaryrecord->{MATCHTOUNSPEC} = 1;
				}
			}

			my %prochash;
			foreach (@$claim) { $prochash{$_->{PROCEDURECODE}."::".$_->{AMOUNT}}++; }
			my @breaktieforcodes = grep { $prochash{$_} >= 2 } keys %prochash;
			foreach my $record (@$claim) {
				if (InList("$record->{PROCEDURECODE}::$record->{AMOUNT}", @breaktieforcodes)) {
					$record->{BREAKTIE} = 1;
				}
			}

			my $claimchecktoggle = Athena::Conf::AthenaNet::AthenaXConf()->get("rollout.colpmd.colpmd_121_2_0_claim_check");
			my $ifskipmatch = 0;
			
			if($claimchecktoggle && defined $claim->[0]->{CLAIMID}) {
				$ifskipmatch = BusCall::Claim::IsManagedByClaimService($dbh, {CLAIMID => $claim->[0]->{CLAIMID}});
			}

			# When grouping ERA records (all formats) for matching, group
			# by parenterarecord after grouping by control number. (see H 108459)
			my $thisclaimmatched;
			
			# Skip 1.0 Match for Claims created by 2.0 Claim service
			if(!$ifskipmatch) {
				$thisclaimmatched = DBTransaction(sub { 
					$self->MatchClaim($dbh,{
						USERNAME => $args->{USERNAME},
						ERABATCH => $erabatch,
						ERARECORDS => $claim,
						$args->{MANUALUI} ? (MANUALUI =>  $args->{MANUALUI}) : (),
					});
				},$dbh);
			}
			$allrecordsmatched &&= $thisclaimmatched;

		} 
		elsif ($claim->[0]->{ACTION} eq "BATCHEXCEPTION") {
			my $thisexceptionmatched = DBTransaction(sub {
				$self->MatchBatchException($dbh,{
					USERNAME => $args->{USERNAME},
					ERABATCH => $erabatch,
					ERARECORDS => $claim
				});
			},$dbh);
			$allrecordsmatched &&= $thisexceptionmatched;
		}
		elsif ($claim->[0]->{ACTION} eq 'REFUNDREQUESTLINE') {
			my $refundrequestsmatched = DBTransaction(sub {
				$self->MatchRefundRequests($dbh, {
					USERNAME => $args->{USERNAME},
					ERABATCH => $erabatch,
					ERARECORDS => $claim,
				});
			}, $dbh);
			$allrecordsmatched &&= $refundrequestsmatched;
		}

		if ($args->{SHOWPERCENTAGEBAR}) {
			$self->{PERCENTAGEBAR_CURRENTRECORD} += @$claim;
			PercentageUpdate($self->{PERCENTAGEBAR_CURRENTRECORD});
			### HACK!!! ###
			# print a whole bunch of whitespace to keep the buffer moving through the proxy
			print (' 'x4096);
			### HACK!!! ###
			print "\n";
			BufferFlush();
		}
	}

	# Update erarecord.aegisinsurancepackageid, erabatch.aegisinsreportingcategoryid
	# and erabatch.aegiskickreasoncategoryid.
	UpdateERARecordERABatchAegisInfo($dbh, {
		ERARECORDS => \@erarecords,
		ERABATCH => $erabatch,
		USERNAME => $args->{USERNAME},
	})  unless ($args->{MATCHFROMUI});

	return $allrecordsmatched;
}


#########################################################################################
# UpdateERARecordERABatchAegisInfo
#
# Description:
#	This sets some values needed for the practice version of claimops.erarecordview:
#	- erarecord.aegisinsurancepackageid
#	- erabatch.aegiskickreasoncategoryid
#	- erabatch.aegisinsreportingcategoryid
#	(Before this, these were stored in claimops.erarecordinfo and erabatchinfo.)
#
# Parameters:
#	ERARECORDS
#	ERABATCH
#	USERNAME
#
# Return Value:
#	(none - updates database directly)
#########################################################################################
sub UpdateERARecordERABatchAegisInfo {
	my ($dbh, $args) = @_;

	AssertFields($args, [qw(ERARECORDS ERABATCH USERNAME)]);

	# First, handle the erarecords.

	undef $_->{AEGISINSURANCEPACKGEID} for @{ $args->{ERARECORDS} };
	# Leave null for PATIENTPAYMENTs.
	my @erarecords = grep { $_->{ACTION} ne 'PATIENTPAYMENT' } @{ $args->{ERARECORDS} };
	# If we have erarecord.patientinsuranceid, use patientinsurance.insurancepackageid.
	my %patientinsuranceids = map { $_->{PATIENTINSURANCEID} => 1 } @erarecords;

	my $insurancepackageidssql = SQL::Select->new(
		)->Flags(
			{TEMPTABLEOPTIN => 1},
		)->Select(
			"id",
			"insurancepackageid",
		)->From(
			"patientinsurance",
		)->Where(
			["id in (??)", [keys %patientinsuranceids]],
		);

	my %insurancepackageids = $insurancepackageidssql->ColumnValues($dbh);

	for my $erarecord (@erarecords) {
		$erarecord->{AEGISINSURANCEPACKAGEID} = $insurancepackageids{ $erarecord->{PATIENTINSURANCEID} };
	}
	ProcessTable($dbh, {
		TABLENAME => 'ERARECORD',
		TABLEROWS => [grep { $_->{ACTION} ne 'PATIENTPAYMENT' } @{ $args->{ERARECORDS} }],
		OPERATION => 'Update',
		COLUMNNAMES => [qw(AEGISINSURANCEPACKAGEID)],
		USERNAME => $args->{USERNAME},
	});

	# Now, the erabatch.

	# Get majority-rule KRC from erarecord.kickreasoncategorid.
	my $majoritykickreasoncategoryid = SQLValues("select stats_mode(kickreasoncategoryid) from erarecord where erabatchid = ?", $dbh , $args->{ERABATCH}{ID});

	# Get majority-rule IRC from erarecord.aegisinsurancepackageid
	# OR from the majority-rule KRC (by matrix or name).
	my $majorityinsurancereportingcategoryid = SQLValues(q{
		select stats_mode(insurancepackage.insurancereportingcategoryid)
		from insurancepackage, erarecord
		where erarecord.aegisinsurancepackageid = insurancepackage.id
		and erarecord.erabatchid = ?
	}, $dbh, $args->{ERABATCH}{ID});
	if ($majorityinsurancereportingcategoryid eq '' && $majoritykickreasoncategoryid ne '') {
		$majorityinsurancereportingcategoryid = SQLValues(q{
			select
			case
				when (
					select	count(distinct insurancereportingcategoryid)
					from	kickreasonmatrix
					where	kickreasoncategoryid = ?
						and insurancereportingcategoryid is not null
						and deleted is null
				) = 1
				then (
					select	max(insurancereportingcategoryid)
					from	kickreasonmatrix
					where	kickreasoncategoryid = ?
						and insurancereportingcategoryid is not null
						and deleted is null
				)
				else null
			end
			from dual
		}, $dbh, ($majoritykickreasoncategoryid) x 2);
	}
	if ($majorityinsurancereportingcategoryid eq '' && $majoritykickreasoncategoryid ne '') {
		$majorityinsurancereportingcategoryid = SQLValues(q{
			select
				insurancereportingcategory.id
			from
				insurancereportingcategory,
				kickreasoncategory
			where
				insurancereportingcategory.name = kickreasoncategory.name
				and kickreasoncategory.id = ?
		}, $dbh, $majoritykickreasoncategoryid);
	}
	ProcessTable($dbh, {
		TABLENAME => 'ERABATCH',
		TABLEROWS => [{
			ID => $args->{ERABATCH}{ID},
			AEGISKICKREASONCATEGORYID => $majoritykickreasoncategoryid,
			AEGISINSREPORTINGCATEGORYID => $majorityinsurancereportingcategoryid,
		}],
		OPERATION => 'Update',
		COLUMNNAMES => [qw(AEGISKICKREASONCATEGORYID AEGISINSREPORTINGCATEGORYID)],
		USERNAME => $args->{USERNAME},
	}) if $majoritykickreasoncategoryid || $majorityinsurancereportingcategoryid;

	return;
}


#########################################################################################
# _DiscardRecordRequirePosterReview
#
# Description:
#	-Under certain conditions, we autodiscard and may autoapply some erarecords
#	 during matching.
#	-One of the conditions is usually that posterreviewflag is turned off for this
#	 eradiscardstatusreason.
# 	-A payor override might be:
#		for Medicare, if eradiscardstatusreason is BALANCEFORWARD,
#		then ignore posterreviewflag
#
# Parameters:
#	$dbh:  application database handle object
#	$args: hashref of arguments:
#		ERARECORD
#
# Return Value:
#	1 if the record may be discarded, otherwise undef
#########################################################################################
sub _DiscardRecordRequirePosterReview {
	my ($self, $dbh, $args) = @_;

	# Hydra 208151: Auto discard ERA Segment Parse unpostables for Remit Received batches
	my $erarecord = $args->{ERARECORD};
	my $isremitreceivedbatch = SQLValues("
		select
			count(*)
		from
			erarecord
		where
			erabatchid = ? and
			eradiscardstatusreasonid = 'EOBUNLISTED' and
			action = 'BATCHEXCEPTION'
	", $dbh, $erarecord->{ERABATCHID});

	if ($isremitreceivedbatch && $erarecord->{ERADISCARDSTATUSREASONID} eq 'ERAPARSEFAILURE' && $erarecord->{PAYMENT} != 0) {
		return;
	}
	elsif ($isremitreceivedbatch && !(InList($erarecord->{ERADISCARDSTATUSREASONID}, ('ERAPARSEFAILURE', 'EOBUNLISTED'))) && ($erarecord->{PAYMENT} != 0)) {
		#275024 - Auto-discard All Batch Exceptions in RAR Batches.
		return;
	}

	my $posterreviewflag = SQLValues(q{
		select posterreviewflag from eradiscardstatusreason where id = ?
	}, $dbh, $args->{ERARECORD}{ERADISCARDSTATUSREASONID});

	return $posterreviewflag eq 'Y' ? 1 : undef;
}


#########################################################################################
# UnMatchBatch
#
# Description:
# 	-Unmatches a PREMATCHED batch and puts it back into a CREATED status.
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCHID
#		USERNAME
#
# Return Value:
#	1 if all matches succeeded, otherwise 0
#########################################################################################
sub UnMatchBatch {
	my $self = shift;
	my @args = @_;
	DBTransaction sub { $self->_UnMatchBatch(@args); };		
}

sub _UnMatchBatch {
	my ($self, $dbh, $args) = @_;
	
	AssertRequiredFields($args,['ERABATCHID','USERNAME']);
	my @clearcols = qw(
	CHARGEID TRANSFERTYPE KICKREASONID BALANCETRANSACTIONTYPE PATIENTINSURANCEID
	BALANCETRANSACTIONREASON MANUALFLAG NOTE MATCHINGSCORE MATCHED MATCHEDBY
	);

	my %erabatch = SQLHash("select * from erabatch where id = ?",$dbh,$args->{ERABATCHID});
	Assert($erabatch{STATUS} eq 'PREMATCHED',"Can't unmatch a batch that is not in PREMATCHED status.");
	
	my $nullsql = join(",", map { "$_=null"} @clearcols);
	my $sql = "update erarecord set $nullsql,lastmodified=SYSDATE,lastmodifiedby=? where erabatchid=?";
	SQLDo($sql,$dbh,$args->{USERNAME},$args->{ERABATCHID});
	
	ProcessForm('ERABATCH',$dbh,$args->{USERNAME},{ operation=>'Update',ID=>$args->{ERABATCHID}, STATUS=>'CREATED' },['STATUS']);
}


#########################################################################################
# MatchPatientAccount
#
# Description:
#	Matches ERA PatientPayment records to the patient's account.  If the account is found, 
#	and if the lastname in teh erarecord matches somebody who would likely be paying, then 
#	the record is marked "matched".
#
# Parameters:
#	$dbh
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs
#		USERNAME
#
# Return Value:
#	$recordsmatched (boolean)
#########################################################################################
sub MatchPatientAccount {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args,['ERABATCH','ERARECORDS','USERNAME']);

	my $erabatch = $args->{ERABATCH};
	my @erarecords = @{$args->{ERARECORDS}};

	my $providergrouppermissions = GetTablespaceValueWithDefault($dbh, { KEY => 'Provider-Group-Based Data Permissions' }) eq 'ON';

	my $patientpaylog = Athena::Conf::AthenaNet::Rollout('colpci')->{COLPCI_5237_PATIENTPAYLOG};
	my $practiceid = Athena::Util::Database::SessionInfo($dbh)->{context};
	_Log("Batch: $erabatch->{ID}R$practiceid ProvierGroupPermission: $providergrouppermissions") if ($patientpaylog);
	_Log("Batch: $erabatch->{ID}R$practiceid BankaccountProvierGroupIds of batch are " . join(",", @{ $erabatch->{BANKACCOUNTPROVIDERGROUPIDS} || [] })) if ($patientpaylog);
	my $xstat = BusCall::PracticeStructure::InterProviderGroupPostingEnabled($dbh);

	# store a copy of the original record with each record, then write 
	# only CHANGED records to DB at the end (minor performance boost)
	map { $_->{originalrecord} = {%$_} } @erarecords;

	my $allmatch = 1;
	my $log = $ENV{ERAFILE_MATCHPATIENTLOGFILE} ? 1 : 0;
	my @matchlogs;

	# if unpostable routing is turned on and if the whole account is unmatched, then
	# any record which matches an unpostable routing rule should be auto-discarded.
	# some rules will check to see whether we've TRIEDMATCHING; here, we haven't.
	my $routed = 0;
	if (
		GetTablespaceValue("Unpostable Routing", $dbh) eq "ON"
	) {
		my $hashighmatchscorerecords = (grep { $_->{MATCHINGSCORE} >= $self->GetMinimumAllowedChargeMatchingScore($dbh) } @erarecords);
		foreach my $erarecord (grep { !$_->{MATCHED} } @erarecords) {
			if ($self->GetDefaultUnpostableRoute($dbh, {
				ERARECORD => $erarecord,
				CONTEXTID => GetPracticeID(),
				TRIEDMATCHING => undef,
				HIGHMATCHSCORERECORDS => $hashighmatchscorerecords,
			})) {
				$routed = 1 if $routed == 0;
				if ($erarecord->{MANUALFLAG} ne 'D') {
					$erarecord->{MANUALFLAG} = 'D';
					$erarecord->{ERADISCARDSTATUSREASONID} = 'ROUTETOOTHER';
				}
			}
		}
	}

	# If we didn't just now manage to route anything, try matching.
	# If nothing matches, try more aggressive routing.
	if (!$routed) {
		foreach my $erarecord (@erarecords) {

			# The providergroupid checks below are redundant and borderline legacy,
			# pre-8.0 behavior.
			# There may be things that rely on having erarecord.providergroupid set
			# instead of having to look it up from erarecord.patient.providergroupid.
			# So we continue to set it - but really $providergrouppermissions means that
			# the patientid tells us the providergroup so a record with patientid is
			# fully matched.
			my $matched = $erarecord->{MATCHED};
			$matched &&= ($erarecord->{PROVIDERGROUPID} ne '') if $providergrouppermissions;

			# If the record is already matched, we may still want to run matching again:
			# if this is an XStat practice and we have bank information, let's
			# check again to make sure that the erarecord controlnumber didn't find us a
			# patientid for a providergroup that is inappropriate for the batch bank
			# account.
			next unless !$matched || $xstat && @{ $erabatch->{BANKACCOUNTPROVIDERGROUPIDS} || [] };

			my %patient = $self->FindPatientForRecord($dbh, {
				ERARECORD => $erarecord,
				BANKACCOUNTPROVIDERGROUPIDS => $erabatch->{BANKACCOUNTPROVIDERGROUPIDS},
			});
			if ($log) {
				my %matchlog = ERA::QA::GenerateLogMatchPatientAccountForRecord($dbh, {
					PATIENT => \%patient,
					ERARECORD => $erarecord,
					ERAFILEID => $erabatch->{RPOERAFILEID},
				});
				push @matchlogs, \%matchlog;
			}
			$matched = $patient{ID} ? 1 : 0;
			if ($providergrouppermissions) {
				$matched &&= $patient{PROVIDERGROUPID} ne '';
			}

			if ($matched) {
				# Update matched/by only if the erarecord wasn't matched before
				# or it was matched to something else,
				if (!$erarecord->{MATCHED} || $erarecord->{PATIENTID} != $patient{ID}) {
					$erarecord->{PATIENTID} = $patient{ID};
					if ($providergrouppermissions) {
						$erarecord->{PROVIDERGROUPID} = $patient{PROVIDERGROUPID};
					}
					$erarecord->{MATCHED} = 'SYSDATE';
					$erarecord->{MATCHEDBY} = $args->{USERNAME};
				}
			}
			else {
				$erarecord->{MATCHED} = '';
				$erarecord->{MATCHEDBY} = '';
				$allmatch = 0;
			}
		}
		ERA::QA::LogMatchPatientAccount(@matchlogs) if $log;

		# if unpostable routing is turned on and if the whole account is unmatched, then
		# any record which matches an unpostable routing rule should be auto-discarded.
		if (
			GetTablespaceValue("Unpostable Routing",$dbh) eq "ON"
		) {
			my $hashighmatchscorerecords = (grep { $_->{MATCHINGSCORE} >= $self->GetMinimumAllowedChargeMatchingScore($dbh) } @erarecords);
			foreach my $erarecord (grep { !$_->{MATCHED} } @erarecords) {
				if (
					$erarecord->{MANUALFLAG} ne 'D'
					&& $self->GetDefaultUnpostableRoute($dbh, {
						ERARECORD => $erarecord,
						CONTEXTID => GetPracticeID(),
						TRIEDMATCHING => 1,
						HIGHMATCHSCORERECORDS => $hashighmatchscorerecords,
					})
				) {
					$erarecord->{MANUALFLAG} = 'D';
					$erarecord->{ERADISCARDSTATUSREASONID} = 'ROUTETOOTHER';
				}
			}
		}
	}

	# match to kickreasons
	foreach my $erarecord (@erarecords) {
		my $kicks = $erarecord->{KICKS} || [];
		foreach my $erakick (@$kicks) {
			$self->MatchRecordToKick($dbh,{
				ERARECORD => $erarecord,
				ERAKICK   => $erakick,
				USERNAME  => $args->{USERNAME},
			});
		}
	}

	foreach my $erarecord (@erarecords) {
		# store match info in database
		my @sqlcols = qw(MATCHED MATCHEDBY PROVIDERGROUPID PATIENTID);

		# Hydra 236862: If PAIDPRACTICE if +ve and FEE is 0 and there's no payment mark the record as manually posted even if it is not matched.
		if ($erarecord->{MANUALFLAG} eq "D") {
			# Discard record if it hasn't been already.
			my $discard = $erarecord->{originalrecord}->{MANUALFLAG} ne "D";
			# Any discarded record of a type where poster review is not required,
			# set it applied and create an unpostable now, if we haven't yet
			# (but only if this erabatch already has an associated payment batch).
			my $apply =
				!$erarecord->{APPLIED}
				&& SQLValues("select id from paymentbatch where erabatchid = ? ", $dbh, $erarecord->{ERABATCHID})
				&& !$self->_DiscardRecordRequirePosterReview($dbh, {ERARECORD => $erarecord});
			if ($apply) {
				$erarecord->{APPLIED} = 'SYSDATE';
				$erarecord->{APPLIEDBY} = 'ATHENA';
				push @sqlcols, qw(APPLIED APPLIEDBY);
			}
			if ($discard || $apply) {
				my $contextid = GetPracticeID();
				$self->DiscardRecord($dbh, {
					ERARECORD => $erarecord,
					USERNAME => $args->{USERNAME},
					CONTEXTID => $contextid,
					GENERATESTATUS => $discard,
				});
			}
		} elsif (($erarecord->{PAIDPRACTICE} != 0) && ($erarecord->{FEE} == 0) && ($erarecord->{PAYMENT} == 0)
			&& SQLValues("select id from paymentbatch where erabatchid = ? ", $dbh, $erarecord->{ERABATCHID})) {
			$erarecord->{MANUALFLAG} = 'Y';
			$erarecord->{APPLIED} = 'SYSDATE';
			$erarecord->{APPLIEDBY} = 'ATHENA';
			push @sqlcols, qw(APPLIED APPLIEDBY MANUALFLAG);
		}

		# check if record has changed at all
		if (grep {$erarecord->{$_} ne $erarecord->{originalrecord}{$_}} @sqlcols) {
			# if any column has changed its value, update the row in the DB
			$erarecord->{operation} = 'Update';
			ProcessForm('ERARECORD',$dbh,$args->{USERNAME},$erarecord,\@sqlcols);
		}
	}

	return $allmatch;
}


#########################################################################################
# FindPatientForRecord
#
# Description:
#	Given an erarecord, looks for a matching patient.  It requires an id and
#	a name that is at least related to that id.  It may end up with a
#	different id (depending on merging and providergroups), but first it
#	needs to identify the human.
#
# Parameters:
#	$dbh
#		ERARECORD	hashref
#		BANKACCOUNTPROVIDERGROUPIDS arrayref
#
# Return Value:
#	hash of patient data
#########################################################################################
sub FindPatientForRecord {

	my ($self, $dbh, $args) = @_;

	AssertFields($args, [qw(ERARECORD)], [qw(BANKACCOUNTPROVIDERGROUPIDS)]);
	my $erarecord = $args->{ERARECORD};
	my @bankaccountprovidergroupids = @{ $args->{BANKACCOUNTPROVIDERGROUPIDS} || [] };
	my $patientpaylog = Athena::Conf::AthenaNet::Rollout('colpci')->{COLPCI_5237_PATIENTPAYLOG};
	my $practiceid = Athena::Util::Database::SessionInfo($dbh)->{context};

	_Log("Batch: $erarecord->{ERABATCHID}R" . $practiceid . " For the record $erarecord->{ID}, Patientid and providergroupid post import
		is $erarecord->{PATIENTID} and $erarecord->{PROVIDERGROUPID}") if ($patientpaylog);
	# Query has to take into account case where this patient has been merged to another
	# patient has been merged to another patient that does not have status deleted.
	my $patientsql = SQL::Select->new(
	)->Select(
		'client.*',
	)->From(
		'client',
	)->Where(
		'client.newpatientid is null',
	)->StartWith(
		['client.id = ?', $erarecord->{PATIENTID}],
		[q{
			soundex(client.LASTNAME) = soundex(?)
			or soundex(client.RPLASTNAME) = soundex(?)
			or soundex(client.PREVIOUSLASTNAME) = soundex(?)
		}, ($erarecord->{PATIENTLASTNAME}) x 3],
	)->ConnectBy(
		'prior client.newpatientid = client.id',
	);
	# Hydra 609871: Check for restricted providergroup
	if(GetTablespaceValueWithDefault($dbh, { KEY => 'Provider-Group-Based Data Permissions' }) eq 'ON') {
		$patientsql->From(
			'providergroup',
		)->Joins(
			'client.providergroupid = providergroup.id',
		)->Where(
			"nvl(providergroup.restrictedyn,'N') != 'Y'",
		);
	}
	my %patient = $patientsql->Hash($dbh);
	_Log("Batch: $erarecord->{ERABATCHID}R$practiceid After new patient query: Patientid, Enterprise and Proviergroup details of patientcontrolnumber $erarecord->{PATIENTCONTROLNUMBER}
		in erarecord $erarecord->{ID} are $patient{ID}, $patient{ENTERPRISEID} and $patient{PROVIDERGROUPID}") if ($patientpaylog);

	# That query allowed a deleted patient only for logging purposes -
	# but don't actually return a deleted patient.
	if ($patient{STATUS} eq 'd') {
		%patient = ( LOGFINALMERGEDPATIENTDELETED => 1 );
	}

	# If enterprise and the batch bank account is restricted to certain providergroups,
	# then make sure our patient is from one of them.
	if (
		@bankaccountprovidergroupids
		&& GetTablespaceValueWithDefault($dbh, { KEY => 'Provider-Group-Based Data Permissions' }) eq 'ON'
		&& !InList($patient{PROVIDERGROUPID}, @bankaccountprovidergroupids)
	) {
		my $logbadprovidergroupid = $patient{PROVIDERGROUPID};
		%patient = SQL::Select->new()->Select(
			'client.*',
		)->From(
			'client',
			'providergroup',
		)->Flags({
			TEMPTABLEOPTIN => 1,
		})->Joins(
			'client.providergroupid = providergroup.id',
		)->Where(
			["enterpriseid = ?", $patient{ENTERPRISEID}],
			"nvl(providergroup.restrictedyn,'N') != 'Y'",
			["providergroupid in (??)", \@bankaccountprovidergroupids],
		)->OrderBy(
			# If there are multiple results, prefer a client who should be paying.
			"outstandingp desc",
		)->Hash($dbh);
		$patient{LOGBADPROVIDERGROUPID} = $logbadprovidergroupid;
		_Log("Batch: $erarecord->{ERABATCHID}R$practiceid Post Enterprise Query: Patientid, Enterprise and Proviergroup details of patient $erarecord->{PATIENTCONTROLNUMBER}
			in erarecord $erarecord->{ID} are $patient{ID}, $patient{ENTERPRISEID} and $patient{PROVIDERGROUPID}") if ($patientpaylog);
	}

	return %patient;
}


#########################################################################################
# MatchBatchException
#
# Description:
#	See if there are any batch exceptions that can be discarded without poster review
#
# Parameters:
#	$dbh
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs
#		USERNAME
#
# Return Value:
#	$recordsmatched (boolean)
#########################################################################################
sub MatchBatchException {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args,['ERABATCH','ERARECORDS','USERNAME']);

	my $erabatch = $args->{ERABATCH};
	my $kickreasoncategoryid = $args->{ERABATCH}->{KICKREASONCATEGORYID};
	my @erarecords = @{$args->{ERARECORDS}};
	my $contextid = GetPracticeID();

	# Hydra 238558 : Auto-discard MU payments from all Medicaids as incentive payments
	my $ismedicaidpayor = $self->GetShortName() =~ /Medicaid/ && $self->GetClaimAction() eq 'ERA';

	if ($ismedicaidpayor) {
		$self->AutoDiscardIncentivePaymentsForMedicaids($dbh,$args);
	}

	# Hydra 369324,413703 : Client 6385,3908, 7598, 1654, 7654, 7661, 8209, 8363, 2144 Patient Statement Review discard rule
	if (InList($contextid,(6385,3908,7598,1654,7654,7661,8209,8363,2144))) { ##no critic (ProhibitMagicNumbers)
		$self->DiscardAsPatientStatementReview($dbh, $args);
	}

	my $allmatch = 1;

	my $paymentbatchid = SQLValues("select id from paymentbatch where erabatchid = ? ", $dbh, $erabatch->{ID});

	foreach my $erarecord (@erarecords) {
		# Validate the claim ID
		my $isvalidclaimid = SQLValues("
			select
				count(*)
			from
				claim,
				tcharge
			where
				claim.id = ?
				and tcharge.claimid=claim.id
		", $dbh, $erarecord->{CLAIMID});

		unless ($isvalidclaimid) {
			$erarecord->{CLAIMID} = '';
			$self->EngineProcessTable($dbh, {
				TABLENAME	=> 'ERARECORD',
				USERNAME	=> $args->{USERNAME},
				OPERATION	=> 'Update',
				TABLEROWS	=> [$erarecord],
				COLUMNNAMES	=> ['CLAIMID']
			});
		}

		# Automatically discard any batchexceptions that do not require poster review
		if (($erarecord->{MANUALFLAG} eq "D" || $erarecord->{SKIP})
			&& ($erarecord->{APPLIED} eq '')
			&& $erarecord->{ERADISCARDSTATUSREASONID}
			&& $paymentbatchid
		) {
			# If the record satisfies any era discard rules, set ruleid
			# and payorcontrolnumberstripped for unpostable based upon
			# the rule.
			my $unpostabletype;
			my $requireposterreview;
			unless ($erarecord->{SKIP} || $erarecord->{ERADISCARDRULEID}) {
				$requireposterreview = $self->_DiscardRecordRequirePosterReview($dbh, {ERARECORD => $erarecord});
				my $eradiscardrules = $self->GetERADiscardRules($dbh, { KICKREASONCATEGORYID => $kickreasoncategoryid });
				($erarecord->{ERADISCARDRULEID}, $erarecord->{PAYORCONTROLNUMBERSTRIPPED}, $unpostabletype) = $self->GetERADiscardRuleIDAndPCNStripped($dbh, { RULES => $eradiscardrules, ERARECORD => $erarecord});
                                $erarecord->{PAYORCONTROLNUMBERSTRIPPED} = '' if ($erarecord->{PAYORCONTROLNUMBERSTRIPPED} eq '1');
			}

			#302989 - Remove interest from the parser : Discard CHKEOBDISCREP and Interest as Unpostable without poster review when it has note like AMT*I
			#		and also mark L6 records as manually posted when they are returned with interest records.
			$erarecord->{SKIP} =1 if($erarecord->{PLBREASONCODE} eq 'L6' && !($erarecord->{PAYMENT}) && $erarecord->{ORIGINALPAYMENT});

			if( ($erarecord->{ERADISCARDSTATUSREASONID} eq 'INTEREST' || $erarecord->{ERADISCARDSTATUSREASONID} eq 'CHKEOBDISCREP')
				&& ($erarecord->{NOTE} =~ /AMT\~I/)){

				$erarecord->{APPLIED} = 'SYSDATE';
				$erarecord->{APPLIEDBY} = 'ATHENA';
				$self->DiscardRecord($dbh, {
					ERARECORD => $erarecord,
					USERNAME => $args->{USERNAME},
					CONTEXTID => $contextid,
					GENERATESTATUS => 1,
				});
				my $unpostableid = SQLValues("select id from unpostable where erarecordid = ?",$dbh,$erarecord->{ID});
				if($unpostableid){
					Unpostable::AddNote($dbh, {
						UNPOSTABLEID => $unpostableid,
						NOTE         => $erarecord->{NOTE},
						USERNAME     => 'ATHENA',
					});
				}
				$erarecord->{NOTE} = '';
				$self->EngineProcessTable($dbh, {
					TABLENAME	=> 'ERARECORD',
					USERNAME	=> $args->{USERNAME},
					OPERATION	=> 'Update',
					TABLEROWS	=> [$erarecord], 
					COLUMNNAMES	=> ['NOTE']
				});
			}

			my $merged;

			if ((!$requireposterreview) || $erarecord->{ERADISCARDRULEID} || $erarecord->{SKIP}) {

				$erarecord->{APPLIED} = 'SYSDATE';
				$erarecord->{APPLIEDBY} = 'ATHENA';

				$unpostabletype ||= $erarecord->{ERADISCARDSTATUSREASONID};
				if ($paymentbatchid && ($unpostabletype eq 'READJNOTICE')) {
					$merged = $self->MergeReadjudicationNoticeUnpostables($dbh, { ERARECORD => $erarecord, PAYMENTBATCHID => $paymentbatchid, USERNAME => $args->{USERNAME}});
				}
				# If the record has the key SKIP set then its automatcially marked as 'manally posted'
				$self->DiscardRecord($dbh, {
					ERARECORD => $erarecord,
					USERNAME => $args->{USERNAME},
					CONTEXTID => $contextid,
					GENERATESTATUS => 1,
				});
			}

			$self->HandleClaimAssociationAndUnpostableNote($dbh, {
					RULEID => $erarecord->{ERADISCARDRULEID},
					ERARECORD => $erarecord,
					CONTEXTID => $contextid,
					PAYMENTBATCHID => $paymentbatchid,
					UNPOSTABLETYPEID => $unpostabletype,
					MERGED => $merged,
					USERNAME => $args->{USERNAME}
			}) if ($erarecord->{ERADISCARDRULEID});

		} else {
			$allmatch = 0;
		}
	}

	return $allmatch;
}

#########################################################################################
# MergeReadjudicationNoticeUnpostables
#
# Description:
#	We override this function to payer specific if:
#	- Payer sends multiple READJNOTICEs for the same claim within a batch; 1 per cpt
#	  This causes a problem trying to match the subseqent PTs.
#	- Aggregate them into a single READJNOTICE instead of creating multiples
#
# Parameters:
#	$dbh
#		ERARECORD - Hashref of ERA record info
#		PAYMENTBATCHID - PaymentBatch ID
#		USERNAME
#
# Return Value:
#	none
#########################################################################################
sub MergeReadjudicationNoticeUnpostables
{
	my ($self, $dbh, $args) = @_;

	return;
}

#########################################################################################
# HandleClaimAssociationAndUnpostableNote
#
# Description:
# 	- This function calls a bunch of function which takes care of 
#	  claimassociation part and trigger the unpostable notes based
#	  upon the claim association
#
# Parameters:
#	$dbh
#		RULEID		ERA discard Rule ID
#		PAYORCONTROLNUMBERSTRIPPED	payercontrolnumber stripped
#		ERARECORD	hashref of batchexception record
#		USERNAME
#
# Return Value:
#	none
#########################################################################################
sub HandleClaimAssociationAndUnpostableNote
{
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args, [qw( RULEID ERARECORD PAYMENTBATCHID USERNAME )]);

	my $ruleid = $args->{RULEID};

	my $erarecord = $args->{ERARECORD};
	my $contextid = $args->{CONTEXTID};
	my $paymentbatchid = $args->{PAYMENTBATCHID};
	my $unpostabletypeid = $args->{UNPOSTABLETYPEID};
	my $merged = $args->{MERGED};

	my $rv = $self->SetControlNumberForBatchExceptionRecord($dbh, {
		RULEID => $ruleid,
		ERARECORD => $erarecord,
		USERNAME => $args->{USERNAME},
		CONTEXTID => $contextid,
	});

	# If the erarecord is merged and marked as manually posted, dont add claimnote
	return if $merged;

	# Update unpostable fields when associate claim is identified
	# Fields to be updated are:
	# claimid, providerid, providergroupid and medicalgroupid
	if ($rv->{CLAIMID} && $rv->{MATCHED}) {
		$self->UpdateUnpostableFieldsUsingClaimID($dbh, {
			USERNAME => $args->{USERNAME},
			CLAIMID => $rv->{CLAIMID},
			ERARECORDID => $erarecord->{ID},
			CONTEXTID => $contextid
		});
	}
	# Update the notes for unpostable(and claimid if identified)
	# Apply unpostable/claim note for either associated record found and matched else no associated record found
	# We dont want to deal any notes part for unpostables where there is associated claims and its not matched
	my $unpostableid = $self->AddClaimAndUnpostableNotesUsingRuleID($dbh, {
		RULEID => $ruleid,
		CLAIMID => $rv->{CLAIMID},
		USERNAME => $args->{USERNAME},
		CONTROLNUMBER => $rv->{CONTROLNUMBER},
		TRANSFERTYPE => $rv->{TRANSFERTYPE},
		ERARECORD => $erarecord,
		MATCHED => $rv->{MATCHED},
		DISCARDED => $rv->{DISCARDED},
		ASSOCIATEDRECORDID => $rv->{ERARECORDID},
		UNPOSTABLETYPEID => $unpostabletypeid,
	});
	# The unpostable created will be in the status INPROCESS, we need to move it to PEND by reapplying event
	# but... make sure we leave the unpostables whose associated records are not matched. We will be firing the
	# events and notes only the records are matched.
	if ($unpostableid) {
		my $unpostableeventid = SQL::Select->new()->Select(
			"eradiscardruleview.unpostableeventid",
		)->From(
			"eradiscardruleview",
		)->Where(
			["eradiscardruleview.id = ?", $ruleid],
			"eradiscardruleview.deleted is null",
		)->Values($dbh);

		Unpostable::Update($dbh, {
			ID => $unpostableid,
			PAYMENTBATCHID => $paymentbatchid,
			POSTPAYMENTBATCHID => $paymentbatchid,
			CONTEXTID => $contextid,
			POSTDATE => AthenaToday(),
			USERNAME => $args->{USERNAME},
			UNPOSTABLEEVENTID => $unpostableeventid,
		});
	}
}

#########################################################################################
# MatchRefundRequests
#
# Create REFUNDREQUESTLINE records associated with a Refund Request Letter unpostable (REFUND).
#
# Parameteres:
#	$dbh
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs. All ERARECORDs passed should share a PARENTERARECORDID
#		USERNAME
#
# Return Value:
#	$recordsmatched (boolean)
#########################################################################################
sub MatchRefundRequests {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args,['ERABATCH','ERARECORDS','USERNAME']);

	my @erarecords = @{$args->{ERARECORDS}};

	# We cannot create the REFUNDREQUEST lines until the payment batch for the erabatch has been created.
	my $paymentbatchid = SQLValues("select id from paymentbatch where erabatchid = ?", $dbh, $args->{ERABATCH}->{ID});
	return 0 unless $paymentbatchid;
	
	# All the provided ERA records should have the same PARENTERABATCHID, but we validate that here.
	my $parenterarecordid = $erarecords[0]{PARENTERARECORDID};
	Assert(scalar(grep {$_->{PARENTERARECORDID} != $parenterarecordid} @erarecords) == 0,
		"All ERA Records passed to MatchRefundRequests must share a PARENTERARECORDID.");
	
	# The REFUND unpostable must already exist.
	my $refundunpostableid = SQL::Select->new(
	)->Select(
		'id'
	)->From(
		'unpostable',
	)->Where(
		'voided is null',
		"unpostabletypeid = 'REFUND'",
		['erarecordid = ?', $parenterarecordid],
	)->Values($dbh);
	return 0 unless $refundunpostableid;

	my $allmatch = 1;		
	my @refundrequestlines;
	my @records;
	foreach my $erarecord (@erarecords) {
		if ($erarecord->{MANUALFLAG} eq 'D'
			&& $erarecord->{APPLIED} eq ''
			&& $erarecord->{ACTION} eq 'REFUNDREQUESTLINE'
		) {
			# Add to the array for a later process table.
			my %refundrequest;
			foreach my $field (qw(PAYORCONTROLNUMBER PAYORCONTROLNUMBERSTRIPPED PATIENTLASTNAME PATIENTFIRSTNAME PATIENTINSURANCEIDNUMBER FROMDATE TODATE AMOUNT IMAGEFILEPAGENUMBER)) {
				$refundrequest{$field} = $erarecord->{$field};
			} 
			$refundrequest{UNPOSTABLEID} = $refundunpostableid;
			$refundrequest{ERARECORDID} = $erarecord->{ID};
			$refundrequest{PROVIDERNUMBER} = $erarecord->{PAYORPROVIDERNUMBER};
			push @refundrequestlines, \%refundrequest;

			# ... and mark the ERARECORD as applied.
			push @records, $erarecord->{ID};
		}
		else {
			$allmatch = 0;
		}
	}
		
	$self->EngineProcessTable($dbh, {
		TABLENAME	=> 'ERARECORD',
		USERNAME	=> $args->{USERNAME},
		OPERATION	=> 'Update',
		TABLEROWS	=> [map{ {
			ID => $_,
			APPLIED => 'SYSDATE',
			APPLIEDBY => 'ATHENA'
		}}@records],
		COLUMNNAMES	=> ['APPLIED','APPLIEDBY'],
		SYSDATEHACK	=> 1
	});
	# Actually add the refund request lines to the REFUNDREQUESTLINE table.
	ProcessTable($dbh, {
		USERNAME => $args->{USERNAME},
		TABLENAME => 'REFUNDREQUESTLINE',
		OPERATION => 'Add',
		TABLEROWS => \@refundrequestlines,
	}) if scalar(@refundrequestlines);
			
	return $allmatch;
}

#########################################################################################
# MatchClaim
#
# Description:
# 	-Matches the erarecords to chargeids for an entire erabatch
#	-calls through to MatchClaim to do matching work
#	-sets all of the erarecord.chargeid's in the database
#
# Parameters:
#	$dbh
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs
#		USERNAME
#
# Return Value:
#	$recordsmatched (boolean)
#########################################################################################
sub MatchClaim {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args,['ERABATCH','ERARECORDS','USERNAME']);

	my $erabatch = $args->{ERABATCH};
	my @erarecords = @{$args->{ERARECORDS}};
	# store a copy of the original record with each record, then write 
	# only CHANGED records to DB at the end (minor performance boost)
	map { $_->{originalrecord} = {%$_} } @erarecords;

	my $paymentbatchid = SQLValues("select id from paymentbatch where erabatchid = ?", $dbh, $erabatch->{ID});

	# flag to indicate if all records matched ok (on chargeid AND payment amount)
	my $allmatchpayment = 1;

	# we'll use this to compare against remits' dates of service,
	# to see if we care when they don't match at all
	my $earliestdos = SQLValues("select to_char(golivedate, 'MM/DD/YYYY') from context where id = ?", $dbh, GetPracticeID());

	#h1050573 - Identify Medicare Crossover from a Wrong Payer,
	#and award full matching points for PAYOR
	$self->SetFlagForMedicareCrossoverRecords($dbh, $args);

	# COLPMI-181 - Identify secondary dual eligible records for matching it agianst primary insurance
	#and award full matching points for TRANSFERTYPE
	$self->SetFlagForDualEligibleSecondaryRecords($dbh, $args);
	
	# if unpostable routing is turned on and if the whole account is unmatched, then
	# any record which matches an unpostable routing rule should be auto-discarded,
	# without attempting to match.
	# some rules will check to see whether we've TRIEDMATCHING; here, we haven't.
	my $routed = 0;

	if (
		GetTablespaceValue("Unpostable Routing", $dbh) eq "ON"
	) {
		my $hashighmatchscorerecords = (grep { $_->{MATCHINGSCORE} >= $self->GetMinimumAllowedChargeMatchingScore($dbh) } @erarecords);
		foreach my $erarecord (grep { !$_->{MATCHED} } @erarecords) {
			my $routeid = $self->GetDefaultUnpostableRoute($dbh, {
				ERARECORD => $erarecord,
				CONTEXTID => GetPracticeID(),
				TRIEDMATCHING => undef,
				HIGHMATCHSCORERECORDS => $hashighmatchscorerecords,
			});

			if ($routeid) {
				$routed = 1;

				if ($erarecord->{MANUALFLAG} ne 'D') {
					$erarecord->{MANUALFLAG} = 'D';
					$erarecord->{ERADISCARDSTATUSREASONID} = 'ROUTETOOTHER';
					$erarecord->{UNPOSTABLEROUTEID} = $routeid;
				}
			}
		}

		if ($erarecords[0]->{CONTROLNUMBER} !~ /^\d+V\d+$/) {
			# if all of the records have a FROMDATE earlier than the
			# go live date, discard them as ROUTETOOTHER
			if (!$routed && $earliestdos
				&& !(grep { $_->{MATCHED} || !$_->{FROMDATE} || AthenaDate::DeltaDays($_->{FROMDATE}, $earliestdos) >= 0 } @erarecords)) {

				$routed = 1;

				foreach my $erarecord (@erarecords) {
					if ($erarecord->{MANUALFLAG} ne "D") {
						$erarecord->{MANUALFLAG} = 'D';
						$erarecord->{ERADISCARDSTATUSREASONID} = 'ROUTETOOTHER';
					}
				}
			}
		}

		#660845 - [Minute Clinic] Poly Posting - Auto Discard Records as IPT Unpostables
		my $practiceid = GetPracticeID($dbh);
		my $isminuteclinicpractice = BusCall::PracticeStructure::IsDescendantOf($dbh, {
			CONTEXTID => $practiceid,
			ANCESTORS => [1677],
		});
		if( $isminuteclinicpractice && $erarecords[0]->{CONTROLNUMBER} =~ /^(\d+)V(\d+)$/i  && !$routed)  {
			if($2 != $practiceid)	{
				$routed = 1;

				foreach my $erarecord (@erarecords) {
					if ($erarecord->{MANUALFLAG} ne "D") {
						$erarecord->{MANUALFLAG} = 'D';
						$erarecord->{ERADISCARDSTATUSREASONID} = 'WRONGPAYTO';
					}
				}
			}
		}

		my $wrongpayto_toggle_on = Athena::RolloutToggle::GetEnabledVersion($dbh, {
				KEY => 'CPDC_5410_WRONGPAYTO_BACKLOG_REDUCTION',
			}) eq 'ON';

		if($wrongpayto_toggle_on){
			try {
				my @contexttocheck = $practiceid;
				if($erarecords[0]->{CONTROLNUMBER} =~ /^(\d+)V(\d+)$/i  && !$routed){
					my ($controlclaimid,$controlcontextid) = $erarecords[0]->{CONTROLNUMBER} =~ /^(\d+)V(\d+)$/;
					push @contexttocheck, $controlcontextid;

					my $ispolypostpractice = InternalWebService($dbh, {
						SUB => "BusCall::Remittance::IsPolyPostingConfigPresent",
						ARGS => {
							CONTEXTIDS => \@contexttocheck
						},
						PRACTICEID => 1
					});

					if(!($ispolypostpractice || ($practiceid  =~ $controlcontextid || $controlcontextid  =~ $practiceid ) || 
						$self->_IsDigitSequeceMatch({PRACTICEID => $practiceid, ERACONTEXTID => $controlcontextid})) )	{
						$routed = 1;

						foreach my $erarecord (@erarecords) {
							if ($erarecord->{MANUALFLAG} ne "D") {
								$erarecord->{MANUALFLAG} = 'D';
								$erarecord->{ERADISCARDSTATUSREASONID} = 'WRONGPAYTO';
								$erarecord->{NOTE} .= " [WPT_DISCARD]";
							}
						}
					}
				}
			} 
			catch {
				AthenaScribe->new(
					category => 'preprocesserabatches',
					logidentifier => 'preprocess_erabatches_wrong_pay_to',
				)->log(
					message => "Error while discarding using wrong pay to: " . $_,
					level => 'error',
				);
			};
		}
	}

	# If we didn't just now manage to route anything, try matching.
	# If nothing matches, try more aggressive routing.
	if (!$routed) {

		# If it is ever the case that we bill two identical (or at least very
		# nearly identical) looking charges, we may get two remittance lines
		# with identical charge-identifying information.  Now, if the logic
		# we use for matching is nothing more than "for each remittance line,
		# find one (and only one) matching charge"... then this case would get
		# messed up... as both remittance lines would match both charges (and
		# thus neither remittance line would match only one charge).  Anyway,
		# the logic is then, instead, for every identical-looking (as far as
		# their charge-identifying-info fields) set of remittance records, 
		# find as many matching charges (exactly) as their are remittance
		# lines in the set.
		#
		# So, first, group together the erarecords by their charge-identifying
		# fields... then do all of the charge-matching operations on these
		# sets of erarecords (not "give me a charge for this record", but
		# rather "give me a set of charges for this set of erarecords"... of
		# course, typicaly these will be sets of only one erarecord and one
		# charge... but this way the same logic fits seemlesly around both
		# cases.

		my @recordgroups = GroupBy(['AMOUNT', @ERA::RecordMatchingCols, 'MATCHTOUNSPEC'] ,@erarecords);

		# Hydra 106261: ERA Matching Improvements - Multiple SVCs in ERA 1 Charge Line in Athenanet
		# Matching columns for multiple records when charged for a single charge
		# The other columns in RecordMatchingCols are not in tcharge.
		# Hydra 178903 : Remove procedurecode from matchingcols.
		# Sometimes the procedure code in ERA is the revenuecode instead of the actual procedurecode on charge.
		# Lets go ahead with many-many matching when procedurecode differs in charges.
		my @matchingcols = ('CLAIMID', 'AMOUNT', 'FROMDATE', 'TODATE', 'DAYS');

		foreach my $group (@recordgroups) {
			next unless @$group > 1;

			# ABP matching later may see these records one at a time, but will want to
			# know whether there were originally multiple records with these matching
			# criteria.
			# If it does not see this tag, it may choose treat them as claim-level remit.
			$_->{TRIEDMATCHINGINAGROUP} = 1 for @$group;

			# we only need bother looking if any of these records is not matched
			if (grep {$_->{MATCHED} eq ''} @$group) {
			
				# find the matching charge(s)
				my @charges = $self->MatchRecordsToCharges($dbh,{
					ERABATCH => $erabatch,
					ERARECORDS => $group,
				});
				# Null out charges, if the charges returned as unmatched with erarecords
				undef @charges if ($charges[0] && $charges[0]->{UNMATCH});
				
				# if there's a charge for every remit, then this was a successful match.
				# (comparing the list LENGTHS here, not the lists, themselves)
				if (scalar(@charges) == scalar(@$group)) {
								
					my $mismatch;
					foreach my $col(@matchingcols) {
						my $colpopulated = scalar(grep {defined $_->{$col}} @$group);
						next unless $colpopulated;
						my @count = UniqueElements(map {$_->{$col}} @charges);
						if (scalar(@count) > 1) {
							$mismatch = 1;
							last;
						}
					}
					last if $mismatch;

					# sort the charges by unpaid-amount and the remits by payment-amount,
					# so that the biggest payment ends up on the charge with the most
					# unpaid, and on down.
					# also order by id desc, in case any outstandings or payments are the
					# same.  "desc" is to pay off the first charges of ties first.
					@charges = SortBy([['OUTSTANDING','#'], ['ID','#','-']], @charges);
					@$group = SortBy([['PAYMENT','#'], ['ID','#','-']], @$group);

					# mark the chargeids/transfertypes/claimids that these remits
					# match to in the erarecords (and the overpaid flag)
					for (my $i=0; $i<@charges; $i++) {
						# similarly mark any sub-records as matched to the same charge as
						# the parent record
						foreach my $erarecord ($group->[$i],@{$group->[$i]->{subrecords}}) {
							@{$erarecord}{       qw(CLAIMID CHARGEID       TRANSFERTYPE PATIENTINSURANCEID ACTION MATCHINGSCORE     MATCHED   MATCHEDBY)} =
								(@{$charges[$i]}{qw(CLAIMID PARENTCHARGEID TRANSFERTYPE PATIENTINSURANCEID ACTION MATCHINGSCORE)}, 'SYSDATE', $args->{USERNAME});
						}
					}
				}
			}
		}

		# somewhat of an ungraceful hack... since sometimes we really want to match
		# many remits to one charge, we run one-to-one matching after trying
		# many-to-many matching.  This will at least gracefully handle the case
		# of many-to-one... it does not handle the case of many-to-less (but still many)
		# (It handles many-to-one matching, because each one-to-one match is ignorant
		# of any other matches to that same charge)

		# Hydra 130380: We record matching score only to the matched erarecords.
		# We should start recording Highest Matching score for all unmatched erarecords
		# and set a threshold limit to auto discard erarecords

		foreach my $erarecord (@erarecords) {
			# we only need bother looking if this record is not matched
			if ($erarecord->{MATCHED} eq '') {
			
				# find the matching charge(s)
				my @charges = $self->MatchRecordsToCharges($dbh,{
					ERABATCH => $erabatch,
					ERARECORDS => [$erarecord]
				});

				# Record the score as -1 if we dont find any potential matching charges for that erarecord
				if ($charges[0] && $charges[0]->{UNMATCH}) {
					$erarecord->{MATCHINGSCORE} = $charges[0]->{MATCHINGSCORE};
					$erarecord->{NOTE} = $charges[0]->{DESCRIPTION};
					warn sprintf "[PROOF:STD:UNMATCH] erarecord=%s reason=\"%s\" score=%s proc=%s amt=%s from=%s to=%s\n",
    		    ($erarecord->{ID}//''), ($charges[0]{DESCRIPTION}//''), ($charges[0]{MATCHINGSCORE}//''),
		        ($erarecord->{PROCEDURECODE}//''), ($erarecord->{AMOUNT}//''), ($erarecord->{FROMDATE}//''), ($erarecord->{TODATE}//'');
					# By now we have got the score/description what we are interested for..
					# clear the charges, else it may affect other operations
					undef @charges;
				}

				# if there's one charge for the one remit, then this was a successful match.
				if (@charges == 1) {
					warn sprintf "[PROOF:STD:ASSIGN] erarecord=%s claim=%s parentcharge=%s xfer=%s score=%s action=%s\n",
    		    ($erarecord->{ID}//''), ($charges[0]{CLAIMID}//''), ($charges[0]{PARENTCHARGEID}//''),
		        ($charges[0]{TRANSFERTYPE}//''), ($charges[0]{MATCHINGSCORE}//''), ($charges[0]{ACTION}//'');
					# mark the chargeid/transfertype/claimid/matched/matchedby that this remit
					# matches to in the erarecord (and the overpaid flag)
					@{$erarecord}{      qw(CLAIMID CHARGEID       TRANSFERTYPE PATIENTINSURANCEID ACTION MATCHINGSCORE     MATCHED   MATCHEDBY)} = 
						(@{$charges[0]}{qw(CLAIMID PARENTCHARGEID TRANSFERTYPE PATIENTINSURANCEID ACTION MATCHINGSCORE)}, 'SYSDATE', $args->{USERNAME});
						$erarecord->{NOTE} .= '[NMC]' if $args->{MANUALUI};
				}
				# if this remit has a FROMDATE that predates the practice coming onto
				# athenaNet, then mark the remit as old DOS, and don't really count 
				# it as a failed match.
				# however, if this is a V number, then let's not mark that as olddos.
				elsif ( ($erarecord->{CONTROLNUMBER} !~ /^\d+V\d+$/) && $earliestdos && $erarecord->{FROMDATE} && AthenaDate::DeltaDays($erarecord->{FROMDATE},$earliestdos) < 0) {
					$erarecord->{MANUALFLAG} = 'D';
					$erarecord->{ERADISCARDSTATUSREASONID} = 'ROUTETOOTHER';
				}
			}
		}


		my $practiceid = Athena::Util::Database::SessionInfo($dbh)->{context};
		my @toggledpractices;
		my $togglevalue;
		my $conf = Athena::Conf::AthenaNet::Rollout('cpdc');
		my $toggleenabled;
		my $confvariable = $conf->{CPDC_5413_PARTIAL_MEMBER_ID_MATCH};
		if ($confvariable) {
			@toggledpractices = @{$conf->{tslistfortoggle_cpdc_5413} || []};
			if ((scalar @toggledpractices) > 1) {
				$togglevalue = (InList($practiceid, @toggledpractices)) ? 1 : 0;
			}
		}		
		$toggleenabled = ($confvariable && $togglevalue) ? 1 : 0;
		if($toggleenabled && !InList($erabatch->{PAYMENTBATCHROUTEID}, qw(1 1293))){		
		
			my @groupedbyclaim = GroupBy(['CLAIMID'], @erarecords);
			foreach my $claimgroup (@groupedbyclaim) {
				my $claimid = @{$claimgroup}[0]->{CLAIMID};
				if ($claimid eq '') {
					next;
				}

				try {
					$self->MatchBasedOnMemberId($dbh, {
						CLAIMID => $claimid,
						CHARGES => $claimgroup,
						USERNAME => $args->{USERNAME},
					});
				} catch {
					AthenaScribe->new(
						category => 'preprocesserabatches',
						logidentifier => 'preprocess_erabatches_match_using_memberid',
					)->log(
						message => "Error while matching using member id check: " . $_,
						level => 'error',
					);
				};
			}
		}
	}

	# Hydra 277681: match $0 billed record to first charge on claim
	my %unmatchedzerodollarrecordsids = ();
	my @unmatchedzerodollarrecords = map { $unmatchedzerodollarrecordsids{$_->{ID}} = 1; $_ } grep { $_->{AMOUNT} == 0 && $_->{PAYMENT} == 0 && !$_->{MATCHED} } @erarecords;
	my @otherrecords = grep { !$unmatchedzerodollarrecordsids{$_->{ID}} } @erarecords;
	my $unmatchedrecordexists = List::MoreUtils::any { !$_->{MATCHED} } @otherrecords;

	if (@unmatchedzerodollarrecords && @otherrecords && !$unmatchedrecordexists) {
		my @matchedclaimids = UniqueElements(map { $_->{CLAIMID} } @otherrecords);
		my @matchedtransfertypes = UniqueElements(map { $_->{TRANSFERTYPE} } @otherrecords);
		my @matchedpatientinsuranceids = UniqueElements(map { $_->{PATIENTINSURANCEID} } @otherrecords);

		my $zerodollarautotoggle = Athena::Conf::AthenaNet::AthenaXConf()->get("rollout.colpmm.nmc_reduction_zero_dollar");

		if (scalar(@matchedclaimids) == 1 && scalar(@matchedtransfertypes) == 1 && scalar(@matchedpatientinsuranceids) == 1) {
			foreach (@unmatchedzerodollarrecords) {
				$_->{CLAIMID} = $otherrecords[0]->{CLAIMID};
				$_->{TRANSFERTYPE} = $otherrecords[0]->{TRANSFERTYPE};
				$_->{PATIENTINSURANCEID} = $otherrecords[0]->{PATIENTINSURANCEID};
				$_->{MATCHED} = $otherrecords[0]->{MATCHED};
				$_->{MATCHEDBY} = $otherrecords[0]->{MATCHEDBY};
				$_->{MATCHINGSCORE} = $otherrecords[0]->{MATCHINGSCORE};
				$_->{CHARGEID} = $otherrecords[0]->{CHARGEID};
				$_->{ACTION} = $otherrecords[0]->{ACTION};
				$_->{NOTE} = '$0 record auto-matched - charge matching is ambiguous or below minimum allowed score';

				if($zerodollarautotoggle && $otherrecords[0]->{MATCHEDBY} ne 'ATHENA'){
					$_->{NOTE} .= '[nmc-auto-matched]';
					$_->{MATCHEDBY} = 'ATHENA';
				}
			}
		}
	}

	# if unpostable routing is turned on and if the whole claim is unmatched, then
	# any record which matches an unpostable routing rule should be auto-discarded.
	if (
		GetTablespaceValue("Unpostable Routing",$dbh) eq "ON"
	) {
		my $hashighmatchscorerecords = (grep { $_->{MATCHINGSCORE} >= $self->GetMinimumAllowedChargeMatchingScore($dbh) } @erarecords);
		for my $erarecord (grep { ($_->{MANUALFLAG} ne 'D') && (!$_->{MATCHED}) } @erarecords) {
			my $routeid = $self->GetDefaultUnpostableRoute($dbh, {
				ERARECORD => $erarecord,
				CONTEXTID => GetPracticeID(),
				TRIEDMATCHING => 1,
				HIGHMATCHSCORERECORDS => $hashighmatchscorerecords,
			});
			if ($routeid) {
				$erarecord->{MANUALFLAG} = 'D';
				$erarecord->{ERADISCARDSTATUSREASONID} = 'ROUTETOOTHER';
				$erarecord->{UNPOSTABLEROUTEID} = $routeid;
			}
		}
	}

	foreach my $erarecord (@erarecords) {
		if ($erarecord->{MANUALFLAG} eq "D"
			&& $erarecord->{APPLIED} eq ""
			&& $erarecord->{ERADISCARDSTATUSREASONID}
			&& !(
				$erarecord->{ERADISCARDSTATUSREASONID} eq "ROUTETOOTHER"
				&& !(
					$erarecord->{UNPOSTABLEROUTEID}
					|| (($erarecord->{CONTROLNUMBER} !~ /^\d+V\d+$/) && $earliestdos && $erarecord->{FROMDATE} && AthenaDate::DeltaDays($erarecord->{FROMDATE},$earliestdos) < 0)
				)
			)
			&& $paymentbatchid ne ""
			&& !$self->_DiscardRecordRequirePosterReview($dbh, {ERARECORD => $erarecord})
		) {

			$erarecord->{APPLIED} = 'SYSDATE';
			$erarecord->{APPLIEDBY} = 'ATHENA';
		}
	}

	# Hydra 1073062 - NOPOTENTIALCHARGE: Discard NMCP Auto as RTFS Auto (with exceptions)
	# Hydra 1077872 - REMITBEFOREBILLING: Discard non-athena records that precede billing
	# Hydra 1073570 - OVERPAYMENT: Discard non-athena records causing overpayments
	# Hydra 1079227 - CPT: Discard non-athena records with missing CPTs
	# Hydra 1082201 - CLMAMT: Discard non-athena records with DOS and claim billed mismatch
	
	my $parentclaimid;
   	my $practiceid = Athena::Util::Database::SessionInfo($dbh)->{context};  
    	my @toggledpractices;
   	my $togglevalue;
	my $conf = Athena::Conf::AthenaNet::Rollout('colpci');
    	my $partialposttoggle;
	my $posttoggle= $conf->{MC_COLPCI_2102_partial_post};
    	if($posttoggle){
		@toggledpractices =@{$conf->{mc_colpci_2102_partialpost_contexts} || []};
        	if((scalar @toggledpractices) > 1)
			{
				$togglevalue = (InList($practiceid,@toggledpractices)) ? 1 : 0;
			}	
    	}	
	$partialposttoggle = ($posttoggle && $togglevalue) ? 1 : 0;
    	if ($partialposttoggle) {
		my ($matchedrecord) = grep { $_->{CLAIMID} ne '' && $_->{MATCHED} ne '' } @erarecords;
		$parentclaimid = $matchedrecord->{CLAIMID}; 
	}

	foreach my $erarecord (@erarecords) {
		my $exceptions = $self->GlobalRTFSExclusions($dbh, { ERARECORD => $erarecord,
								     CLAIMID => $parentclaimid,
								     PAYMENTBATCHROUTEID => $erabatch->{PAYMENTBATCHROUTEID},
                                                                     PARTIALPOSTTOGGLE => $partialposttoggle
                                                                   });
		
		if ($erarecord->{CONTROLNUMBER} !~ /^\d+[vV]\d+$/ && $paymentbatchid ne "" && $exceptions) {
			my $remitbilling = SQLValues("
				select
					1
				from
					erarecord,
					claimnote
				where
					erarecord.claimid = claimnote.claimid
					and claimnote.action = 'BILL'
					and claimnote.created < erarecord.created
					and erarecord.id = ?", $dbh, $erarecord->{ID});

			my %charge = SQLHash("
				select
					*
				from
					tcharge
				where
					parentchargeid = ?
					and transfertype = ?", $dbh, $erarecord->{CHARGEID}, $erarecord->{TRANSFERTYPE});

			my $chargeamountchanged = SQLValues("
				select
					1
				from
					tcharge,
					chargenote
				where
					chargenote.chargeid = tcharge.chargeid
					and chargenote.fieldname = 'AMOUNT'
					and tcharge.type = 'CHARGE'
					and tcharge.id = ?", $dbh, $charge{ID});

			my $scorecomponents = $self->GetChargeMatchingScoringComponents($dbh);
			my $procedurecodematchingscore = $scorecomponents->{PROCEDURECODE}->{TEST}->($self, $dbh, { ERARECORD => $erarecord, CHARGE => \%charge });
			my $procedurecodemodifiermatchingscore = $scorecomponents->{PROCEDURECODEMODIFIER}->{TEST}->($self, $dbh, { ERARECORD => $erarecord, CHARGE => \%charge });
			my $dosmatch = 1;
			$dosmatch = AthenaDate::DeltaDays($erarecord->{FROMDATE}, $charge{FROMDATE}) if ($erarecord->{FROMDATE} && $charge{FROMDATE});

			my $transfer = SumCurrency (map {$_->{KICKEDAMOUNT}} grep {$_->{BALANCETRANSACTIONTYPE} =~ /(TRANSFER)/} @{$erarecord->{KICKS}});
			my $adjustment = SumCurrency (map {$_->{KICKEDAMOUNT}} grep {$_->{BALANCETRANSACTIONTYPE} =~ /(ADJUSTMENT)/} @{$erarecord->{KICKS}});
			my $newpayment =  $erarecord->{PAYMENT};
			my $totaloutstanding = SQLValues("
				select sum(outstanding) 
				from tcharge 
				where parentchargeid = ? 
				and transfertype = ?
			", $dbh, $erarecord->{CHARGEID}, $erarecord->{TRANSFERTYPE});
			my $claimstatus = SQLValues("
				select 
				case erarecord.transfertype 
				when '1' then claim.status1 
				when '2' then claim.status2 
				when 'p' then claim.statusp 
				else null end as claimstatus 
				from claim, erarecord 
				where claim.id = erarecord.claimid 
				and erarecord.id = ?
			", $dbh, $erarecord->{ID});
			my $isalreadyposted = SQLValues("
				select 1 
				from erarecord e1,erarecord e2 
				where REGEXP_LIKE (e1.controlnumber, '[[:digit:]][Vv][[:digit:]]') 
				and e1.matched is not null 
				and e1.applied is not null 
				and e1.transfertype = e2.transfertype 
				and e1.id < e2.id 
				and e1.created < e2.created 
				and e1.payment > 0 
				and e1.claimid = e2.claimid 
				and e2.matched is not null 
				and e2.applied is null 
				and e1.claimid = ?
			", $dbh, $erarecord->{CLAIMID});
			my $isoverpayment = ($transfer+ $adjustment+ $newpayment) > $totaloutstanding ? 1 : 0;
			my $rtfsreason;
			if ($erarecord->{MATCHINGSCORE} == -1) {
				$rtfsreason = 1;
				$erarecord->{GLOBALROUTINGREASON} = 'NOPOTENTIALCHARGE';
				$erarecord->{UNPOSTABLENOTE} = 'No potential matching charge was found in athenaNet for this non-athena record.';
			}
			elsif ($exceptions == 2 && $partialposttoggle){
				$rtfsreason = 1;
				$erarecord->{GLOBALROUTINGREASON} = 'REMITBEFOREBILLING';
				$erarecord->{UNPOSTABLENOTE} = 'Non-athena record discarded for being received prior to billing the matching claim, CSN Not Matched.';
			}
			elsif ($dosmatch != 0) {
				if (!$remitbilling) {
					$rtfsreason = 1;
					$erarecord->{GLOBALROUTINGREASON} = 'REMITBEFOREBILLING';
					$erarecord->{UNPOSTABLENOTE} = 'Non-athena record discarded for being received prior to billing the matching claim.';
				}
				elsif ($isoverpayment && ($claimstatus eq 'CLOSED' || $isalreadyposted)) {
					$rtfsreason = 1;
					$erarecord->{GLOBALROUTINGREASON} = 'OVERPAYMENT';
					$erarecord->{UNPOSTABLENOTE} = 'Non-athena record discarded to prevent an overpayment.';
				}
				elsif ($procedurecodematchingscore->{SCORE} <= 0 && $procedurecodemodifiermatchingscore->{SCORE} <= 0) {
					$rtfsreason = 1;
					$erarecord->{GLOBALROUTINGREASON} = 'CPT';
					$erarecord->{UNPOSTABLENOTE} = 'Non-athena record discarded due to procedure code discrepancies.';
				}
				elsif (!$chargeamountchanged && $charge{AMOUNT} != $erarecord->{AMOUNT} && $erarecord->{MATCHED} ne "" && $erarecord->{MATCHEDBY} ne "" && $erarecord->{CLAIMID} ne "") {
					$rtfsreason = 1;
					$erarecord->{GLOBALROUTINGREASON} = 'CLMAMT';
					$erarecord->{UNPOSTABLENOTE} = 'Non-athena record discarded due to non-matching claim billed amount.';
				}
			}

			if ($rtfsreason) {
				$erarecord->{APPLIED} = 'SYSDATE';
				$erarecord->{APPLIEDBY} = 'ATHENA';
				$erarecord->{MANUALFLAG} = 'D';
				$erarecord->{ERADISCARDSTATUSREASONID} = 'ROUTETOOTHER';
			}
		}
	}

	# match to kickreasons
	foreach my $erarecord (@erarecords) {
		foreach my $erakick (@{$erarecord->{KICKS}}) {
			$self->MatchRecordToKick($dbh,{
				ERARECORD => $erarecord,
				ERAKICK   => $erakick,
				USERNAME  => $args->{USERNAME},
			});
		}
	}
	# START: aggregate incentive payments
	my @records;
	foreach my $erarecord (@erarecords) {
		
		my %record;
		my @incentiveprogramids;
		my @incentivekicks =  grep { $_->{BALANCETRANSACTIONTYPE} eq 'INCENTIVE' } @{$erarecord->{KICKS}};

		if (@incentivekicks) {
			@incentivekicks = SQL::Select->new()->Select(
					'erakick.kickcode',
					'erakick.kickedamount',
					'kickreason.incentiveprogramid',
					'incentiveprogram.name incentiveprogramname',
				)->From(
					'erakick',
					'kickreason',
					'incentiveprogram'
				)->Flags(
					{ TEMPTABLEOPTIN => 1 }
				)->Joins(
					'erakick.kickreasonid = kickreason.id',
					'kickreason.incentiveprogramid = incentiveprogram.id',
				)->Where(
					['erakick.id in ( ?? )', [map {$_->{ID}} @incentivekicks] ],
				)->TableHash($dbh);
				
			push @incentiveprogramids, $erarecord->{INCENTIVEPROGRAMID} if $erarecord->{INCENTIVEPAYMENT};
			@incentiveprogramids = UniqueElements( map {$_->{INCENTIVEPROGRAMID}} @incentivekicks);
			if (scalar (@incentiveprogramids) == 1) {
				$erarecord->{INCENTIVEPROGRAMID} = $incentiveprogramids[0];
			} else {
				$erarecord->{INCENTIVEPROGRAMID} = undef;
				foreach my $kick (@incentivekicks) {
					$kick->{NOTE} = "Kick $kick->{KICKCODE} ($kick->{INCENTIVEPROGRAMNAME}) for \$ $kick->{KICKEDAMOUNT}";
				}
				my $incentivenote = join',', map {$_->{NOTE}} @incentivekicks;
				unless(Athena::Conf::AthenaNet::Rollout('crzs')->{hpci_886_disable_prevent_dupe_note}){	
					my $programnote = "Multiple incentive programs remitted on this:";
					unless($erarecord->{NOTE} =~ /$programnote/) {
						$erarecord->{NOTE} .= "$programnote $incentivenote";
					}
				}
				else {
					$erarecord->{NOTE} .= "Multiple incentive programs remitted on this: $incentivenote";
				}
			}

			my $incentivepayment = SumCurrency(map {-$_->{KICKEDAMOUNT}} @incentivekicks);
			$erarecord->{INCENTIVEPAYMENT} = $incentivepayment;
			$erarecord->{INCENTIVEPROGRAMYEAR} = AthenaDate::SafeFormatDate($erarecord->{FROMDATE}, 'YYYY');

			$record{ID} = $erarecord->{ID};
			$record{INCENTIVEPAYMENT} = $erarecord->{INCENTIVEPAYMENT};
			$record{INCENTIVEPROGRAMID} = $erarecord->{INCENTIVEPROGRAMID};
			$record{INCENTIVEPROGRAMYEAR} = $erarecord->{INCENTIVEPROGRAMYEAR};
			$record{NOTE} = $erarecord->{NOTE};
			push @records, \%record;
		}
	}
	$self->EngineProcessTable($dbh, {
		TABLENAME	=> 'ERARECORD',
		USERNAME	=> $args->{USERNAME},
		OPERATION	=> 'Update',
		TABLEROWS	=> \@records,
		COLUMNNAMES	=> ['INCENTIVEPAYMENT', 'INCENTIVEPROGRAMID', 'INCENTIVEPROGRAMYEAR', 'NOTE']
	});
	#END: aggregate incentive payments

	# Auto-Discard the records as 'NOMATCHINGCHARGE' or 'UNMATCHREC' whose:
	# -score is <= 0
	# -controlnumber not athena-style or not for this practice
	# -patient lastname is not null
	foreach my $erarecord (@erarecords) {
		if (
			defined $erarecord->{MATCHINGSCORE}
			&& $erarecord->{MATCHINGSCORE} <= 0
			&& $erarecord->{MANUALFLAG} ne 'D'
			&& $erarecord->{APPLIED} eq ""
			&& $erarecord->{PATIENTLASTNAME} ne ""
			&& $paymentbatchid ne ""
		) {
			my ($practiceid) = $erarecord->{CONTROLNUMBER} =~ /^F?\d+[VA](\d+)(?:G(\d+))?$/i;
			# If the record is for this practice, but it didn't match, something is weird.  Let a poster look at it.
			unless ($practiceid == GetPracticeID()) {
				$erarecord->{MANUALFLAG} = 'D';
				$erarecord->{APPLIED} = 'SYSDATE';
				$erarecord->{APPLIEDBY} = 'ATHENA';
				# If it looks like one of ours but for another practice, discard as "Unmatched AthenaNet Record."
				# If the controlnumber doesn't look like one of ours, discard as "No matching charge/patient."
				$erarecord->{ERADISCARDSTATUSREASONID} = $practiceid ? 'UNMATCHREC' : 'NOMATCHINGCHARGE';
			}
		}
	}

	# store match info in database
	my @sqlcols = qw(
		CHARGEID CLAIMID TRANSFERTYPE KICKREASONID BALANCETRANSACTIONTYPE 
		BALANCETRANSACTIONREASON ACTION MANUALFLAG NOTE MATCHINGSCORE MATCHED MATCHEDBY
		MAPPED MAPPEDBY ERADISCARDSTATUSREASONID ERADISCARDSTATUSID 
		ASSIGNMENTGROUPCLASSID PENDALARMDATE PATIENTINSURANCEID UNPOSTABLEROUTEID
	);
	my $allmatch = 1;
	foreach my $erarecord (@erarecords) {
		if ($erarecord->{MANUALFLAG} eq "D"
			&& ($erarecord->{originalrecord}->{MANUALFLAG} ne "D"
				|| ($erarecord->{APPLIED} && !$erarecord->{originalrecord}->{APPLIED}))) {

			my $contextid = GetPracticeID();
			$self->DiscardRecord($dbh, {
				ERARECORD => $erarecord,
				USERNAME => $args->{USERNAME},
				CONTEXTID => $contextid,
				GENERATESTATUS => 1,
			});
		}
		

		if (($erarecord eq '' && $erarecord->{KICKCODE} ne '') || $erarecord->{MATCHED} eq '') {
			$allmatch = 0;
		}
		# check if record has changed at all
		foreach my $col (@sqlcols) {
			# if any column has changed its value, update the row in the DB and move on
			if ($erarecord->{$col} ne $erarecord->{originalrecord}->{$col}) {
				$erarecord->{operation} = 'Update';
				ProcessForm('ERARECORD',$dbh,$args->{USERNAME},$erarecord,[@sqlcols]);
				last;
			}
		}
	}

	# H269546: [Crossover Payer] Populate member ID when remittance is received
	my @notmatched = grep { $_ ne '' } map { !$_->{MATCHED} } @erarecords;
	if($erarecords[0]{TRANSFERTYPE} eq '2' && !@notmatched && $paymentbatchid) {
		my @patientinsurances = SQL::Select->new()->Select(
			"patientinsurance.id",
			"patientinsurance.patientid",
			"patientinsurance.notes",
			"patientinsurance.insurancepackageid",
			"insurancepackage.idnumberformat",
			"insurancepackage.invalididnumberformat",
		)->From(
			"patientinsurance",
			"insurancepackage",
		)->Flags(
			{ TEMPTABLEOPTIN => 1 }
		)->Where(
			"patientinsurance.insurancepackageid = insurancepackage.id",
			["patientinsurance.id in ( ?? )", [map {$_->{PATIENTINSURANCEID}} @erarecords] ],
			"patientinsurance.sourceerarecordid is not null",
			"insurancepackage.id <> 18500",
			"patientinsurance.idnumber is null",
		)->TableHash($dbh);
		foreach my $patientinsurance (@patientinsurances) {
			my @erarecord = grep {$_->{PATIENTINSURANCEID} == $patientinsurance->{ID}} @erarecords;
			# If we have both patient and corrected and no subscriber, the patient is the subscriber.
			# If we have all three, the corrected is a correction to subscriber, not to patient.
			my $idnumber = $erarecord[0]{CORRECTEDINSURANCEIDNUMBER} && $erarecord[0]{PATIENTINSURANCEIDNUMBER} && $erarecord[0]{SUBSCRIBERINSURANCEIDNUMBER}
						 ? $erarecord[0]{PATIENTINSURANCEIDNUMBER}
						 : nvl($erarecord[0]{CORRECTEDINSURANCEIDNUMBER}, $erarecord[0]{PATIENTINSURANCEIDNUMBER}, $erarecord[0]{SUBSCRIBERINSURANCEIDNUMBER})
			;
			if ($idnumber) {
				my $insuredidnumber = nvl($erarecord[0]{CORRECTEDINSURANCEIDNUMBER}, $erarecord[0]{SUBSCRIBERINSURANCEIDNUMBER}, $erarecord[0]{PATIENTINSURANCEIDNUMBER});
				# Make sure id number(s) match the expected format for this package.
				my $validnumbers = BusCall::Insurance::CheckIDNumber({
					IDNUMBER => $idnumber,
					IDNUMBERFORMAT => $patientinsurance->{IDNUMBERFORMAT},
					INVALIDIDNUMBERFORMAT => $patientinsurance->{INVALIDIDNUMBERFORMAT},
				});
				$validnumbers &&= BusCall::Insurance::CheckIDNumber({
					IDNUMBER => $insuredidnumber,
					IDNUMBERFORMAT => $patientinsurance->{IDNUMBERFORMAT},
					INVALIDIDNUMBERFORMAT => $patientinsurance->{INVALIDIDNUMBERFORMAT},
				}) if $insuredidnumber ne $idnumber;
				if ($validnumbers) {
					my $notes = join "\n", ($patientinsurance->{NOTES}, sprintf("Member ID has been populated from remittance received in Payment Batch %dA%d", $paymentbatchid, GetPracticeID()));
					BusCall::PatientInsurance::Update($dbh, {
						PATIENTINSURANCE => {
							ID => $erarecord[0]{PATIENTINSURANCEID},
							IDNUMBER => $idnumber,
							INSUREDIDNUMBER => $insuredidnumber,
							PATIENTID => $patientinsurance->{PATIENTID},
							NOTES => $notes,
						},
						USERNAME => 'ATHENA',
						SQLCOLS => [qw(IDNUMBER INSUREDIDNUMBER NOTES)],
					});
				}
			}
		}
	}
	return $allmatch;
}


sub _IsDigitSequeceMatch {
	my ($self, $args) = @_;
	my $practiceid = $args->{PRACTICEID};
	my $eracontextid = $args->{ERACONTEXTID};

	return 0 if length($practiceid) != length($eracontextid);

	#To check %100 matched but digits are in different sequence 
	my $digitsequencematched = 1;
	my @sortederacontext = sort (split(//, $eracontextid));
	my @sortedpracticeid = sort (split(//, $practiceid));
    
	#To check %75 matched - exact matched digits / total length  - should be > 75%
	my $matchedcount = 0;
	my $index = 0;
	foreach my $digit (split(undef, $practiceid)) {
   		
		my @eradigits = split(undef, $eracontextid);
		if(@eradigits[$index] == $digit ){
			$matchedcount++;
		}
		
		#To check %100 matched
		if(@sortederacontext[$index] != @sortedpracticeid[$index]){
			$digitsequencematched =0;
		}

		$index++;
	}  

	return 1 if (($matchedcount / length($practiceid) * 100  >= 75) || $digitsequencematched);
}

#########################################################################################
# MatchRecordsToCharges
#
# Description:
# 	-Matches a set of identical erarecords (identical on matching criteria at least) 
#		to a set of charges
#	-when matching chargeid, also pulls claimid and transfertype for optimizing some
#		queries a little later on (like KickReason::Lookup, and ApplyBatch for example)
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs
#
# Return Value:
#	matched erarecord hash
#########################################################################################
sub MatchRecordsToCharges {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args, [qw( ERABATCH ERARECORDS )]);
	AssertValidFields($args,    [qw( ERABATCH ERARECORDS )]);

	my $erabatch = $args->{ERABATCH};
	my @erarecords = @{$args->{ERARECORDS}};
	my $contextid = GetPracticeID($dbh);

	my $numberofmatches = @erarecords;
	my $erarecord = $erarecords[0];

	my $matchlog;
	if ($ENV{ERAFILE_MATCHCLAIMLOGFILE}) {
		# Logging match results to a file.
		my @erarecordids = sort {$a<=>$b} map {$_->{ID}} @erarecords;
		$matchlog = {
			ERAFILEID => $erabatch->{RPOERAFILEID},
			ERABATCHID => $erabatch->{ID},
			ERARECORDIDS => \@erarecordids,
			ERARECORD => $erarecord,
			CONTEXTID => GetPracticeID(),
		 };
	}

	# verify that all of the supposedly identical erarecords are, in fact, identical
	if (@erarecords > 1) {
		foreach my $othererarecord (@erarecords[1..$#erarecords]) {
			# compare the important fields
			foreach my $col (@ERA::RecordMatchingCols) {
				if ($othererarecord->{$col} ne $erarecord->{$col}) {
					my $errorstring = "ERA::Engine::MatchRecordsToCharges called with a set of erarecords containing different match criteria.";
					if ($matchlog) {
						$matchlog->{DESCRIPTION} = $errorstring;
						ERA::QA::LogRecordMatchingScores($matchlog);
					}
					confess $errorstring;
				}
			}
		}
	}

	# Unless eradiscardstatusreason.posterreviewflag for this erarecord's
	# eradiscardstatusreason, create an unpostable, and remove this from the worklist
	# If reason is ROUTETOOTHER or ROUTETONONE, also have to confirm that
	# $erarecord->{UNPOSTABLEROUTEID} is not null.
	if (
		!$erarecord->{APPLIED}
		&& $erarecord->{ERADISCARDSTATUSREASONID}
		&& !(
			InList($erarecord->{ERADISCARDSTATUSREASONID}, qw( ROUTETOOTHER ROUTETONONE ))
			&& !$erarecord->{UNPOSTABLEROUTEID}
		)
		&& !$self->_DiscardRecordRequirePosterReview($dbh, {ERARECORD => $erarecord})
	) {
		my $paymentbatchid = SQLValues("select id from paymentbatch where erabatchid = ? ", $dbh, $erarecord->{ERABATCHID});
		if ($paymentbatchid) {
			for my $routed (@erarecords) {		
				$routed->{APPLIED} = 'SYSDATE';
				$routed->{APPLIEDBY} = 'ATHENA';
				$self->DiscardRecord($dbh, {
					USERNAME => 'ATHENA',
					ERARECORD => $routed,
					CONTEXTID => GetPracticeID(),
					GENERATESTATUS => 1,
				});
			}
		}
	}

	# If we already have the CHARGEID and all, we're all set.
	unless ($erarecord->{CHARGEID} eq '' || $erarecord->{CLAIMID} eq '' || $erarecord->{TRANSFERTYPE} eq '') {
		if ($matchlog) {
			$matchlog->{DESCRIPTION} = 'erarecord ' . $erarecord->{ID} . ' already matched.';
			ERA::QA::LogRecordMatchingScores($matchlog);
		}
		return ();
	}

	# a bunch of stuff needs this action maintained later if it is patientpayment.
	$erarecord->{ACTION} = '' unless $erarecord->{ACTION} =~ /PATIENTPAYMENT|CLAIMLEVELREMIT/;
	$erarecord->{CHARGEID} = $erarecord->{TRANSFERTYPE} = '';

	# actually determine which charges match
	my @matchingcharges = $self->GetMatchingCharges($dbh, {
		ERARECORDS => \@erarecords,
		ERABATCH => $args->{ERABATCH},
		CONTEXTID => $contextid,
		MATCHLOG => $matchlog,
	});
	return @matchingcharges;
}


#########################################################################################
# GetMatchingCharges
#
# Description:
# 	-Finds a set of charges matching a set of identical erarecords (identical on
#	 	matching criteria at least)
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs
#		CONTEXTID	look in this context for matching charges
#		(optional)
#		MATCHLOG	a data structure populated with logging / debugging
#					information if passed
#
# Return Value:
#	array of charge hashrefs
#########################################################################################
sub GetMatchingCharges {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORDS ERABATCH CONTEXTID )]);
	AssertValidFields($args,    [qw( ERARECORDS ERABATCH CONTEXTID MATCHLOG )]);
	my $matchlog = $args->{MATCHLOG};
	my @erarecords = @{$args->{ERARECORDS}};
	my $graphitelogconf = Athena::Conf::AthenaNet::AthenaXConf()->get("rollout.colpci.COLPCI_562_log_graphite_data");
	my @potentialcharges = $self->GetPotentialMatchingCharges($dbh, {
		ERARECORDS => \@erarecords,
		ERABATCH => $args->{ERABATCH},
		CONTEXTID => $args->{CONTEXTID},
		MATCHLOG => $matchlog,
		AUTOMATESELFPAYCHARGESINSURANCEPACKAGES => [$self->GetAutomateSelfPayChargesInsurancePackages($dbh, {
			CONTEXTID => $args->{CONTEXTID},
			MATCHING => 1,
		})],
	});
	# automatch doesn't need chargeid when there are multiple charges
	map { $_->{PARENTCHARGEID} = '' if $_->{ACTION} eq 'CLAIMLEVELREMIT' } @potentialcharges;

	# if there aren't enough candidates for matching, then we can't possibly
	# have enough matches

	# Return the first record of the potentialcharges to record the score
	# The score is sorted in decending order and hence we are interested in
	# in the first record. Also, set a flag UNMATCH to distinguish between
	# matched and umatched charges.
	# Also, record the score as -1 if we dont have any potential charges for
	# the erarecord
	my $desc;
	my $potentialmatch;
	my $score;

	if (!@potentialcharges || ((scalar(@potentialcharges) == 1) && $potentialcharges[0]->{TOOMANYCHARGES})) {
		$desc = scalar(@potentialcharges) ? "Too many potential charges found" : "No potential charges found";
		$score = -1;
		if ($matchlog) {
			$matchlog->{DESCRIPTION} = $desc;
			ERA::QA::LogRecordMatchingScores($matchlog);
		}
		@{$potentialmatch}{ qw(MATCHINGSCORE UNMATCH DESCRIPTION) } = ($score, 1, $desc);
		warn sprintf "[PROOF:STD:UNMATCH:DETAIL] recs=[%s] reason=\"%s\" topScore=%s needed=%d got=%d",
  	  (join(",", map { $_->{ID} } @{$args->{ERARECORDS}})),
	    ($desc//''),
    	($potentialcharges[0]{MATCHINGSCORE}//''),
  	  scalar(@erarecords),
	    scalar(@potentialcharges));
		return $potentialmatch;
	}
	#201484 - Wiki page to regress matching scores for ERA.
	#If full match flag is set to get all the matching charges, Return all potential charges

	if($ENV{FULLMATCH} == 1){
		if ($matchlog) {
			$matchlog->{DESCRIPTION} = $desc;
			ERA::QA::LogRecordMatchingScores($matchlog);
		}
		return ($potentialmatch);
	}

	if (@potentialcharges < @erarecords) {
		$desc = "Too few potential charges";
		$score = $potentialcharges[0]{MATCHINGSCORE};
		if ($matchlog) {
			$matchlog->{DESCRIPTION} = $desc;
			ERA::QA::LogRecordMatchingScores($matchlog);
		}
		@{$potentialmatch}{ qw(MATCHINGSCORE UNMATCH DESCRIPTION) } = ($score, 1, $desc);
		warn sprintf "[PROOF:STD:UNMATCH:DETAIL] recs=[%s] reason=\"%s\" topScore=%s needed=%d got=%d",
  	  (join(",", map { $_->{ID} } @{$args->{ERARECORDS}})),
	    ($desc//''),
    	($potentialcharges[0]{MATCHINGSCORE}//''),
  	  scalar(@erarecords),
	    scalar(@potentialcharges));
		return ($potentialmatch);
	}

	# remove as many charges as needed from the front (best) of the candidates
	my @matchingcharges = splice(@potentialcharges, 0, scalar(@erarecords));

	# the worst matching of all the charges we're planning on counting as matching
	# has to be above a minimum acceptance threshold
	my $worstwinner = $matchingcharges[-1];
	my $minallowedscore = $self->GetMinimumAllowedChargeMatchingScore($dbh);
	if ($worstwinner->{MATCHINGSCORE} < $minallowedscore) {
		$desc = "Matching charge below minimum allowed score ($minallowedscore)";
		$score = $potentialcharges[0]{MATCHINGSCORE};
		if ($matchlog) {
			$matchlog->{DESCRIPTION} = $desc;
			ERA::QA::LogRecordMatchingScores($matchlog);
		}
		@{$potentialmatch}{ qw(MATCHINGSCORE UNMATCH DESCRIPTION) } = ($score, 1, $desc);
		warn sprintf "[PROOF:STD:UNMATCH:DETAIL] recs=[%s] reason=\"%s\" topScore=%s needed=%d got=%d",
  	  (join(",", map { $_->{ID} } @{$args->{ERARECORDS}})),
	    ($desc//''),
    	($potentialcharges[0]{MATCHINGSCORE}//''),
  	  scalar(@erarecords),
	    scalar(@potentialcharges));
		return ($potentialmatch);
	}

	# if the "worst winner" is too close to the "best loser", then we should
	# consider this no better than a tie, i.e. an ambiguous (and therefore
	# unsafe) match
	# When matching a charge to 18500, lets break the tie in case of ambiguous charges.
	my $bestloser = $potentialcharges[0] || {MATCHINGSCORE => 0};
	my $mindifference = $self->GetMinimumAllowedChargeMatchingScoreDifference($dbh);
	if (($worstwinner->{MATCHINGSCORE} - $bestloser->{MATCHINGSCORE}) < $mindifference) {
		my $breaktie;
		my $tieresult = $self->TieBreakAmongAmbiguousMatches($dbh, {
			ERARECORDS => \@erarecords,
			POTENTIALCHARGES => \@potentialcharges,
			MATCHINGCHARGES => \@matchingcharges,
		});
		if ($tieresult->{RESULT} eq 'Y') {
			$breaktie = 1;
		}
		$desc = $tieresult->{DESC};
		$score = $potentialcharges[0]{MATCHINGSCORE};
		if ($matchlog && !$breaktie) {
			$matchlog->{DESCRIPTION} = $desc;
			ERA::QA::LogRecordMatchingScores($matchlog);
		}
		@{$potentialmatch}{ qw(MATCHINGSCORE UNMATCH DESCRIPTION) } = ($score, 1, $desc);
		warn sprintf "[PROOF:STD:UNMATCH:DETAIL] recs=[%s] reason=\"%s\" topScore=%s needed=%d got=%d",
  	  (join(",", map { $_->{ID} } @{$args->{ERARECORDS}})),
	    ($desc//''),
    	($potentialcharges[0]{MATCHINGSCORE}//''),
  	  scalar(@erarecords),
	    scalar(@potentialcharges));
		return ($potentialmatch) unless($breaktie);
	}

	# when matching "identical payments to identical charges", the matching
	# charges must all be on the same claim/transfertype and with the same
	# patientinsurance (that is: we would not want to count the same charge
	# matching primary for one payment and secondary for another payment... or
	# matching the same charge/transfertype but with different policies)
	if (GroupBy([qw( CLAIMID TRANSFERTYPE PATIENTINSURANCEID )], @matchingcharges) > 1) {
		$desc = scalar(@erarecords) . " identical payments matched to charges on different claims / payors";
		$score = $potentialcharges[0]{MATCHINGSCORE};
		if ($matchlog) {
			$matchlog->{DESCRIPTION} = $desc;
			ERA::QA::LogRecordMatchingScores($matchlog);
		}
		@{$potentialmatch}{ qw(MATCHINGSCORE UNMATCH DESCRIPTION) } = ($score, 1, $desc);
		warn sprintf "[PROOF:STD:UNMATCH:DETAIL] recs=[%s] reason=\"%s\" topScore=%s needed=%d got=%d",
  	  (join(",", map { $_->{ID} } @{$args->{ERARECORDS}})),
	    ($desc//''),
    	($potentialcharges[0]{MATCHINGSCORE}//''),
  	  scalar(@erarecords),
	    scalar(@potentialcharges));
		return ($potentialmatch);
	}

	# success!

	# If we have just now matched to a policy the patient doesn't yet have, we need to
	# create a "New Unspecified Payor" patientinsurance.
	# Currently this should happen only for certain matches as primary to self-pay
	# charges.
	# (An erarecord from any normal kind of matching will have already found a
	# patientinsurance by this point.)
	# We have to take care of this some time:
	# -after we have decided that there are charges we will match to
	#  (which we just did)
	# -and before recording the match back up in MatchClaim
	#  (which we are about to do)
	$self->_MaybeAddUnspecifiedRemitPayor($dbh, {
		%$args,
		MATCHINGCHARGES => \@matchingcharges
	});

	if ($matchlog) {
		$matchlog->{MATCHINGOBJECTS} = \@matchingcharges;
		$matchlog->{DESCRIPTION} = "Successful match";
		ERA::QA::LogRecordMatchingScores($matchlog);
	}
	if($graphitelogconf){
		foreach my $stats_matchingcharges (@matchingcharges) {
			StatsD::Count({ KEY => "remittance.match_scope_contextid.$stats_matchingcharges->{MATCHINGQUERYSCOPE}.$stats_matchingcharges->{CONTEXTID}", PRODUCT => 'Collector'});
			StatsD::Count({ KEY => "remittance.match_scope.$stats_matchingcharges->{MATCHINGQUERYSCOPE}", PRODUCT => 'Collector'});
        	}
        }
	return @matchingcharges;
}


#########################################################################################
# GetPotentialMatchingCharges
#
# Description:
# 	-Finds and *scores* a set of charges matching which might match a set of
#	 	identical erarecords (identical on matching criteria at least)
#	-What we mean by a "charge" here is really the particular combination of a
#		*service* (transaction.type='CHARGE') billed to a payor
#		(patientinsurance) for a transfertype (1 or 2).  So the relevant
#		structure being passed around is just that: a CHARGE (no transferins!),
#		joined with a patientinsurance and a transfertype.
#	-Performs an iterative, widening search.  If a narrow search cannot find a
#		sufficiently large stock of potential charges, it will run respectively
#		broader queries until it either finds a large enough stock of charges
#		(at least as many charges whose score meets the minimum threshold as
#		there are erarecords being matched against), or it has exhausted all
#		possible sources for charges.  This is entirely a performance
#		constraint.  It tends to get in the way a little bit, but it is
#		unfortunately fairly necessary.
#	-Each charge gets a score assigned to it as MATCHINGSCORE: a number that
#		reflects how good of a match to the supplied erarecords it is.  It also
#		has a detailed listing of the individual field-matching results from
#		whith the score is derived (as a hashref called MATCHINGRESULTS).
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs
#		CONTEXTID	look in this context for matching charges
#		(optional)
#		MATCHLOG	a data structure populated with logging / debugging
#					information if passed
#		CUSTOMFILTER a SQL object which, if passed, will be the basis of the
#					search, rather than the default iterative broadening search.
#					This is used, e.g., in the manual search form, wherein a
#					user is supplying a filter
#		UNSPECIFIEDREMITPAYOR
#					(boolean) Include a dummy patientinsurance in the results
#					Currently passed only by MatchingFormCharges on behalf
#					of billing/eramatchcharge.esp (manual matching).
#		AUTOMATESELFPAYCHARGESINSURANCEPACKAGES
#					(listref) Allow matches to charges on self-pay claims only
#					where the primary policy is for one of these packages.
#					Currently passed only by GetMatchingCharges (auto-
#					matching) and only for Minute-Clinic contexts.
#
# Return Value:
#	array of charge hashrefs
#########################################################################################
sub GetPotentialMatchingCharges {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORDS ERABATCH CONTEXTID )]);
	AssertValidFields($args,    [qw( ERARECORDS ERABATCH CONTEXTID
		MATCHLOG
		CUSTOMFILTER
		UNSPECIFIEDREMITPAYOR
		AUTOMATESELFPAYCHARGESINSURANCEPACKAGES
	)]);

	my @erarecords = @{$args->{ERARECORDS}};
	my $matchlog = $args->{MATCHLOG};
	my $minallowedscore = $self->GetMinimumAllowedChargeMatchingScore($dbh);
	my $mingoodscore = $self->GetMinimumGoodChargeMatchingScore($dbh);
	
	# a place in which to memoize the results of intermediate computations
	my $cache = {};
	
	# for the sake of speed, we run a more narrow search, on well-indexed
	# fields, before running a broader search that could still (potentially)
	# find good enough results
	my (@potentialcharges, %potentialchargesindex);
	my @searchscopes = $args->{CUSTOMFILTER} ? ('CUSTOM') : qw( EXACT NARROW CLAIMPATIENT POLICY LASTNAME BROAD );
	for my $scope (@searchscopes) {
		### THIS IS ENTIRELY A PERFORMANCE OPTIMIZATION ###
		# if the EXACT or NARROW queries turned up any results, AND the
		# patient/date-of-service on those was correct, then it's a reasonable
		# assumption that the controlnumber listed in the ERA/EOB was correct,
		# and we're really not going to do any better by searching broadly for
		# other claims.  Just as well skip that BROAD query, then, because it
		# can be very slow
		last if
			$scope eq 'BROAD'
			&& grep {
				$_->{MATCHINGRESULTS}->{PATIENTLASTNAME}->{SCORE}
				&& $_->{MATCHINGRESULTS}->{FROMDATE}->{SCORE}
			} @potentialcharges;
	
		my ($t0, $t1);
		$t0 = [gettimeofday] if $matchlog;
		my @newpotentialcharges = $self->GetPotentialMatchingChargesByScope($dbh, {
			SCOPE => $scope,
			ERARECORD => $erarecords[0],
			ERABATCH => $args->{ERABATCH},
			CONTEXTID => $args->{CONTEXTID},
			CUSTOMFILTER => $args->{CUSTOMFILTER},
			UNSPECIFIEDREMITPAYOR => $args->{UNSPECIFIEDREMITPAYOR},
			AUTOMATESELFPAYCHARGESINSURANCEPACKAGES => $args->{AUTOMATESELFPAYCHARGESINSURANCEPACKAGES},
		});
		$t1 = [gettimeofday] if $matchlog;
		# If the number of potential charges exceeds a threshold, we can possibly run out of
		# memory when trying to score them. In this case, its better to skip scoring
		# now and let the user/poster use a better custom filter to find matches.
		if (scalar(@newpotentialcharges) > MAXPOTENTIALCHARGES) {
			if ($matchlog) {
				$matchlog->{STATS}->{$scope}->{QUERYTIME} += tv_interval($t0, $t1);
				$matchlog->{STATS}->{$scope}->{QUERYRESULTS} += scalar(@newpotentialcharges);
				$matchlog->{STATS}->{$scope}->{QUERYITERATIONS} ++;
			}
			return ({TOOMANYCHARGES => 1});
		}
		foreach my $newpotentialcharge (@newpotentialcharges) {
			# add the new potential charge to the growing list of potential
			# charges, but only if it isn't already there
			my $key = join(',', @{$newpotentialcharge}{qw( PARENTCHARGEID TRANSFERTYPE PATIENTINSURANCEID )});
			next if $potentialchargesindex{$key};
			$potentialchargesindex{$key} = $newpotentialcharge;
			push(@potentialcharges, $newpotentialcharge);
			$newpotentialcharge->{MATCHINGQUERYSCOPE} = $scope;

			# figure and attach the score
			my $scoreresults = $self->GetObjectMatchingScore($dbh, {
				CHARGE => $newpotentialcharge,
				ERARECORD => $erarecords[0],
				ERABATCH => $args->{ERABATCH},
				CONTEXTID => $args->{CONTEXTID},
				CACHE => $cache,
			});
			$newpotentialcharge->{$_} = $scoreresults->{$_} for qw( MATCHINGSCORE MATCHINGRESULTS );
			$matchlog->{STATS}->{$scope}->{SCOREITERATIONS}++ if $matchlog;
		}
		if ($matchlog) {
			$matchlog->{STATS}->{$scope}->{QUERYTIME} += tv_interval($t0, $t1);
			$matchlog->{STATS}->{$scope}->{QUERYRESULTS} += scalar(@newpotentialcharges);
			$matchlog->{STATS}->{$scope}->{QUERYITERATIONS} ++;
			$matchlog->{STATS}->{$scope}->{SCORETIME} += tv_interval($t1, [gettimeofday]);
		}
		# don't go arround again if we have a good enough working stock of
		# candidates
		my $allowedmatches = grep {$_->{MATCHINGSCORE} >= $minallowedscore} @potentialcharges;
		last if $allowedmatches > @erarecords;
		# subtle difference: if you only have exactly enough "allowably good"
		# charges, then we should go a little broader and see if there are any
		# better looking charges... but if we have enough "really good" charges,
		# then we can assume that we wouldn't find any better ones by looking
		# more broadly.
		my $goodmatches = grep {$_->{MATCHINGSCORE} >= $mingoodscore} @potentialcharges;
		last if $goodmatches >= @erarecords;
	}

	# sort them
	@potentialcharges = SortBy [
		# by score, numerically, descending
		['MATCHINGSCORE','#','-'],
		# also by amount desc and id asc, to help any tiebreaker logic choose
		# the largest or first charge
		['AMOUNT','#','-'],
		['ID','#','+'],
	], @potentialcharges;

	# if we have a CLAIMLEVELREMIT, group potential charges by CLAIMID/TRANSFERTYPE/PATIENTINSURANCEID
	if (@erarecords == 1 && $erarecords[0]->{ACTION} eq 'CLAIMLEVELREMIT') {
		my @groupcols = qw(CLAIMID TRANSFERTYPE ACTION PATIENTINSURANCEID);
		my @groupedcharges = GroupBy(\@groupcols, @potentialcharges);

		@potentialcharges = sort { $b->{MATCHINGSCORE} <=> $a->{MATCHINGSCORE} } map {
			my $chargegroup     = $_;
			my $firstcharge     = (SortBy([['ID', '#',]], @$chargegroup))[0];
			my $matchingresults = $firstcharge->{MATCHINGRESULTS};

			if (@$chargegroup == 1) {
				$firstcharge->{ACTION} = '';                # claim has only one charge
			}
			else {
				$firstcharge->{ACTION} = 'CLAIMLEVELREMIT';	# claim has multiple charges

				if ( exists($matchingresults->{PROCEDURECODE}) ) {
					my $procedurecodescore = $matchingresults->{PROCEDURECODE}->{SCORE};
					my $procedurecode      = join ' + ', map { $_->{PROCEDURECODE} } @$chargegroup;
					$matchingresults->{PROCEDURECODE}->{SCORE} = int($matchingresults->{PROCEDURECODE}->{POSSIBLESCORE} / 2);
					$matchingresults->{PROCEDURECODE}->{DESCRIPTION} = 'Claim level remittance has no procedurecode';
					$firstcharge->{PROCEDURECODE}  = $procedurecode;
					$firstcharge->{MATCHINGSCORE} += $matchingresults->{PROCEDURECODE}->{SCORE} - $procedurecodescore;
				}

				if ( exists($matchingresults->{AMOUNT}) ) {
					my $amountscore = $matchingresults->{AMOUNT}->{SCORE};
					my $amount      = SumCurrency(map { $_->{AMOUNT} } @$chargegroup);
					if ($amount == $erarecords[0]->{AMOUNT}) {
						$matchingresults->{AMOUNT}->{SCORE} = $matchingresults->{AMOUNT}->{POSSIBLESCORE};
						$matchingresults->{AMOUNT}->{DESCRIPTION} = 'Billed amount matches.';
					}
					else {
						$matchingresults->{AMOUNT}->{SCORE} = 0;
						$matchingresults->{AMOUNT}->{DESCRIPTION} = 'Billed amount does not match.';
					}
					$firstcharge->{AMOUNT}         = $amount;
					$firstcharge->{MATCHINGSCORE} += $matchingresults->{AMOUNT}->{SCORE} - $amountscore;
				}

				if ( exists($matchingresults->{OUTSTANDING}) ) {
					my $outstandingscore = $matchingresults->{OUTSTANDING}->{SCORE};
					my $outstanding      = SumCurrency(map { $_->{OUTSTANDING} } @$chargegroup);
					if ($outstanding >= $erarecords[0]->{PAYMENT}) {
						$matchingresults->{OUTSTANDING}->{SCORE} = $matchingresults->{OUTSTANDING}->{POSSIBLESCORE};
						$matchingresults->{OUTSTANDING}->{DESCRIPTION} = 'Claim would not be overpaid';
					}
					else {
						$matchingresults->{OUTSTANDING}->{SCORE} = 0;
						$matchingresults->{OUTSTANDING}->{DESCRIPTION} = 'Claim would be overpaid';
					}
					$firstcharge->{OUTSTANDING}    = $outstanding;
					$firstcharge->{MATCHINGSCORE} += $matchingresults->{OUTSTANDING}->{SCORE} - $outstandingscore;
				}
			}

			$firstcharge;		# replace each group of charges with firstcharge (after modification above)
		} @groupedcharges;
	}
	$matchlog->{POTENTIALMATCHES} = \@potentialcharges if $matchlog;
	return @potentialcharges;
}


#########################################################################################
# GetPotentialMatchingChargesByScope
#
# Description:
#	-Performs a search of variable (specified) breadth for charges which might
#		match an erarecord
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		SCOPE       how broad / what type of query should be run?
#		ERABATCH	hashref
#		ERARECORD	hashref
#		CONTEXTID	look in this context for matching charges
#		(situational)
#		CUSTOMFILTER (required when the scope is 'CUSTOM')
#					a SQL object which, if passed, will be the basis of the
#					search, rather than the default iterative broadening search.
#					This is used, e.g., in the manual search form, wherein a
#					user is supplying a filter
#		(optional)
#		UNSPECIFIEDREMITPAYOR
#					(boolean) Include a dummy patientinsurance in the results
#					Currently passed only by MatchingFormCharges on behalf
#					of billing/eramatchcharge.esp (manual matching).
#		AUTOMATESELFPAYCHARGESINSURANCEPACKAGES
#					(listref) Allow matches to charges on self-pay claims only
#					where the primary policy is for one of these packages.
#					Currently passed only by GetMatchingCharges (auto-
#					matching) and only for Minute-Clinic contexts.
#
# Return Value:
#	array of charge hashrefs
#########################################################################################
sub GetPotentialMatchingChargesByScope {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORD ERABATCH SCOPE CONTEXTID )]);
	AssertValidFields($args,    [qw( ERARECORD ERABATCH SCOPE CONTEXTID
		CUSTOMFILTER
		UNSPECIFIEDREMITPAYOR
		AUTOMATESELFPAYCHARGESINSURANCEPACKAGES
	)]);
	
	my $contextid = $args->{CONTEXTID};
	my $erarecord = $args->{ERARECORD};
	my $scope = $args->{SCOPE};
	my $unspecifiedremitpayor = $args->{UNSPECIFIEDREMITPAYOR};
	my @automateselfpaychargesinsurancepackages;
	@automateselfpaychargesinsurancepackages = (ref $args->{AUTOMATESELFPAYCHARGESINSURANCEPACKAGES} eq 'ARRAY') ?
		@{$args->{AUTOMATESELFPAYCHARGESINSURANCEPACKAGES}} : ();

	my %defaultconf = (
                enabled => 0,
                contexts => [],
        );
	my $wrapclaimid;
	my $practiceid = Athena::Util::Database::SessionInfo($dbh)->{context};
        my $confdata = Athena::Conf::AthenaNet::AthenaXConf()->get("rollout.colpci.MEDICAIDKY_WRAP_CLAIM_MATCHING") // \%defaultconf;
        my $confcontexts = (defined $confdata->{contexts}) ? $confdata->{contexts} : $defaultconf{contexts};
        my $wrapclaimconfvalue = (defined $confdata->{enabled}) ? $confdata->{enabled} : $defaultconf{enabled};
	if( $wrapclaimconfvalue && InList($practiceid, @{$confcontexts})) {
		$wrapclaimid = $self->GetWrapClaimidToSearch($dbh, {
			ERARECORD => $args->{ERARECORD},
		});
	}
	my $sql = SQL::Select->new(
	)->Select(
		'distinct charge.*',
		"'$contextid' contextid",
		(map { "patient.$_ patient$_" } qw(
			LASTNAME
			FIRSTNAME
			MIDDLEINITIAL
			PREVIOUSLASTNAME
			DOB
			SSN
		)),
		# TRY.* namespace these selected fields so they don't clash with names
		# of charge columns, rename in result-set.  this is sadly easier than
		# spelling out and aliasing all the charge columns
		'transfertype.transfertype trytransfertype',
		'patientinsurance.id trypatientinsuranceid',
		'patientinsurance.sequencenumber patientinsurancesequencenumber',
		'patientinsurance.insuranceoverridename',
		'patientinsurance.cancelled patientinsurancecancelled',
		'patientinsurance.notes patientinsurancenotes',
		'patientinsurance.sourceerarecordid patientinsurancesourceerarec',
		'patientinsurance.idnumber trypatientinsuranceidnumber',
		'patientinsurance.insurancepackageid tryinsurancepackageid',
		'insurancepackage.name tryinsurancepackagename',
		"decode(transfertype.transfertype, '1', claim.primarypatientinsuranceid, '2', claim.secondarypatientinsuranceid) currentpatientinsuranceid",
		"nvl(decode(transfertype.transfertype, '1', claim.outstanding1, '2', claim.outstanding2),0) claimoutstanding",
	)->From(
		'<!transaction!> charge',
		'<!client!> patient',
		'<!claim!>',
		'<!patientinsurance!>',
		'<!insurancepackage!>',
		# Yes this is deliberately a cartesian product accross transfertypes,
		# even if there are no charges (currently) to that transfertype... this
		# is necessary to account for crossovers
		"(select 1 transfertype from dual union all select 2 transfertype from dual) transfertype",
	)->Joins(
		'charge.patientid = patient.id',
		'charge.claimid = claim.id',
		# Yes, this is deliberately the cartesian product of charges and patient
		# insurance policies (within a patient)!  They get winowed down, later
		'patientinsurance.patientid = patient.id',
		'patientinsurance.insurancepackageid = insurancepackage.id',
	)->Where(
		"charge.type = 'CHARGE'",
		"charge.voidparentid is null",
		["patientinsurance.insurancepackageid <> ?", $Insurance::PAYMENTPLANPACKAGEID],
		# protect ourselves from the pathological case of "visit-specific
		# policies", policies which are repeatedly created and cancelled
		"(
			claim.primarypatientinsuranceid = patientinsurance.id
			or claim.secondarypatientinsuranceid = patientinsurance.id
			or exists (
				select 1
				from
					claimaudit
				where
					claimaudit.claimid = claim.id
					and claimaudit.fieldname in ('PRIMARYPATIENTINSURANCEID', 'SECONDARYPATIENTINSURANCEID')
					and claimaudit.oldvalue = to_char(patientinsurance.id)
			)
			or patientinsurance.cancelled is null
			or not exists (
				select /*+ index(morerecentsimilarpolicy patientinsurance_patientid) */ 1
				from
					patientinsurance morerecentsimilarpolicy
				where
					morerecentsimilarpolicy.patientid = patient.id
					and patientinsurance.insurancepackageid = morerecentsimilarpolicy.insurancepackageid
					and patientinsurance.patientid = morerecentsimilarpolicy.patientid
					and claim.patientid = morerecentsimilarpolicy.patientid
					and patientinsurance.idnumber = morerecentsimilarpolicy.idnumber
					and (
						morerecentsimilarpolicy.cancelled is null
						or morerecentsimilarpolicy.cancelled > patientinsurance.cancelled
					)
					and nvl(patientinsurance.insuranceoverridename,'x') = nvl(morerecentsimilarpolicy.insuranceoverridename, 'x')
			)
		)",
	);

	if(GetTablespaceValueWithDefault($dbh, { KEY => 'Provider-Group-Based Data Permissions' }) eq 'ON') {
		$sql->Where(
			# Do not automatch records to restricted provider groups
			"patient.providergroupid not in (select id from providergroup where restrictedyn = 'Y')",
		);
	}
	if ($scope eq 'CUSTOM') {
		AssertRequiredFields($args, [qw( CUSTOMFILTER )]);
		$sql->Where(
			$args->{CUSTOMFILTER},
		);
	}
	elsif ($scope eq 'EXACT') {
		# "exact" scope limits the charges to only those which match the claimid
		# and only for the current payor
		# If we want the record to match to 18500 (when MATCHTOUNSPEC is set),
		# Lets allow patientinsurances other than the primary/secondary on claim also
		# in the result even for the EXACT scope.
		return () if $erarecord->{CLAIMID} eq '';
		unless ($erarecord->{MATCHTOUNSPEC}) {
			$sql->Joins(
				"decode(transfertype.transfertype, '1', claim.primarypatientinsuranceid, '2', claim.secondarypatientinsuranceid) = patientinsurance.id",
			);
		}
		if($wrapclaimid) {
			$sql->Where(
				['claim.id in (??)', [$erarecord->{CLAIMID}, $wrapclaimid]],
				"patientinsurance.insurancepackageid > 0",
			);
		}
		else {
			$sql->Where(
				['claim.id = ?', $erarecord->{CLAIMID}],
				"patientinsurance.insurancepackageid > 0",
			);
		}
	}
	elsif ($scope eq 'NARROW') {
		# "narrow" scope limits the charges to only those charges for the same
		# claim as the controlnumber indicates, and only for the payors which
		# have been on those claims or were currrent around the date of service
		# (the point being: just don't haul in every policy the patient's ever
		# had... that list can get really long in practices which have been
		# around for a while).
		return () if $erarecord->{CLAIMID} eq '';
		$sql->Where(
			["(
				claim.primarypatientinsuranceid = patientinsurance.id
				or claim.secondarypatientinsuranceid = patientinsurance.id
				or exists (
					select 1
					from
						claimaudit
					where
						claimaudit.claimid = claim.id
						and claimaudit.fieldname in ('PRIMARYPATIENTINSURANCEID', 'SECONDARYPATIENTINSURANCEID')
						and claimaudit.oldvalue = to_char(patientinsurance.id)
				)
				or patientinsurance.cancelled is null
				or patientinsurance.cancelled >= nvl(to_date(?), sysdate) - 180
				or patientinsurance.insurancepackageid = ?
				or patientinsurance.idnumber in ( ?? )
			)", $erarecord->{FROMDATE}, $erarecord->{INSURANCEPACKAGEID}, [ $erarecord->{PATIENTINSURANCEIDNUMBER}, $erarecord->{SUBSCRIBERINSURANCEIDNUMBER}, $erarecord->{CORRECTEDINSURANCEIDNUMBER} ]],
			"patientinsurance.insurancepackageid > 0",
			["patientinsurance.insurancepackageid <> ?", $Insurance::PAYMENTPLANPACKAGEID],
		);
		if($wrapclaimid) {
			$sql->Where(
				['claim.id in (??)', [$erarecord->{CLAIMID}, $wrapclaimid]],
			);
		}
		else {
			$sql->Where(
				['claim.id = ?', $erarecord->{CLAIMID}],
			);
		}
		#$unspecifiedremitpayor = 1;
	}
	else {
		# otherwise launch a much broader (and probably much slower) query.
		# there's gonna be a crapload of "or"s in here... which makes me wonder
		# if I should just trust oracle to be really smart about turning those
		# into unions (so that it can utilize indices), or if I should just be
		# clever and do my own management of creating unions...

		# ... stuff like procedurecode and servicedate and patient name
		my @filtersets = $self->GetChargeMatchingBroadQueryFilterSets($dbh, {
			ERARECORD => $erarecord,
			ERABATCH => $args->{ERABATCH},
			CONTEXTID => $contextid,
		});
		return () unless @filtersets;
		$sql->Where(
			@filtersets,
		);

		if ($scope eq 'CLAIMPATIENT') {
			return () if $erarecord->{CLAIMID} eq '';
			$sql->From(
				'claim patientclaim',
			)->Joins(
				'patientclaim.patientid = patient.id and patientclaim.patientid = claim.patientid',
			)->Where(
				['patientclaim.id = ?', $erarecord->{CLAIMID}],
				"patientinsurance.insurancepackageid > 0",
			);
		}
		elsif ($scope eq 'POLICY') {
			return () if (($erarecord->{PATIENTINSURANCEIDNUMBER} eq '') && ($erarecord->{SUBSCRIBERINSURANCEIDNUMBER} eq '') && ($erarecord->{CORRECTEDINSURANCEIDNUMBER} eq ''));
			$sql->Where(
				["patientinsurance.idnumber in ( ?? )", [$erarecord->{PATIENTINSURANCEIDNUMBER}, $erarecord->{SUBSCRIBERINSURANCEIDNUMBER}, $erarecord->{CORRECTEDINSURANCEIDNUMBER}] ],
				"patientinsurance.insurancepackageid > 0",
			);
		}
		elsif ($scope eq 'LASTNAME') {
			# Sometimes payer swaps the patient firstname and lastname in adjudication.
			# Firstname and lastname of the records are swapped and matched to see for
			# possible matches. Also, sometimes payer strips the suffixes (II,III,IV,JR,SR)
			# of lastname. The suffixes are stripped (both in patient and in erarecord)
			# before the matching
			my $strippedlastname = $erarecord->{PATIENTLASTNAME};
			my $swappedlastname = $erarecord->{PATIENTFIRSTNAME};
			$strippedlastname =~ s/\b(II|III|IV|JR|SR)\W*$//g;
			$strippedlastname =~ s/[-(),.' ]//g;
			$swappedlastname =~ s/\b(II|III|IV|JR|SR)\W*$//g;
			$swappedlastname =~ s/[-(),.' ]//g;
			return () if $strippedlastname eq '';

			$sql->Where(
				# this transformation on patient lastname is stored as a
				# function-based-index... so this is actually an indexable search
				["(
					(
						translate(patient.lastname, 'A(),.''- ' , 'A' ) = (?)				
						and translate(patient.firstname, 'A(),.''- ' , 'A' ) = (?)
					)		
					or (
						translate(patient.firstname, 'A(),.''- ' , 'A' ) = (?)		
						and translate(patient.lastname, 'A(),.''- ' , 'A' ) = (?)
					)	
				)" , $strippedlastname, $swappedlastname , $strippedlastname,$swappedlastname],
				# Two parts:
				# -any regular payor claim x the patient's payor policies
				# -any self-pay claim x its current self-pay policy
				# Wait until this scope because, for the self-pay cases we
				# want to handle this rev, we will generally not have an
				# erarecord.claimid.
				[q{
					patientinsurance.insurancepackageid > 0
					or (
						claim.primarypatientinsuranceid = patientinsurance.id
						and patientinsurance.insurancepackageid in (??)
						and transfertype.transfertype = '1'
						and charge.transfertype = 'p'
					)
				}, \@automateselfpaychargesinsurancepackages],
			);
		}
		elsif ($scope eq 'BROAD') {
			if ($erarecord->{PATIENTFIRSTNAME} =~ /^[A-Z]$/i) {
				$sql->Where(
					["upper(substr(patient.firstname, 0, 1)) = upper(?)", $erarecord->{PATIENTFIRSTNAME}],
				);
			}
			else {
				$sql->Where(
					["soundex(patient.firstname) = soundex(?)", $erarecord->{PATIENTFIRSTNAME}],
				);
			}
			$sql->Where(
				["soundex(translate(patient.lastname,'A(),.''- ','A')) = soundex(?)", $erarecord->{PATIENTLASTNAME} ],
				"patientinsurance.insurancepackageid > 0",
			);
		}
		#$unspecifiedremitpayor = 1;
	}
	# If we want the record to match to a 18500 insurance package on secondary, lets remove all charges with transfertype 1
	# and set the flag SKIPCHARGEYN to denote if this patientinsurance is the primary on the claim.
	if ($erarecord->{MATCHTOUNSPEC}) {
		$sql->Select(
			"(case claim.primarypatientinsuranceid when patientinsurance.id then 'Y' else 'N' end) SKIPCHARGEYN",
		)->Where(
			"transfertype.transfertype <> '1'"
		);
	}
	my ($sqltext, @binds) = $sql->ToSQLAndBinds();
	my @charges = SQLTableHash({
		SQL => $sqltext,
		CONTEXTID => $contextid,
	}, $dbh, @binds);
	# Populating Secondary Billed amount for charges that were scoped. We want to try matching them to the amount in erarecord
	# if the billed amount doesn't match as per H#217978
	my @chargeids = UniqueElements(map { $_->{ID} } @charges);
	my @secondarybilledamounts = SQL::Select->new(
		)->Select(
			"parentchargeid chargeid",
			"sum(amount) secondarybilledamount",
		)->From(
			"transaction",
		)->Where(
			"type = 'TRANSFERIN'",
			"voided is null",
			"transfertype = '2'",
		)->GroupBy(
			"parentchargeid",
		)->TableHashInChunks($dbh, {
			EXPRESSION => "parentchargeid",
			VALUES     => \@chargeids,
	});

	#Populate SECONDARYBILLEDAMOUNT for the potential charges
	foreach my $charge (@charges) {
		my ($secondarybilledamount) = map { $_->{SECONDARYBILLEDAMOUNT} } grep { $_->{CHARGEID} == $charge->{ID} } @secondarybilledamounts;
		$charge->{SECONDARYBILLEDAMOUNT} = $secondarybilledamount || 0;
	}

	# swap names of TRY.* and CHARGE.* fields.  See aliasing of columns in query
	foreach my $key (sort map {/^TRY(.*)/ ? $1 : ()} keys %{$charges[0] || {}}) {
		foreach my $charge (@charges) {
			($charge->{$key}, $charge->{"CHARGE$key"}) = ($charge->{"TRY$key"}, $charge->{$key});
		}
	}
	# If there is an existing 18500 on the claim, which we have found, lets not create a new one.
	my @existingunspec = grep {( $_->{PATIENTINSURANCENOTES} =~ /^created new unspecified policy for claim $_->{CLAIMID}V$contextid in erabatch/)
					&& ($_->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID)
					&& ($_->{TRANSFERTYPE} eq '2') } @charges;
	
	# This is essentially saying "what if we were to create a new
	# patientinsurance policy to match the payor?".  We don't try to do it in
	# the query (by, say, replacing patientinsurance with "(select from
	# patientinsurance UNION select <dummy values> from dual)" or similar)
	# because it would be pretty hard, and almost certainly really slow.

	# We do this in three cases:
	if (
		# -if the calling code has asked for it explicitly using arg
		#  UNSPECIFIEDREMITPAYOR (currently, this is manual matching, where
		#  MatchingFormCharges has called GetPotentialMatchingCharges with a
		#  CUSTOMFILTER on behalf of eramatchcharge.esp in order to produce
		#  this "none of the above" menu option)
		$unspecifiedremitpayor

		# -automatching as an unexpected primary on a self-pay claim where the
		#  current primary patient insurance package is on the practice's list
		#  (GetMatchingCharges has called GetPotentialMatchingCharges with a
		#  AUTOMATESELFPAYCHARGESINSURANCEPACKAGES arg; we didn't look for these
		#  until the LASTNAME scope, because:
		#  -all earlier scopes require a claimid
		#  -for the typical Minute-Clinic selfpay case (the only one we are
		#   addressing so far), we do not expect to have one)
		|| @automateselfpaychargesinsurancepackages && $scope eq 'LASTNAME'

		# If the MATCHTOUNSPEC flag is set, it means we want this record to match
		# to a 18500 policy on the secondary. Lets add the new unspecified policy
		# to the list of charges when it is set.
		|| ($erarecord->{MATCHTOUNSPEC} && !@existingunspec)

	) {
		my @chargetransfertypes = GroupBy(['PARENTCHARGEID', 'TRANSFERTYPE'], @charges);
		foreach my $chargetransfertype (@chargetransfertypes) {

			# If we are here via the LASTNAME scope, we add this option only
			# under certain circumstances (this rev is a small experiment):
			if (@automateselfpaychargesinsurancepackages && $scope eq 'LASTNAME') {

				# Look through the policies in this @$chargetransfertype.
				my @insurancepackageids = map {$_->{INSURANCEPACKAGEID}} @$chargetransfertype;

				# If there is a self-pay policy among them, it is the
				# current policy on this self-pay charge - and an
				# insurance package for which doing extra automation for
				# matching and posting has been approved.
				next unless grep {$_ < 0} @insurancepackageids;
				Assert(
					grep {InList($_, @automateselfpaychargesinsurancepackages)} @insurancepackageids,
					"Got here with packages (" . join(', ', @insurancepackageids) . ") - but expected to see a package from (" . join(', ', @automateselfpaychargesinsurancepackages) . ")",
				);
				Assert(
					$chargetransfertype->[0]{TRANSFERTYPE} eq '1' && $chargetransfertype->[0]{CHARGETRANSFERTYPE} eq 'p',
					"\$chargetransfertype->[0]{TRANSFERTYPE} was '$chargetransfertype->[0]{TRANSFERTYPE}' vs 1 and \$chargetransfertype->[0]{CHARGETRANSFERTYPE} was '$chargetransfertype->[0]{CHARGETRANSFERTYPE}' vs 'p'",
				);

				# If we have already created a "new uns payor" policy for
				# this claim based on an already-processed erarecord in
				# this batch, then we do not need to create another one
				# now.
				# Recognize it by its patientinsurance.patientinsurancenotes:
				next if grep {
					$_->{PATIENTINSURANCENOTES} eq
					"created apropos of remittance for claim $_->{CLAIMID}V$contextid in erabatch $args->{ERABATCH}{ID}"
				} @{$chargetransfertype};

				# Some clients may want us to go ahead and autocreate a
				# "new uns payor" policy only in the case where the
				# patient has no other old payor policies.  That would
				# mean having some sort of "next if" right here.
			}

			# copy a charge from the group
			my $unspecifiedremitpayorcharge = {%{$chargetransfertype->[0]}};
			# and dummy it up all "unspecified remit payor"-style
			$unspecifiedremitpayorcharge->{PATIENTINSURANCEID} = '';
			$unspecifiedremitpayorcharge->{PATIENTINSURANCEIDNUMBER} = '';
			$unspecifiedremitpayorcharge->{PATIENTINSURANCECANCELLED} = Today();
			$unspecifiedremitpayorcharge->{INSURANCEPACKAGEID} = $Insurance::UNSPECIFIEDREMITPACKAGEID;
			$unspecifiedremitpayorcharge->{INSURANCEPACKAGENAME} = "New Unspecified Payor";
			$unspecifiedremitpayorcharge->{SEQUENCENUMBER} = $unspecifiedremitpayorcharge->{TRANSFERTYPE};
			push @charges, $unspecifiedremitpayorcharge;
		}
	}

	# For these two scopes, we may have allowed patientinsurance rows with
	# negative insurancepackageid (self-pay policies).  But we don't want
	# those in the final results.  (We have sort of replaced them with a
	# "New Unspecified Payor" option.)
	@charges = grep {$_->{INSURANCEPACKAGEID} > 0} @charges if InList($scope, qw( CUSTOM LASTNAME ));

	if ($erarecord->{MATCHTOUNSPEC}) {
		# SKIPCHARGEYN would be set to Y for all charges whose PATIENTINSURANCEID is currently the claim.primarypatientinsuranceid
		# Lets remove these charges from @charges, as we do not want the record to match to the primary on claim.
		@charges = grep { !(($_->{INSURANCEPACKAGEID} != $Insurance::UNSPECIFIEDREMITPACKAGEID) && ($_->{SKIPCHARGEYN} eq 'Y'))} @charges;
	} else {
		# Exclude any policies that was created for the secondary claim which has a primary in the same batch. See H171702.
		@charges = grep { !(($_->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID) &&
					($_->{PATIENTINSURANCENOTES} =~ /^created new unspecified policy for claim $_->{CLAIMID}V$contextid in erabatch/)) } @charges;
	}
	return @charges;
}

#########################################################################################
# GetWrapClaimidToSearch
#
# Description:
#       -Finds the siblling claimid of a wrap claim - CRZS-2573
#
#
# Parameters:
#       $dbh:  application databse handle object
#       $args: hashref of arguments:
#               ERARECORD     Erarecord hash to matchto
# Return Value:
#       wrap claimid
#########################################################################################
sub GetWrapClaimidToSearch {
	return ;
}


#########################################################################################
# GetChargeMatchingBroadQueryFilterSets
#
# Description:
#	-given an erarecord against which to match, figures out a set of filters for
#		the broad potential-charges query.
#	-the idea here is to come up with a set of filters that will bring in most
#		(ideally all) charges which *might* pass the bar for matching... but
#		without bringing in *so* many charges which won't pass the bar or be
#		*so* slow that we kill the system on every broad query
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCH	hashref
#		ERARECORD	hashref
#		CONTEXTID	look in this context for matching charges
#
# Return Value:
#	array of filters
#########################################################################################
sub GetChargeMatchingBroadQueryFilterSets {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORD ERABATCH CONTEXTID )]);
	AssertValidFields($args,    [qw( ERARECORD ERABATCH CONTEXTID )]);
	
	my $erarecord = $args->{ERARECORD};
	my $fromdate = $erarecord->{FROMDATE};
	my $procedurecode = (split(/,/, nvl($erarecord->{SUBMITTEDPROCEDURECODE}, $erarecord->{PROCEDURECODE})))[0];

	# there's filters that loosely couple to a charge, and those that loosely couple to a patient...
	my $sql;
	if ($procedurecode && $fromdate) {
		my @dateclauses = (
			["charge.fromdate between to_date(?)-30 and to_date(?)+30", ($fromdate) x 2],
			["charge.created between to_date(?)-30 and to_date(?)+30", ($fromdate) x 2],
			["to_char(charge.fromdate, 'mm/dd') = to_char(to_date(?), 'mm/dd')", $fromdate],
		);
		my @procedureclauses = (
			["charge.fromdate = ?", $fromdate],
			["charge.procedurecode like ? || '%'", $procedurecode],
			["charge.printprocedurecode like ? || '%'", $procedurecode],
			["exists (
				select 1 from <!chargenote!>
				where
					chargenote.chargeid = charge.id
					and fieldname in ('PROCEDURECODE', 'PRINTPROCEDURECODE')
					and oldvalue like ? || '%')", $procedurecode],
		);
		$sql = SQL->And(
			SQL->Or(@dateclauses),
			SQL->Or(@procedureclauses),
		);
	}
	elsif ($fromdate) {
		$sql = SQL->new(["charge.fromdate = ?", $fromdate]);
	}

	# if either patient or charge has NO restrictions, at all, then we can't
	# really run the query, and have to turn the filters into 1=0
	return defined $sql ? $sql : ();
}

#########################################################################################
# GetMinimumAllowedChargeMatchingScoreDifference
#
# Description:
#	-defines a parameter of the matching algorithm: how close must two different
#		charges' scores be to be considered a tie (i.e. an ambiguous match)
#
# Return Value:
#	number (score difference)
#########################################################################################
sub GetMinimumAllowedChargeMatchingScoreDifference { 1 }

#########################################################################################
# GetMinimumGoodChargeMatchingScore
#
# Description:
#	-defines a parameter of the matching algorithm: a charge whith this score
#		(or higher) is basically guaranteed to be a good match
#
# Return Value:
#	number (score)
#########################################################################################
sub GetMinimumGoodChargeMatchingScore { 93 }


#########################################################################################
# GetMinimumAllowedChargeMatchingScore
#
# Description:
#	-defines a parameter of the matching algorithm: below what score is a charge
#		considered too bad of a match, even if it is the BEST match ("Not even
#		if you were the last charge on Earth, honey.")
#
# Return Value:
#	number (score)
#########################################################################################
sub GetMinimumAllowedChargeMatchingScore { 75 }


#########################################################################################
# TieBreakAmongAmbiguousMatches
#
# Description:
#	-can be used to break ties between matching charges.
#
# Return Value:
#	'Y' - can break ties between charges
#	'N' - should not break ties
#########################################################################################
sub TieBreakAmongAmbiguousMatches {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args,['ERARECORDS', 'POTENTIALCHARGES', 'MATCHINGCHARGES']);

	my @erarecords = @{$args->{ERARECORDS}};
	my @potentialcharges = @{$args->{POTENTIALCHARGES}};
	my @matchingcharges = @{$args->{MATCHINGCHARGES}};
	my $worstwinner = $matchingcharges[-1];
	my $mindifference = $self->GetMinimumAllowedChargeMatchingScoreDifference($dbh);
	my @remitcols = grep { $_ ne 'INTEREST' } @ERA::RecordRemitCols ;

	my @remitrecords;

	# If the ambiguous match is between charges with 18500 ins package, lets go ahead and break the tie as long as the
	# charges are for the same claim and transfertype.

	if ($worstwinner->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID) {
		my @matchingchargesdiffclaims = grep { (($worstwinner->{MATCHINGSCORE} - $_->{MATCHINGSCORE}) < $mindifference ) &&
			(($worstwinner->{CLAIMID} ne "$_->{CLAIMID}") ||
			 ($worstwinner->{TRANSFERTYPE} ne "$_->{TRANSFERTYPE}") ||
			 ($worstwinner->{INSURANCEPACKAGEID} ne "$_->{INSURANCEPACKAGEID}"))
		} (@potentialcharges,@matchingcharges);
		unless (@matchingchargesdiffclaims) {
			return {
				RESULT => 'Y',
				DESC => '',
			};
		}
	}
	if (
		(@erarecords == 1) &&
		# if this record was marked for breaking the tie.
		($erarecords[0]->{BREAKTIE} == 1) &&
		# and has max scores for procedurecode with modifiers.
		($worstwinner->{MATCHINGRESULTS}->{PROCEDURECODEMODIFIER}->{SCORE} == $worstwinner->{MATCHINGRESULTS}->{PROCEDURECODEMODIFIER}->{POSSIBLESCORE})
	) {
		my @matchingchargesdiffclaims = grep { (($worstwinner->{MATCHINGSCORE} - $_->{MATCHINGSCORE}) < $mindifference ) &&
			(($worstwinner->{CLAIMID} ne "$_->{CLAIMID}") ||
			 ($worstwinner->{TRANSFERTYPE} ne "$_->{TRANSFERTYPE}") ||
			 ($worstwinner->{MATCHINGRESULTS}->{PROCEDURECODEMODIFIER}->{SCORE} != $_->{MATCHINGRESULTS}->{PROCEDURECODEMODIFIER}->{SCORE})
		 )
		} (@potentialcharges,@matchingcharges);
		unless (@matchingchargesdiffclaims) {
			return {
				RESULT => 'Y',
				DESC => '',
			};
		}
	}

	#Sometimes we receive erarecords which does not have any money or kick value associated to it, when we are trying to match 
	#such a record it often failes to match because of absence of billed amount or no procedure code and so on.
	#So such a record(zero remit record) can be matched to any one /more charge of that claim to avoid unnecessary manual touches.

	foreach my $record (@erarecords) {
		my $kicks = $record->{KICKS};

		if ((grep { abs($record->{$_}) > 0 } @remitcols) || (grep { abs($_->{KICKEDAMOUNT}) > 0 } @$kicks)) {
			push (@remitrecords, $record->{ID});
		}

	}
	if (!@remitrecords) {
		my @matchingchargesdiffclaims = grep { (($worstwinner->{MATCHINGSCORE} - $_->{MATCHINGSCORE}) < $mindifference ) &&
			(($worstwinner->{CLAIMID} ne "$_->{CLAIMID}") ||
			 ($worstwinner->{TRANSFERTYPE} ne "$_->{TRANSFERTYPE}") ||
			 ($worstwinner->{PATIENTINSURANCEID} ne "$_->{PATIENTINSURANCEID}"))
		} (@potentialcharges,@matchingcharges);
		if (@matchingchargesdiffclaims) {
			return {
				RESULT => 'N',
				DESC => "Ambiguous match / tie (minimum allowed score difference: $mindifference) across claims, transfertypes, and/or policies",
			};
		} else {
			return {
				RESULT => 'Y',
				DESC => '',
			};
		}
	}
	return {
		RESULT => 'N',
		DESC => "Ambiguous match / tie (minimum allowed score difference: $mindifference) - Ties not breakable"
	};

}

#########################################################################################
# GetObjectMatchingScore
#
# Description:
#	-given an erarecord against which to match, figures out a set of filters for
#		the broad potential-charges query.
#	-the idea here is to come up with a set of filters that will bring in most
#		(ideally all) charges which *might* pass the bar for matching... but
#		without bringing in *so* many charges which won't pass the bar or be
#		*so* slow that we kill the system on every broad query
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		CHARGE      hashref of charge being scored
#		ERARECORD	hashref of erarecord being scored against
#		ERABATCH	hashref
#		CONTEXTID	look in this context for matching charges
#		CACHE		hashref in which to memoize results of intermediate queries
#					and functon calls.  Since we may pull in a cartesian product
#					of charges, patientinsurances, and transfertypes, it's good
#					to be able to *not* repeat lookups and such against each
#					charge or patientinsurance etc (particularly for looking up
#					audit histories).
#
# Return Value:
#	hashref of {
#		MATCHINGSCORE => num,
#		MATCHINGRESULTS => {
#			$field => {
#				SCORE => num,
#				POSSIBLESCORE => num,
#				DESCRIPTION => "Foo matches / does not match blah blah",
#			}
#			...
#		}
#	}
#########################################################################################
sub GetObjectMatchingScore {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORD ERABATCH CONTEXTID CACHE )]);
	AssertValidFields($args,    [qw( ERARECORD ERABATCH CHARGE CLAIM CONTEXTID CACHE )]);
	Assert($args->{CHARGE} || $args->{CLAIM},"Either Charge Or Claim must be specified.");

	my $object = $args->{CHARGE} ? $args->{CHARGE} : $args->{CLAIM};
	my $erarecord = $args->{ERARECORD};

	# go through a list of relevant matching fields, each with a score
	# indicating its importance in establishing a match ("score components" --
	# see below), and compile a score based on which fields matched, and a data
	# structure detailing how the score was derived (for debugging and for
	# informing the manual match UI).
	my ($score, $possiblescore) = (0, 0);
	my $scorecomponents;
	if ($args->{CHARGE}) {
		$scorecomponents = $self->GetChargeMatchingScoringComponents($dbh);
	} elsif ($args->{CLAIM}) {
		$scorecomponents = $self->GetClaimMatchingScoringComponents($dbh);
	}
	my %matchingresults = ();
	foreach my $fieldname (sort keys %$scorecomponents) {
		my $scorecomponent = $scorecomponents->{$fieldname};
		next unless $scorecomponent;
		my $result;
		# if the score component provides a TEST sub, it will provide a score
		if ($scorecomponent->{TEST}) {
			$result = $scorecomponent->{TEST}->($self, $dbh, $args);
			next if $result->{SKIP};
		}
		# if no TEST sub is provided, just do a direct comparison
		else {
			my $erarecordfieldname = $scorecomponent->{ERARECORDFIELDNAME} || $fieldname;
			next if $erarecord->{$erarecordfieldname} eq '';
			$result = {
				SCORE => $erarecord->{$erarecordfieldname} eq $object->{$fieldname} ? $scorecomponent->{SCORE} : 0
			};
		}

		if ($result->{DESCRIPTION} eq '') {
			my $name = $scorecomponent->{NAME} || ucfirst lc $fieldname;
			$result->{DESCRIPTION} = $result->{SCORE}
				? "$name matches."
				: "$name does not match.";
		}
		if (!defined $result->{POSSIBLESCORE}) {
			$result->{POSSIBLESCORE} = nvl($scorecomponent->{SCORE}, $result->{SCORE});
		}
		# in the charge matching UI, with which charge field should this score
		# component result be shown?
		$result->{SHOWWITHFIELD} ||= $scorecomponent->{SHOWWITHFIELD} || $fieldname;

		$matchingresults{$fieldname} = $result;
		$score += $result->{SCORE};
		$possiblescore += $result->{POSSIBLESCORE};
	}
	#H595722 - FQHC/RHC T1015 Automation - override charge matching score
	if ($args->{CHARGE}) {
		$args->{MATCHINGRESULTS} = \%matchingresults;
		$self->OverrideChargeMatchingScore($dbh, $args);
		$score = 0;
		$score += $matchingresults{$_}->{SCORE} foreach keys %matchingresults;
	}

	return {
		MATCHINGSCORE => ($possiblescore ? int($score * 100 / $possiblescore) : 0),
#		MATCHINGSCORE => $score,
		MATCHINGRESULTS => \%matchingresults,
	};
}

#########################################################################################
# OverrideChargeMatchingScore
#
# Description:
#       #H595722 - FQHC/RHC T1015 Automation
#                -used to override charge matching score
#
#	#H637030 - Encounter Code Posting for Urgent Care Code S9083
#
# Return Value:
#       -no return value
#########################################################################################

sub OverrideChargeMatchingScore {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORD ERABATCH CONTEXTID CACHE MATCHINGRESULTS)]);

	my $erarecord = $args->{ERARECORD};
	my $charge = $args->{CHARGE};
	my $matchingresults = $args->{MATCHINGRESULTS};
	my $scorecomponents = $self->GetChargeMatchingScoringComponents($dbh);

	if($erarecord->{MCRWRONGPAYERFLAG} eq 'Y' && $matchingresults->{PAYOR}->{SCORE} < $scorecomponents->{PAYOR}->{SCORE} && $charge->{TRANSFERTYPE} eq '2') {
		# Since this is an MDP API call, if the API call fails for some reason, but the
		# kickcode gets created in 1.0. Created kickcode will not be aware by 2.0 and
		# the scoring algorithm would be tried again which will end up creating the 
		# kick again. This will help to prevent such cases.
		my $mcrkickadded = $self->_IsKickFiredBefore($dbh, {ERARECORD => $erarecord, KICKCODE => 'MCRCROVRUNMATCHED'});
		$self->_AddFakeKick($dbh, {
			ERARECORD => $erarecord,
			USERNAME => 'ATHENA',
			FAKEKICKCODE => 'MCRCROVRUNMATCHED',
		}) if (!$mcrkickadded);

		$matchingresults->{PAYOR}->{SCORE} = $scorecomponents->{PAYOR}->{SCORE};
	}

	#Updating TRANSFERTYPE score only for secondary dual eligible remit records.
	if( $erarecord->{DUALELIGIBLESECONDARYREMITFLAG} eq 'Y' ) {
		$matchingresults->{TRANSFERTYPE}->{SCORE} = $charge->{TRANSFERTYPE} eq '1' ? $scorecomponents->{TRANSFERTYPE}->{SCORE}  : 0;  
	}

	# Hydra: 747995 - Expand BCBS Routing to other payers
	my $insurancereportingcategoryname;
	my $basesql =  SQL::Select->new(
					)->Select(
						"insurancereportingcategory.name",
					)->From(
						"insurancepackage",
						"insurancereportingcategory",
					)->Joins(
						"insurancereportingcategory.id = insurancepackage.insurancereportingcategoryid",
					)->Where(
						["insurancepackage.id = ?", $charge->{INSURANCEPACKAGEID}],
						"insurancereportingcategory.deleted is null",
						"insurancepackage.deleted is null",
					);
	if (InList($erarecord->{KICKREASONCATEGORYID}, (904, 1226))) {
		$insurancereportingcategoryname = $basesql->Where(
					"insurancereportingcategory.name like 'Health Net%'",
					)->Values($dbh);
	}
	if (InList($erarecord->{KICKREASONCATEGORYID}, (651, 921, 1101, 1189, 1215, 2565, 5966))) {
		$insurancereportingcategoryname = $basesql->Where(
					"insurancereportingcategory.name like 'United%'",
					)->Values($dbh);
		}

	if ($insurancereportingcategoryname && $matchingresults->{PAYOR}->{SCORE} < $scorecomponents->{PAYOR}->{SCORE}) {
		$matchingresults->{PAYOR}->{SCORE} = $scorecomponents->{PAYOR}->{SCORE};
		# Hydra 1064438 - UHCREMIT should only fire once on ERA Record
		my $uhcremitkickfired = $self->_IsKickFiredBefore($dbh, {ERARECORD => $erarecord, KICKCODE => 'UHCREMIT'});
		if($insurancereportingcategoryname =~ /^United/ && !$uhcremitkickfired) {
			$self->_AddFakeKick($dbh, {
				ERARECORD => $erarecord,
				USERNAME => 'ATHENA',
				FAKEKICKCODE => 'UHCREMIT',
			});
		}
	}

	my ($t1015record, $s9083record);
	$t1015record = 1 if (substr($args->{ERARECORD}->{PROCEDURECODE}, 0, 5) eq 'T1015');
	$s9083record = 1 if (substr($args->{ERARECORD}->{PROCEDURECODE}, 0, 5) eq 'S9083');

	return unless ($t1015record || $s9083record);

	my @fieldstobeoverridden = ('AMOUNT','PROCEDURECODE');

	my $basesql = SQL::Select->new(
		)->Select(
			"1",
		)->From(
			"savedscrubdata",
		)->Where(
			["transactionid = ?", $charge->{ID}],
			"billingbatchid is not null",
			"deleted is null",
	);

	my $result;
	if ($t1015record) {
		$result = $basesql->Where(
			SQL->Or("fieldname in ('RHCEncounterCode','FQHCEncounterCode','XHCEncounterCode','ClonedAsMaster') and value like 'T1015%'",
				"fieldname in ('RHCAutoFormatting','FQHCAutoFormatting','XHCAutoFormatting') and value = '1'",
			),
		)->Values($dbh);
	}
	elsif ($s9083record) {
		$result = $basesql->Where(
			SQL->Or("fieldname in ('UCEncounterCode','ClonedAsMaster') and value like 'S9083%'",
				"fieldname in ('UCAutoFormatting') and value = '1'"
			),
		)->Values($dbh);
	}
	return unless $result;

	my @result = map{
		$matchingresults->{$_}->{SCORE} = $scorecomponents->{$_}->{SCORE};
	} grep {InList($_, @fieldstobeoverridden)} keys %$matchingresults;
}

#########################################################################################
# _IsKickFiredBefore
#
# Description:
#     If this is an MDP API call from 2.0 system, and the API call fails for some reason
#     then 2.0 will not be aware if the kick is already created in 1.0. The scoring api will
#     be re-tried and we would be creating the kick again for the same erarecord.
#     This check ensures if the kick is already created in 1.0 system during previous 
#     attempt, and if created adds the kick to response to sync it back to 2.0 system. 
#
# Return Value:
# 	non zero value if kick is already added to erarecord
# 	0 if kick is not added to erarecord
#
#########################################################################################
sub _IsKickFiredBefore {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORD KICKCODE )]);

	my $erarecord = $args->{ERARECORD};
	my $kickcode = $args->{KICKCODE};

	my $is_added_to_response = grep { $_->{KICKCODE} eq $kickcode } @{$erarecord->{ERAKICKENTITIES} || []};
	#	Below logic is used by Scoring algo to skip creation of MCRCROVRUNMATCHED and UHCREMIT
	#	kicks in 1.0 during match. Because after router elimination between import and match,
	#	No ERARECORD entry is created before match. Also it adds kick passed to the MDP API 
	#	response so that addition of the above kicks would be taken care after match by 2.0
	unless ($erarecord->{ID}) {
		if($ENV{NEWMATCHPROCESS} && !$is_added_to_response) {
			$erarecord->{ERAKICKENTITIES} ||= [];  # Initialize to empty array if undef
			push @{$erarecord->{ERAKICKENTITIES}}, {
				KICKCODE => $kickcode,
				KICKEDAMOUNT => 0
			};
			return 1;
		}
	}

	my $isaddedtoresponse = grep { $_->{KICKCODE} eq $kickcode } @{$erarecord->{KICKS} || []};
	my $newkickfired = { SQLHash("select * from erakick where kickcode = ? and erarecordid = ?", $dbh, $kickcode, $erarecord->{ID}) };
	if ($ENV{NEWMATCHPROCESS} && %$newkickfired && !$isaddedtoresponse) {
		push @{$erarecord->{KICKS} || []}, $newkickfired;
		return 1;
	}
	return %$newkickfired;
}

#########################################################################################
# GetChargeMatchingScoringComponents
#
# Description:
#	-The charge scoring code is based on the idea of running several individual
#		tests comparing a charge to an erarecord (or, in some cases, to itself
#		or to constants... but whatever), and combining the results of all of
#		those tests together.  This method actually fetches those tests.
#	-The tests, themselves, are basically little methods with a very strict
#		interface and a bit of meta-data
#	-Because the tests are very similar and there's a good deal of potential
#		redundency in all of the metadata, there's a lot of "defaulting" logic.
#		(This doesn't actually use Data::Inherit, but it wouldn't be a bad fit)
#
#	-Subclass engines which want to override the matching logic, should
#		basically overload this method to return a modified **COPY** of
#		self->SUPER::GetChargeMatchingScoringComponents().  With whatever
#		individual components differ replaced with new components, or new
#		components added, etc.
#
#	-RETURNS A REFERENCE.  DO NOT MODIFY IT'S CONTENTS!
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#
# Return Value:
#	hashref of {
#		$fieldname (name of field in charge hash, e.g. FOOBAR) => {  
#			NAME => english pretty name, default: Foobar
#			SCORE => number of points the field is worth,
#			ERARECORDFIELDNAME => name of field in erarecord hash, default: FOOBAR
#			SHOWWITHFIELD => name of field with which the results of this test
#				should be shown in the matching UI (e.g. PROCEDURECODEMODIFIER
#				shows with the PROCEDURECODE).  Default: FOOBAR
#			TEST => test subroutine, passed ($self, $dbh, $args), with args: {
#					ERARECORD =>
#					CHARGE =>
#					ERABATCH =>
#					CONTEXTID =>
#					CACHE =>
#				}
#				should return a hashref of {
#					SCORE => number, score achieved,
#					POSSIBLESCORE => number, default: the SCORE of the component
#					DESCRIPTION => string,
#						default: "Foobar does [not] match" (depending on whether SCORE = 0 or not)
#				}
#				The default TEST subroutien is something like:
#				sub {
#					my ($self, $dbh, $args) = @_;
#					my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
#					if ($erarecord->{$erarecordfieldname} eq $charge->{$fieldname) {
#						return { SCORE => $score, };
#					}
#					return { SCORE => 0, };
#				},
#			},
#		},
#		...
#	}
#########################################################################################
my $CHARGEMATCHINGSCORINGCOMPONENTS = {
	CLAIMID => {
		NAME => 'Controlnumber',
		SCORE => 10,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			unless ($erarecord->{CONTROLNUMBER} != 0) {
				return { SKIP => 1 };
			}
			# For self-pay charges, no surprise if controlnumber is junk.
			if (
				$charge->{CHARGETRANSFERTYPE} eq 'p'
				&& $erarecord->{CONTROLNUMBER} !~ /^\d+V\d+$/i
			) {
				return { SKIP => 1 };
			}
			my $chargecontrolnumber = $charge->{CLAIMID} . "V" . $args->{CONTEXTID};
			if ($erarecord->{CONTROLNUMBER} eq $chargecontrolnumber) {
				return { SCORE => 10, };
			}
			return { SCORE => 0, };
		},
	},
	
	PROVIDERCONTROLNUMBER => {
		NAME => 'Provider control number (REF.6R in ERA)',
		SCORE => 10,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			unless ($erarecord->{PROVIDERCONTROLNUMBER} =~ /^([\d]+)P$args->{CONTEXTID}$/) {
				return { SKIP => 1 };
			}
			my $chargeid = $1;
			if ($chargeid eq $charge->{ID}) {
				return { SCORE => 10, };
			}
			return { SCORE => 0, };
		},
	},

	PROCEDURECODE => {
		SCORE => 10,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			my $recordsubprocedurecode = (split(/,/, $erarecord->{SUBMITTEDPROCEDURECODE}))[0];
			my $recordprocedurecode = (split(/,/, $erarecord->{PROCEDURECODE}))[0];

			unless (($recordprocedurecode ne '') || ($recordsubprocedurecode ne '')) {
				return { SKIP => 1 };
			}
			my $revenuecode = $charge->{REVENUECODE};
			my $chargeprocedurecode = (split(/,/, $charge->{PROCEDURECODE}))[0];
			my $chargeprintprocedurecode = (split(/,/, $charge->{PRINTPROCEDURECODE}))[0];
			if (InList($chargeprocedurecode, ($recordprocedurecode, $recordsubprocedurecode))
				|| InList($chargeprintprocedurecode, ($recordprocedurecode, $recordsubprocedurecode))) {
				return {
					SCORE => 10,
				};
			}
			elsif (($revenuecode ne '') && ($recordprocedurecode =~ m/^(?:\w+:)?$revenuecode$/)) {
				return {
					SCORE => 10,
					DESCRIPTION => "The revenuecode on charge matches",
				};
			}
			else {
				if (($erarecord->{PROCEDURECODE} =~ /^N4:/) && ($erarecord->{SUBMITTEDPROCEDURECODE} eq '')) {
					my $ndcerarecord = (split(/:/, $erarecord->{PROCEDURECODE}))[1];
					if ($ndcerarecord eq $charge->{NDC}) {
						return {
							SCORE => 10,
							DESCRIPTION => "The NDC on charge matches",
						}
					}
				}
				my $chargeaudit = $args->{CACHE}->{CHARGEAUDITOLDVALUE}->{$charge->{ID}} ||= [SQLTableHash({
					SQL => "select distinct fieldname, oldvalue from chargenote where chargeid = ?",
					CONTEXTID => $args->{CONTEXTID},
				}, $dbh, $charge->{ID})];
				my @oldprocedurecodes = 
					map { (split(/,/, $_->{OLDVALUE}))[0] }
					grep { InList($_->{FIELDNAME}, qw( PROCEDURECODE PRINTPROCEDURECODE )) } @$chargeaudit;
				if (InList($recordprocedurecode, @oldprocedurecodes) || InList($recordsubprocedurecode, @oldprocedurecodes)) {
					return {
						SCORE => 8,
						DESCRIPTION => "A previous procedurecode matches.  See charge's audit history.",
					};
				}
			}
			return {
				SCORE => 0,
			};
		},
	},
	
	PROCEDURECODEMODIFIER => {
		NAME => 'Procedurecode with modifiers',
		SHOWWITHFIELD => 'PROCEDURECODE',
		SCORE => 5,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			my $recordprocedurecode = nvl($erarecord->{SUBMITTEDPROCEDURECODE}, $erarecord->{PROCEDURECODE});
			unless ($recordprocedurecode) {
				return { SKIP => 1 };
			}
			my $chargeprocedurecode = $charge->{PROCEDURECODE};
			my $chargefullprocedurecode = $charge->{PROCEDURECODE} . ($charge->{OTHERMODIFIER} ne '' ? ",$charge->{OTHERMODIFIER}" : "");
			my $chargeprintprocedurecode = $charge->{PRINTPROCEDURECODE};
			my ($recordproccodenomod,@recordmodifiers) = split /,/, $recordprocedurecode;
			my ($chargeproccodenomod,@chargeothermodifiers) = split /,/, $charge->{PROCEDURECODE};
			push @chargeothermodifiers, split /,/, $charge->{OTHERMODIFIER};
			my %modifierhash;
			foreach (@recordmodifiers) { $modifierhash{$_}++; };
			foreach (@chargeothermodifiers) { $modifierhash{$_}--; };

			if (($recordprocedurecode eq $chargeprocedurecode && $chargeprocedurecode =~ /,/)
				|| $recordprocedurecode eq $chargefullprocedurecode
				|| $recordprocedurecode eq $chargeprintprocedurecode
				|| ((@recordmodifiers == @chargeothermodifiers)
					&& ($recordproccodenomod eq $chargeproccodenomod)
					&& (!grep { $modifierhash{$_} != 0 } keys %modifierhash)
				)
			) {
				return {
					SCORE => 5,
				};
			}
			elsif (($chargefullprocedurecode =~ /^\Q$recordprocedurecode\E/ || $chargeprintprocedurecode =~ /^\Q$recordprocedurecode\E/)
				&& ($recordprocedurecode =~ /,/)) {
				return {
					SCORE => 4,
				};
			}
			else {
				my $chargeaudit = $args->{CACHE}->{CHARGEAUDITOLDVALUE}->{$charge->{ID}} ||= [SQLTableHash({
					SQL => "select distinct fieldname, oldvalue from chargenote where chargeid = ?",
					CONTEXTID => $args->{CONTEXTID},
				}, $dbh, $charge->{ID})];
				my @oldprocedurecodes = 
					map { $_->{OLDVALUE} }
					grep { $_->{FIELDNAME} eq 'PRINTPROCEDURECODE' } @$chargeaudit;
				if (InList($recordprocedurecode, @oldprocedurecodes)) {
					return {
						SCORE => 4,
						DESCRIPTION => "A previous procedurecode with modifiers matches.  See charge's audit history.",
					};
				}
			}
			return {
				SCORE => 0,
			};
		},
	},

	PATIENTINSURANCEIDNUMBER => {
		NAME => 'Policy member ID',
		SHOWWITHFIELD => 'PATIENTINSURANCEID',
		SCORE => 8,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			if (
				# old MCRCROVR logic
				$charge->{PATIENTINSURANCENOTES} =~ /Primary crovr1 indicates secondary expected/
				# new CROSSOVERPAYER logic
				|| (
					$charge->{PATIENTINSURANCESOURCEERAREC} ne ''
					&& $charge->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID
				)
			) {
				return { SKIP => 1 };
			}
			if ($erarecord->{MATCHTOUNSPEC} && ($charge->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID)) {
				return {
					SCORE => 8,
					DESCRIPTION => 'Matching secondary claim loop to New Unspecified Payer.',
				};
			}
			
			
			# Hydra 82512. Payers are allowed to send up to three different member identification numbers in ERA:
			# the patient's insurance id number, the insured/subscriber's id number, and patient's corrected number.
			# If either one matches exactly, return a full score. If either one matches loosely,
			# return the partial score of six points. If they all don't match, then return zero.

			my $chargepid = $charge->{PATIENTINSURANCEIDNUMBER};
			$chargepid =~ s/\W|_//g;
			my $recordpid = $erarecord->{PATIENTINSURANCEIDNUMBER};
			$recordpid =~ s/\W|_//g;
			my $subscriberid = $erarecord->{SUBSCRIBERINSURANCEIDNUMBER};
			$subscriberid =~ s/\W|_//g;
			my $correctedid = $erarecord->{CORRECTEDINSURANCEIDNUMBER};
			$correctedid =~ s/\W|_//g;

			if (($recordpid eq '') && ($subscriberid eq '') && ($correctedid eq '')) {
				return { SKIP => 1 };
			}
			if ($chargepid eq '') {
				return { SCORE => 0 };
			}
			if (uc($recordpid) eq uc($chargepid)) {
				return { SCORE => 8 };
			}
			if (uc($subscriberid) eq uc($chargepid)) {
				return { SCORE => 8 };
			}
			if (uc($correctedid) eq uc($chargepid)) {
				return { SCORE => 8 };
			}
			if ((length($chargepid) > 5) && ($recordpid =~ /\Q$chargepid\E/i)) {
				return { SCORE => 6, DESCRIPTION => 'Policy member ID matches loosely',};
			}
			if ((length($recordpid) > 5) && ($chargepid =~ /\Q$recordpid\E/i)) {
				return { SCORE => 6, DESCRIPTION => 'Policy member ID matches loosely',};
			}
			if ((length($chargepid) > 5) && ($subscriberid =~ /\Q$chargepid\E/i)) {
				return { SCORE => 6, DESCRIPTION => 'Policy member ID matches loosely',};
			}
			if ((length($subscriberid) > 5) && ($chargepid =~ /\Q$subscriberid\E/i)) {
				return { SCORE => 6, DESCRIPTION => 'Policy member ID matches loosely',};
			}
			if ((length($chargepid) > 5) && ($correctedid =~ /\Q$chargepid\E/i)) {
				return { SCORE => 6, DESCRIPTION => 'Policy member ID matches loosely',};
			}
			if ((length($correctedid) > 5) && ($chargepid =~ /\Q$correctedid\E/i)) {
				return { SCORE => 6, DESCRIPTION => 'Policy member ID matches loosely',};
			}
			else {
				return { SCORE => 0 };
			}
		},
	},

	PATIENTINSURANCESEQUENCENUMBER => {
		NAME => 'Sequence number (typical transfertype) of policy',
		SHOWWITHFIELD => 'PATIENTINSURANCEID',
		SCORE => 2,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			return {
				SCORE => ($charge->{TRANSFERTYPE} eq $charge->{PATIENTINSURANCESEQUENCENUMBER} ? 2 : 0),
			};
		},
	},

	PATIENTINSURANCEID => {
		NAME => 'Policy',
		SCORE => 10,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge, $erabatch) = @{$args}{qw( ERARECORD CHARGE ERABATCH )};

			# For a self-pay charge, the patient insurance on the claim is
			# currently some self-pay policy - but not for long.  If we have
			# just created a New Uns Payor policy for a self-pay charge, we
			# plan to switch but haven't yet.
			if ($erarecord->{MATCHTOUNSPEC} && $charge->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID) {
				return {
					SCORE => 8,
					DESCRIPTION => 'Matching secondary claim loop to New Unspecified Payer.',
				};
			}

			if (
				$charge->{PATIENTINSURANCENOTES} eq
				"created apropos of remittance for claim $charge->{CLAIMID}V$args->{CONTEXTID} in erabatch $erabatch->{ID}"
				&& !$erarecord->{MATCHTOUNSPEC}
			) {
				# If we have these notes, it's a (barely) existing policy and we
				# have just matched to it on a self-pay charge.
				# So expect patient insurance to be set.
				Assert(
					$charge->{CHARGETRANSFERTYPE} eq 'p',
					"\$erarecord->{ID} $erarecord->{ID}: \$charge->{PATIENTINSURANCENOTES} was $charge->{PATIENTINSURANCENOTES} but \$charge->{PATIENTINSURANCEID} was $charge->{PATIENTINSURANCEID} and \$charge->{CHARGETRANSFERTYPE} was $charge->{CHARGETRANSFERTYPE}",
				);
				return {
					SCORE => $charge->{TRANSFERTYPE} eq '1' ? 10 : 0,
					DESCRIPTION => 'Policy was just created to be primary on this self-pay claim',
				};
			}

			if ($charge->{PATIENTINSURANCEID} eq $charge->{CURRENTPATIENTINSURANCEID}) {
				return {
					SCORE => 10,
					DESCRIPTION => 'Policy is currently selected on the charge',
				};
			}
			else {
				my $claimaudit = $args->{CACHE}->{CLAIMAUDITOLDVALUE}->{$charge->{CLAIMID}} ||= [SQLTableHash({
					SQL => "select distinct fieldname, oldvalue from claimaudit where claimid = ?",
					CONTEXTID => $args->{CONTEXTID},
				}, $dbh, $charge->{CLAIMID})];
				
				my $fieldname = ($charge->{TRANSFERTYPE} eq '1' ? 'PRIMARY' : 'SECONDARY') . 'PATIENTINSURANCEID';
				my @oldpatientinsuranceids = 
					map { $_->{OLDVALUE} }
					grep { $_->{FIELDNAME} eq $fieldname } @$claimaudit;

				if ($charge->{TRANSFERTYPE} ne '1' && $charge->{CURRENTPATIENTINSURANCEID} == 0 && !@oldpatientinsuranceids) {
					return {
						# Discourage matching to self-pay charges as secondary.
						SCORE => ($charge->{CHARGETRANSFERTYPE} ne 'p' ? 5 : 0),
						DESCRIPTION => 'No policy has been selected on the charge, and it is not a primary charge',
					};
				}
				elsif (InList($charge->{PATIENTINSURANCEID}, @oldpatientinsuranceids)) {
					return {
						SCORE => 8,
						DESCRIPTION => 'Policy was previously selected on the charge',
					};
				}
				else {
					return {
						SCORE => 0,
						DESCRIPTION => 'Policy is not the one currently selected on the charge, and has never been associated with the charge',
					};
				}
			}
		},
	},

	PATIENTINSURANCECANCELLED => {
		SHOWWITHFIELD => 'PATIENTINSURANCEID',
		SCORE => 2,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			if (
				# old MCRCROVR logic
				$charge->{PATIENTINSURANCENOTES} =~ /Primary crovr1 indicates secondary expected/
				# new CROSSOVERPAYER logic
				|| (
					$charge->{PATIENTINSURANCESOURCEERAREC} ne ''
					&& $charge->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID
				)
			) {
				return { SKIP => 1 };
			}
			if ($charge->{PATIENTINSURANCECANCELLED}) {
				if ($erarecord->{MATCHTOUNSPEC} && ($charge->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID)) {
					return {
						SCORE => 2,
						DESCRIPTION => 'Matching secondary claim loop to New Unspecified Payor',
					};
				}
				return {
					SCORE => 0,
					DESCRIPTION => 'Policy is cancelled',
				};
			}
			else {
				return {
					SCORE => 2,
					DESCRIPTION => 'Policy is active',
				};
			}
		},
	},

	PAYOR => {
		SHOWWITHFIELD => 'PATIENTINSURANCEID',
		SCORE => 30,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge, $erabatch) = @{$args}{qw( ERARECORD CHARGE ERABATCH )};

			# "New Unspecified Payor"... it's worse than any payor which
			# matches, but better than nothing, for secondaries.  (This is built
			# to handle secondaries which are unknown to us, but known to the
			# primary (COB).  There's really no reason whatsoever to get a
			# *primary* payment from a payor we never knew existed, though,
			# except for the new, retail-clinic case where the patient pays in
			# full and is handed a CMS1500 form and told "You bill them, if
			# you like.")
			if ($charge->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID) {
				if ($erarecord->{MATCHTOUNSPEC}) {
						return {
							SCORE => 28,
							DESCRIPTION => 'Matching secondary claim loop to New Unpecified Payor',
						};
					}

				# If this is a New Unspecified Remit Payor we are considering,
				# rather than an Unspecified Remit Payor policy the patient had in
				# the past:
				if ($charge->{PATIENTINSURANCEID} eq '') {

					# Unexpected secondary on an insurance claim.
					if ($charge->{TRANSFERTYPE} eq '2' && $charge->{CHARGETRANSFERTYPE} ne 'p') {
						return {
							SCORE => 18,
							DESCRIPTION => 'New Unspecified Payor policy (presumably unknown COB)',
						};
					}

					# Unexpected primary on a self-pay claim.
					if ($charge->{TRANSFERTYPE} eq '1' && $charge->{CHARGETRANSFERTYPE} eq 'p') {
						return {
							SCORE => 18,
							DESCRIPTION => 'New Unspecified Payor policy (unexpected primary on a self-pay charge)',
						};
					}
				}
				# An existing Unspecified Remit Payor policy.
				else {
					if (
						$charge->{PATIENTINSURANCENOTES} eq
						"created apropos of remittance for claim $charge->{CLAIMID}V$args->{CONTEXTID} in erabatch $erabatch->{ID}"
					) {
						# Nearly-new Unspecified Payor.  In this case, we
						# have just (in this same batch) been through that
						# last block with a previous erarecord for this
						# claim and created a "new unspecified payor" policy
						# (but have not yet switched the policy on the
						# claim).  We want to match to this rather than
						# create a new one.
						Assert(
							$charge->{PATIENTINSURANCEID} ne ''
							&& $charge->{CHARGETRANSFERTYPE} eq 'p',
							"\$erarecord->{ID} $erarecord->{ID}: \$charge->{INSURANCEPACKAGEID} was '$charge->{INSURANCEPACKAGEID}' and \$charge->{CHARGETRANSFERTYPE} was $charge->{CHARGETRANSFERTYPE} and \$charge->{PATIENTINSURANCENOTES} was '$charge->{PATIENTINSURANCENOTES}' but \$charge->{PATIENTHASPAYORPOLICIES} was '$charge->{PATIENTHASPAYORPOLICIES}' and \$charge->{PATIENTINSURANCEID} was '$charge->{PATIENTINSURANCEID}'",
						);
						return {
							SCORE => 30,
							DESCRIPTION => 'Matching "New Unspecified Payor" policy created in this batch for this claim',
						};
					}

					# Just an ordinary old "unspecified remit payor" policy.
					my $erarecordinsurancename = ($args->{CACHE}->{ERARECORDINSURANCENAME}->{$erarecord->{ID}} ||=
						[$self->GetRecordInsuranceName($dbh, { ERARECORD => $erarecord })]
					)->[0];
					if ($erarecordinsurancename eq $charge->{INSURANCEOVERRIDENAME}) {
						return {
							SCORE => 20,
							DESCRIPTION => 'Matching previous "New Unspecified Payor" policy',
						};
					}

					if (
						# old MCRCROVR logic
						$charge->{PATIENTINSURANCENOTES} =~ /Primary crovr1 indicates secondary expected/
						# new CROSSOVERPAYER logic
						|| $charge->{PATIENTINSURANCESOURCEERAREC} ne ''
					) {
						return {
							SCORE => 15,
							DESCRIPTION => 'Matching for 18500 packages for crossover-payer claims',
						};
					}
				}
				return {
					SCORE => 0,
					DESCRIPTION => 'UNSPECIFIEDREMITPACKAGEID but hit none of the cases',
				};
			}

			my $erarecordinsurancepackageid;
			if ($erarecord->{INSURANCEPACKAGEID} ne '') {
				# if this package has been deleted and merged, find the package that it has
				# been merged to by traversing the deletion history
				$erarecordinsurancepackageid = ($args->{CACHE}->{INSURANCEPACKAGEMAPPING}->{$erarecord->{INSURANCEPACKAGEID}} ||= [SQLFirstRow("
					select newid
					from insurancepackagemapping
					start with oldid=?
					connect by prior newid=oldid
					order by level desc
				",$dbh, $erarecord->{INSURANCEPACKAGEID})])->[0] || $erarecord->{INSURANCEPACKAGEID};

				if ($erarecordinsurancepackageid eq $charge->{INSURANCEPACKAGEID}) {
					return {
						SCORE => 30,
						DESCRIPTION => 'Insurancepackage ID matches',
					};
				}
			}

			my $claimaction = $self->GetClaimAction();
			my $erarecordkickreasoncategoryid = nvl($erarecord->{KICKREASONCATEGORYID}, $erabatch->{KICKREASONCATEGORYID});

			# if there's an insurancepackage id on the line, but not a
			# kickreasoncategory, then pull the kickreasoncategory from the
			# insurancepackage on the line
			if ($erarecordinsurancepackageid ne '' && !$erarecordkickreasoncategoryid) {
				$erarecordkickreasoncategoryid = ($args->{CACHE}->{INSURANCEPACKAGEKICKREASONCATEGORYID}->{$erarecordinsurancepackageid} ||= [BusCall::Claim::LookupKickReasonCategory($dbh,{
					CLAIMACTIONID => $claimaction,
					INSURANCEPACKAGEID => $erarecordinsurancepackageid,
					CONTEXTID => $args->{CONTEXTID},
					IDONLY => 1
				})])->[0];
			}
			
			# but kickreasoncategory zero doesn't count... it's just too general
			if ($erarecordkickreasoncategoryid) {
				my $chargekickreasoncategoryid = ($args->{CACHE}->{INSURANCEPACKAGEKICKREASONCATEGORYID}->{$charge->{INSURANCEPACKAGEID}} ||= [BusCall::Claim::LookupKickReasonCategory($dbh,{
					CLAIMACTIONID => $claimaction,
					INSURANCEPACKAGEID => $charge->{INSURANCEPACKAGEID},
					CLAIMID => $charge->{CLAIMID},
					CONTEXTID => $args->{CONTEXTID},
					IDONLY => 1
				})])->[0];
				if ($chargekickreasoncategoryid eq $erarecordkickreasoncategoryid && $erarecordinsurancepackageid eq '') {
					return {
						SCORE => 30,
						DESCRIPTION => 'Payor (kickreason category) matches',
					};
				}
				elsif ($chargekickreasoncategoryid eq $erarecordkickreasoncategoryid && $erarecordinsurancepackageid ne '') {
					return {
						SCORE => 26,
						DESCRIPTION => 'Payor (kickreason category) matches, but insurancepackage ID differs',
					};
				}
				else {
					return {
						SCORE => 0,
						DESCRIPTION => 'Payor (kickreason category) does not match',
					};
				}
			}
			# if there was an insurancepackage but no (usable)
			# kickreasoncategory, then we'll try comparing the IRCs of the
			# insurancepackages on the record and charge
			elsif ($erarecordinsurancepackageid ne '') {
				my $erarecordircid = ($args->{CACHE}->{INSURANCEPACKAGEIRCID}->{$erarecordinsurancepackageid} ||= [
					SQLValues("select insurancereportingcategoryid from insurancepackage where id=?", $dbh, $erarecordinsurancepackageid)
				])->[0];
				
				my $chargeircid = ($args->{CACHE}->{INSURANCEPACKAGEIRCID}->{$charge->{INSURANCEPACKAGEID}} ||= [
					SQLValues("select insurancereportingcategoryid from insurancepackage where id=?", $dbh, $charge->{INSURANCEPACKAGEID})
				])->[0];
				
				if ($erarecordircid eq $chargeircid && $erarecordircid ne '') {
					return {
						SCORE => 23,
						DESCRIPTION => 'Insurancepackage ID does not match, but insurance reporting category does',
					};
				}
				else {
					return {
						SCORE => 0,
						DESCRIPTION => 'Insurancepackage ID does not match',
					};
				}
			}
			# if there was no insurancepackage AND no (usable)
			# kickreasoncategory, then we actually have to skip the payor match.
			# Scary.  It's either that or just always fail to match
			else {
				return { SKIP => 1 };
			}
		},
	},

	PATIENTSSN => {
		SCORE => 5,
		NAME => 'Patient SSN',
	},

	PATIENTFIRSTNAME => {
		NAME => 'Patient first name',
		SCORE => 4,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};

			my $strippederarecordname = uc $erarecord->{PATIENTFIRSTNAME};
			$strippederarecordname =~ s/[^A-Z]//g;
			my $strippedchargename = uc $charge->{PATIENTFIRSTNAME};
			$strippedchargename =~ s/[^A-Z]//g;
	
			if ($strippederarecordname eq $strippedchargename) {
				return { SCORE => 4 };
			}
			elsif (soundex($strippederarecordname) eq soundex($strippedchargename)) {
				return {
					SCORE => 3,
					DESCRIPTION => 'First name matches loosely (sounds alike)',
				};
			}
			else {
				return { SCORE => 0 };
			}
		},
	},

	PATIENTLASTNAME => {
		NAME => 'Patient last name',
		SCORE => 5,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};

			my $strippederarecordname = uc $erarecord->{PATIENTLASTNAME};
			$strippederarecordname =~ s/[^A-Z]//g;
			my $strippedchargename = uc $charge->{PATIENTLASTNAME};
			$strippedchargename =~ s/[^A-Z]//g;
	
			if ($strippederarecordname eq $strippedchargename) {
				return { SCORE => 5 };
			}
			elsif (soundex($strippederarecordname) eq soundex($strippedchargename)) {
				return {
					SCORE => 4,
					DESCRIPTION => 'Last name matches loosely (sounds alike)',
				};
			}
			else {
				return { SCORE => 0 };
			}
		},
	},

	FROMDATE => {
		SCORE => 10,
		NAME => 'From-date of service',
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};

			if ($erarecord->{FROMDATE} eq '') {
				return { SKIP => 1 };
			}

			# Hydra  91904: If the fromdate of the erarecord lies within the range of DOS (fromdate) and DOS (todate),
			#		then award From date 2 points than giving a 0.
			if ($erarecord->{FROMDATE} eq $charge->{FROMDATE}) {
				return { SCORE => 10 };
			}
			elsif ($erarecord->{FROMDATE} && $erarecord->{TODATE} && $charge->{FROMDATE} && $charge->{TODATE}
				&& (AthenaDate::DeltaDays($erarecord->{FROMDATE},$charge->{FROMDATE}) >= 0)
				&& (AthenaDate::DeltaDays($charge->{TODATE},$erarecord->{TODATE}) >= 0)) {
				return {
					SCORE => 2,
					DESCRIPTION => 'From-date of service falls within the DOS dates (between From and to dates of the charge)',
				};
			}
			else {
				return { SCORE => 0 };
			}
		},
	},

	TODATE => {
		SCORE => 2,
		NAME => 'To-date of service',
	},

	DAYS => {
		SCORE => 2,
		NAME => 'Days or units of service',
	},

	AMOUNT => {
		SCORE => 10,
		NAME => 'Billed amount',
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};

			if ($erarecord->{AMOUNT} eq '') {
				return { SKIP => 1 };
			}
			# Hydra 217978: Use the amount billed to the secondary for matching
			if ($erarecord->{AMOUNT} == $charge->{AMOUNT}) {
				return {
					SCORE => 10,
					DESCRIPTION => 'Billed amount matches',
				};
			} elsif (($charge->{SECONDARYBILLEDAMOUNT} != 0) && ($erarecord->{AMOUNT} == $charge->{SECONDARYBILLEDAMOUNT})
				&& ($charge->{TRYTRANSFERTYPE} eq '2')) {
				return {
					SCORE => 10,
					DESCRIPTION => 'Billed amount matches the amount transferred to the secondary',
				}
			} else {
				return { SCORE => 0 };
			}
		},
	},

	TRANSFERTYPE => {
		SCORE => 3,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};
			
			my $reportedtransfertype = 
				$erarecord->{CLAIMSTATUSCODE} =~ /^(1|19)$/ ? '1' :
				$erarecord->{CLAIMSTATUSCODE} =~ /^(2|20)$/ ? '2' :
				$erarecord->{CLAIMSTATUSCODE} =~ /^(3|21)$/ ? '3' :
				$erarecord->{CLAIMSTATUSCODE} eq '4' ? '' :          # "denied"
				'';

			return { SKIP => 1 } if $reportedtransfertype eq '';

			return {
				SCORE => ($charge->{TRANSFERTYPE} eq $reportedtransfertype ? 3 : 0),
			};
		},
	},

	OUTSTANDING => {
		SCORE => 2,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge, $erabatch, $cache ) = @{$args}{qw( ERARECORD CHARGE ERABATCH CACHE )};

			# Expect self-pay charges to be overpaid (paid in full by patient;
			# now payor remits).
			return { SKIP => 1 } if $charge->{CHARGETRANSFERTYPE} eq 'p';

			my $payment = SumCurrency($erarecord->{PAYMENT}, -$erarecord->{INCENTIVEPAYMENT});
			# we really pulled the CHARGE's outstanding, not the transferin's
			if ($charge->{TRANSFERTYPE} eq '2') {
				$charge->{OUTSTANDING} = $cache->{OUTSTANDING2}->{$charge->{PARENTCHARGEID}} ||=
					SQLValues({
						SQL => "
							select nvl(sum(outstanding),0)
							from <!tcharge!>
							where parentchargeid=?
								and transfertype = '2'
						",
						CONTEXTID => $args->{CONTEXTID},
					}, $dbh, $charge->{PARENTCHARGEID});
			}

			# Secondary outstanding is fixed before outstanding check is skipped
			return { SKIP => 1 } if ($payment == 0);

			if ($charge->{OUTSTANDING} >= $payment) {
				return {
					SCORE => 2,
					DESCRIPTION => "Charge would not be overpaid",
				};
			}
			else {
				return {
					SCORE => 0,
					DESCRIPTION => "Charge would be overpaid",
				};
			}
		},
	},

	VOIDED => {
		SCORE => 8,
		TEST => sub {
			my ($self, $dbh, $args) = @_;
			my ($erarecord, $charge) = @{$args}{qw( ERARECORD CHARGE )};

			if ($charge->{VOIDED}) {
				return {
					SCORE => 0,
					DESCRIPTION => 'Charge is voided',
				};
			}
			else {
				return {
					SCORE => 8,
					DESCRIPTION => 'Charge is not voided',
				};
			}
		},
	},
	
};
# function to return the matching scoring components
sub GetChargeMatchingScoringComponents {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( )]);
	AssertValidFields($args,    [qw( )]);

	return $CHARGEMATCHINGSCORINGCOMPONENTS;
}

# Empty subroutine; to be overriden in derived classes
sub GetClaimMatchingScoringComponents {
	return {};
}

#########################################################################################
# _MaybeAddUnspecifiedRemitPayor
#
# Description:
#	If we have just now matched payor remittance to transfertype 1 on what
#	is really a self-pay claim, we need to create a New Unspecified Payor
#	policy, if we haven't already done so.  (For a claim with multiple
#	charges, we will create the New Unspecified Payor policy here for the
#	first charge, and find it in normal matching for subsequent charges.
#
#	We are adding it now, but we wait until posting to SwitchPatientInsurance
#	to it.  Before we can switch, we have to void off any patient payments -
#	and that's kind of postingish, especially if we want to repost them.
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERABATCH	hashref
#		ERARECORDS	listref of hashrefs
#		CONTEXTID	look in this context for matching charges
#		MATCHINGCHARGES	the charges we have decided to match to and are
#				about to pass back up the line
#
# Return Value:
#	None: alters MATCHINGCHARGES array-of-hash directly (sets each patientinsuranceid)
#########################################################################################
sub _MaybeAddUnspecifiedRemitPayor {

	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw( ERARECORDS ERABATCH CONTEXTID MATCHINGCHARGES )]);
	my $matchingcharge = $args->{MATCHINGCHARGES}[0];
	my $erarecord = $args->{ERARECORDS}[0];

	# If we already created a New Uns Payor policy for a previous erarecord
	# on this claim, then we would have matched to it in the usual manner as
	# sort of a payor-policy match, and we would not have null
	# PATIENTINSURANCEID here.
	# H 171702: Allow a new unspecified payer to be added to secondary if the MATCHTOUNSPEC
	# flag is set.
	return unless $matchingcharge->{PATIENTINSURANCEID} eq '';

	Assert(
		($matchingcharge->{INSURANCEPACKAGEID} == $Insurance::UNSPECIFIEDREMITPACKAGEID
		&& (($matchingcharge->{TRANSFERTYPE} eq '1'
		&& $matchingcharge->{CHARGETRANSFERTYPE} eq 'p') || ($erarecord->{MATCHTOUNSPEC}))),
		"_MaybeAddUnspecifiedRemitPayor: properly called with null PATIENTINSURANCEID but INSURANCEPACKAGEID was $matchingcharge->{INSURANCEPACKAGEID} vs \$Insurance::UNSPECIFIEDREMITPACKAGEID $Insurance::UNSPECIFIEDREMITPACKAGEID and TRANSFERTYPE was '$matchingcharge->{TRANSFERTYPE}' and CHARGETRANSFERTYPE was '$matchingcharge->{CHARGETRANSFERTYPE}'",
	);
	my $notes;
	$notes = $erarecord->{MATCHTOUNSPEC} ?
		"created new unspecified policy for claim $matchingcharge->{CLAIMID}V$args->{CONTEXTID} in erabatch $args->{ERABATCH}{ID}" :
			"created apropos of remittance for claim $matchingcharge->{CLAIMID}V$args->{CONTEXTID} in erabatch $args->{ERABATCH}{ID}";
	# Create the new policy.
	my $insurancename = $self->GetRecordInsuranceName($dbh, { ERARECORD => $erarecord }) || "unexpected primary";

	my @previousunspecifiedpayor = SQLColumnValues("select id from patientinsurance where
		patientid = ? and insurancepackageid = ? and sequencenumber = ? and insuranceoverridename = ? and  notes like ? || '%' order by id desc",
		$dbh,
		$matchingcharge->{PATIENTID},
		$Insurance::UNSPECIFIEDREMITPACKAGEID,
		$matchingcharge->{TRANSFERTYPE} eq 'p' ? '' : $matchingcharge->{TRANSFERTYPE},
		$insurancename  eq "unexpected primary" ? 'UNEXPECTED PRIMARY' : $insurancename,
		$erarecord->{MATCHTOUNSPEC} ? "created new unspecified policy for claim $matchingcharge->{CLAIMID}V$args->{CONTEXTID} in erabatch" :
			"created apropos of remittance for claim $matchingcharge->{CLAIMID}V$args->{CONTEXTID} in erabatch"
	);

	if(@previousunspecifiedpayor) {
		$_->{PATIENTINSURANCEID} = $previousunspecifiedpayor[0] for @{$args->{MATCHINGCHARGES}};	
		return;	
	}

	my $patientinsuranceid = BusCall::PatientInsurance::AddUnspecifiedRemitPayor($dbh, {
		PATIENTID => $matchingcharge->{PATIENTID},
		TRANSFERTYPE => $matchingcharge->{TRANSFERTYPE},
		PAYORNAME => $insurancename,
		USERNAME => 'ATHENA',
		# Add this note to tell scoring that this is a very good match
		# for any further erarecord in this batch for this claim.
		NOTES => $notes,
	});

	# Now we can fill in each erarecord patientinsuranceid, for MatchClaim
	# to store along with the other match data (claimid, transfertype, etc).
	$_->{PATIENTINSURANCEID} = $patientinsuranceid for @{$args->{MATCHINGCHARGES}};
}


#########################################################################################
# GetRecordInsuranceName
#
# Description:
# 	-Generates an appropriate insurance name for an erarecord
#	-this is the name that would be used as the insuranceoverridename on an
#		unspecified remit payor policy created from this erarecord
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERARECORD hashref
#
# Return Value:
#	string: name of insurance paying the erarecord
#########################################################################################
sub GetRecordInsuranceName {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args,['ERARECORD']);
	my $erarecord = $args->{ERARECORD};

	my $insurancename;
	if ($erarecord->{INSURANCEPACKAGEID}) {
		$insurancename ||= SQLValues("
			select name||' ['||id||']' from insurancepackage where id=?
		",$dbh,$erarecord->{INSURANCEPACKAGEID});
	} elsif ($erarecord->{KICKREASONCATEGORYID} && $erarecord->{KICKREASONCATEGORYID} != $KickReasonCategory::X12) {
		$insurancename ||= SQLValues("
			select name from kickreasoncategory where id=?
		",$dbh,$erarecord->{KICKREASONCATEGORYID});
	} else {
		$insurancename ||= SQLValues("
			select payorname from erabatch where id=?
		",$dbh,$erarecord->{ERABATCHID});
	}

	return $insurancename;
}


#########################################################################################
# MatchRecordToKick
#
# Description:
# 	-Matches an erarecord to a kickreason
#	-Hydra 81579: As of 9.12.1, saves the matching info in the database.
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERARECORD	hashref
#		ERAKICK		hashref
#		USERNAME
#
# Return Value:
#	none (manipulates the database directly)
#########################################################################################
sub MatchRecordToKick {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, ['ERARECORD', 'ERAKICK', 'USERNAME']);

	my $erarecord = $args->{ERARECORD};
	my $erakick = $args->{ERAKICK};
	my %kickreason;
	
	return if $erarecord->{TRANSFERTYPE} eq '3';

	if ( $erakick->{KICKREASONID} eq ''
		&& $erakick->{KICKCODE} ne 'XXX') {
		%kickreason = $self->StandardKickReasonLookup($dbh,{ERARECORD=>$erarecord,ERAKICK=>$erakick});
	}
	# kickcode _XXX_
	elsif ($erarecord->{CLAIMID} ne '' 
		&& $erarecord->{CHARGEID} ne '' 
		&& $erarecord->{KICKCODE} eq 'XXX' 
		&& $erarecord->{KICKREASONID} eq '') {
		
		my ($sqlwhere,@sqlbinds);
		if ($erarecord->{TRANSFERTYPE} ne '') {
			$sqlwhere .= ' and transfertype = ? ';
			push(@sqlbinds,$erarecord->{TRANSFERTYPE});
		}
		my $outstanding = SQLValues("
			select sum(outstanding)
			from tcharge 
			where parentchargeid = ?
				$sqlwhere
		",$dbh,$erarecord->{CHARGEID},@sqlbinds);

		if ($outstanding == 0) {
			my $kickreasonref;
			$kickreasonref = BusCall::Claim::KickReasonLookup($dbh,{
				KICKCODE => 'REMITRECEIVED'
			});
			%kickreason = %{$kickreasonref};
		}
	}
	
	# We go ahead and store map info right now for erakick kicks
	if (%kickreason) {
		my @kickcols = qw(
			KICKREASONID BALANCETRANSACTIONTYPE BALANCETRANSACTIONREASON MAPPED MAPPEDBY
		);
		$erakick->{operation} = 'Update';
		@{$erakick}{qw(KICKREASONID BALANCETRANSACTIONTYPE BALANCETRANSACTIONREASON MAPPED MAPPEDBY)} = 
			(@kickreason{qw(ID BALANCETRANSACTIONTYPE BALANCETRANSACTIONREASON)},'SYSDATE',$args->{USERNAME});
		# Put this kick into the ERARECORD table if the ERARECORDID is not set, else put it in ERAKICK
		my $erakicktable = $erakick->{ERARECORDID} ? 'ERAKICK' : 'ERARECORD';
		ProcessForm($erakicktable, $dbh, $args->{USERNAME}, $erakick, [@kickcols]);
	}	
}


#########################################################################################
# StandardKickReasonLookup
#
# Description:
#	-used to find a kickreason for an erarecord with erakick
#
# Parameters:
#	$dbh:  application databse handle object
#	$args: hashref of arguments:
#		ERARECORD
#		ERAKICK
#
# Return Value:
#	kickreason hash
#########################################################################################
sub StandardKickReasonLookup {
	my ($self, $dbh, $args) = @_;
	my $erarecord = $args->{ERARECORD};
	my $erakick = $args->{ERAKICK};
	my $claimaction = $self->GetClaimAction();
	my $eraformat = $self->GetFileFormat();

	return if $erakick->{KICKCODE} eq '';

	# for ERA (not ABP) trust that erarecord.kickreasoncategoryid is correct
	if (
		$erakick->{KICKREASONID} eq ''
		&& $erarecord->{KICKREASONCATEGORYID} ne ''
		&& $eraformat ne 'ABP'
	) {
		my $kickreasonref;
		$kickreasonref = BusCall::Claim::KickReasonLookup($dbh, {
			CLAIMACTIONID        => $claimaction,
			KICKREASONCATEGORYID => $erarecord->{KICKREASONCATEGORYID},
			KICKCODE             => uc($erakick->{KICKCODE}),
			FOLLOWKRCMATRIX      => 1,
		});
		my %kickreason = %{$kickreasonref};
		return %kickreason if $kickreason{ID} ne '';
	}

	# try to use erarecord.patientinsuranceid if available
	if ($erarecord->{PATIENTINSURANCEID}) {
		my $insurancepackageid = SQLValues("select insurancepackageid from patientinsurance where id=?", $dbh, $erarecord->{PATIENTINSURANCEID});
		if ($insurancepackageid) {
			my $kickreasonref;
			$kickreasonref = BusCall::Claim::KickReasonLookup($dbh, {
				CLAIMACTIONID => $claimaction,
				INSURANCEPACKAGEID => $insurancepackageid,
				KICKCODE => uc($erakick->{KICKCODE}),
			});

			my %kickreason = %{$kickreasonref};
			return %kickreason if %kickreason;
		}
	}

	my $krcmatrixlookup = Athena::Conf::AthenaNet::Behavior('posting')->{match}->{identify_krcmatrix_for_thirdparty_remittance} // 0;

	# for Remittances manufactured by third parties
	if ($krcmatrixlookup && $erakick->{KICKREASONID} eq ''
		&& $erarecord->{KICKREASONCATEGORYID} ne ''
		&& $self->GetPaymentBatchRouteThirdPartyRemittanceFlag() eq 'Y'
		) {
			my $kickreasoncategoryid = $erarecord->{KICKREASONCATEGORYID};
			my $kickreasonref = BusCall::Claim::KickReasonLookup($dbh, {
				CLAIMACTIONID => $claimaction,
				KICKREASONCATEGORYID => $kickreasoncategoryid,
				KICKCODE => uc($erakick->{KICKCODE}),
				FOLLOWKRCMATRIX => 1,
			});
		my %kickreason = %{$kickreasonref};
		return %kickreason if $kickreason{ID} ne '';
	}

	# match the kickcode, but only if the payor/kickreasoncategory can be
	# identified, and the code is not already matched
	if ($erarecord->{CLAIMID} ne '' 
	&& $erarecord->{TRANSFERTYPE} ne '' 
		&& $erakick->{KICKCODE} ne 'XXX' 
		&& $erakick->{KICKREASONID} eq '') {
		my $kickreasonref;

		$kickreasonref = BusCall::Claim::KickReasonLookup($dbh,{
			CLAIMACTIONID => $claimaction,
			CLAIMID => $erarecord->{CLAIMID},
			TRANSFERTYPE => $erarecord->{TRANSFERTYPE},
			KICKCODE => uc($erakick->{KICKCODE}),
		});
		my %kickreason = %{$kickreasonref};
		return %kickreason;
	}

	if (
		$erakick->{KICKREASONID} eq ''
		&& $erarecord->{INSURANCEPACKAGEID} ne ''
	) {
		my $kickreasonref;
		$kickreasonref = BusCall::Claim::KickReasonLookup($dbh,{
			CLAIMACTIONID => $claimaction,
			INSURANCEPACKAGEID => $erarecord->{INSURANCEPACKAGEID},
			KICKCODE => uc($erakick->{KICKCODE}),

		});
		my %kickreason = %{$kickreasonref};
		return %kickreason if $kickreason{ID} ne '';
	}

	if (
		$erakick->{KICKREASONID} eq ''
		&& $erarecord->{KICKREASONCATEGORYID} ne ''
	) {
		my $kickreasonref;
		$kickreasonref = BusCall::Claim::KickReasonLookup($dbh,{
			CLAIMACTIONID => $claimaction,
			KICKREASONCATEGORYID => $erarecord->{KICKREASONCATEGORYID},
			KICKCODE => uc($erakick->{KICKCODE}),
		});
		my %kickreason = %{$kickreasonref};
		return %kickreason if $kickreason{ID} ne '';
	}

	# trust the engine kickreasoncategoryid, if the result is a posting override kick.
	# that should be a sign that we had a particular kick clearly in mind when we called this.
	if ($erakick->{KICKREASONID} eq '') {
		my $kickreasoncategoryid = $self->GetKickReasonCategoryID();
		my $kickreasonref;
		$kickreasonref = BusCall::Claim::KickReasonLookup($dbh, {
			CLAIMACTIONID        => $claimaction,
			KICKREASONCATEGORYID => $kickreasoncategoryid,
			KICKCODE             => uc($erakick->{KICKCODE}),
			FOLLOWKRCMATRIX      => 1,
		});
		my %kickreason = %{$kickreasonref};
		return %kickreason if $kickreason{POSTINGOVERRIDEYN} eq 'Y';
	}

	return;
}



#########################################################################################
# KickReasonLookupForceCategory
#
# Description:
# 	-used to find a kickreason if we want to force the kickreasoncategory
#	-basically used for BCBS scenarios where the kickcode doesn't come from 
#	 the claim, but from the person who sent us the ERA
#	-Converts the found kickcode to athenakickcode, so AddNote doesn't complain
#	-NOT USED in this parent object!
#
# Parameters:
#	$dbh:  application database handle object
#	$args: hashref of arguments:
#		ERARECORD
#		KICKREASONCATEGORYID
#		KICKREASONCATEGORYNAME
#
# Return Value:
#	kickreason hash (also, alters erarecord.note directly)
#########################################################################################
sub KickReasonLookupForceCategory {
	my ($self, $dbh, $args) = @_;
	my $erarecord = $args->{ERARECORD};
	# find the kickreason by CATEGORY
	my $kickreasonref = BusCall::Claim::KickReasonLookup($dbh,{
		KICKREASONCATEGORYID => $args->{KICKREASONCATEGORYID},
		KICKCODE => $erarecord->{KICKCODE}
	});
	my %kickreason = %{$kickreasonref};

	# map it to its athenakickcode, but put the real reason in the note 
	if ($kickreason{ID}) {
		$erarecord->{NOTE} = substr("$kickreason{KICKCODE} $args->{KICKREASONCATEGORYNAME}: $kickreason{NAME}",0,1999);
		$kickreasonref = BusCall::Claim::KickReasonLookup($dbh,{
			KICKREASONCATEGORYID => 0,
			KICKCODE => $kickreason{ATHENAKICKCODE}
		});
		%kickreason = %{$kickreasonref};
	}
	return %kickreason;
}



#########################################################################################
# LookupKickReasonCategory 
#
# Description:
# 	-used to find a kickreasoncategory
#	-overridable method: BCBS scenarios will want to force it
#
# Parameters:
#	$dbh:  application database handle object
#	$args: hashref of arguments:
#		CLAIMID
#		TRANSFERTYPE
#
# Return Value:
#	kickreasoncategory hash
#########################################################################################
sub LookupKickReasonCategory {
	my ($self, $dbh, $args) = @_;
	my $claimaction = $self->GetClaimAction();
	return BusCall::Claim::LookupKickReasonCategory($dbh,{
		CLAIMACTIONID => $claimaction,
		CLAIMID => $args->{CLAIMID},
		TRANSFERTYPE => $args->{TRANSFERTYPE}
	});
}

#########################################################################################
# CombineRecordsIntoUnpostable
#
# Description:
# 	-used to combine ERARECORDs into a single UNPOSTABLE
#	-currently can be used for CAPCHECK or MANAGEMENTFEE unpostables
#		-for CAPCHECKs, we also create a CAPPAYMENT that is associated with
#			the unpostable, and close the unpostable
#	-ERARECORDS can be batch exceptions or regular records
#		-ERARECORDS are marked as APPLIED after unpostable generation;
#			batch exceptions will then appear as Discarded
#
# Parameters:
#	$dbh:  application database handle object
#	$args: (required)
#		ERARECORDS - listef of records
#		KICKREASONCATEGORYID - for unpostable
#		USERNAME
#		PAYMENTBATCHID
#		UNPOSTABLETYPEID - CAPCHECK or MANAGEMENTFEE
#		(required for CAPCHECK)
#		PAYDATE
#		
# Return Value:
#	the ID of the UNPOSTABLE we create
#########################################################################################
sub CombineRecordsIntoUnpostable {
	my ($self, $dbh, $args) = @_;

	my @requiredfields = ('ERARECORDS', 'KICKREASONCATEGORYID', 'USERNAME', 'PAYMENTBATCHID', 'UNPOSTABLETYPEID');
	push (@requiredfields, 'PAYDATE') if $args->{UNPOSTABLETYPEID} eq 'CAPCHECK';
	
	AssertRequiredFields($args, \@requiredfields);
	my $erabatchid = $args->{ERARECORDS}->[0]->{ERABATCHID};
	my $ercmflag = SQL::Select->new()->Select(
			"count(*)",
		)->From(
			"erarecord",
		)->Where(
			["erabatchid = ?",$erabatchid],
			"action = 'BATCHEXCEPTION'",
			"eradiscardstatusreasonid = 'EOBUNLISTED'",
			"payment <> 0",
	)->Values($dbh);

	if ($ercmflag) {
		# If the batch is a ercm batch, lets mark these records as manually posted. The unpostable will be created when we
		# have a paymentbatch.
		foreach my $erarecord(@{$args->{ERARECORDS}}) {
			my $erarecordnote;
			$erarecordnote->{operation} = 'Add';
			$erarecordnote->{ERARECORDID} = $erarecord->{ID};
			$erarecordnote->{NOTE} = $erarecord->{DISCARDNOTE};
			$erarecordnote->{ERADISCARDSTATUSID} = 'INPROCESS';
			$erarecordnote->{ERADISCARDSTATUSREASONID} = $args->{UNPOSTABLETYPEID};
			ProcessForm('ERARECORDNOTE', $dbh, 'AUTOPOSTER', $erarecordnote, [qw(
				ERARECORDID ERADISCARDSTATUSREASONID NOTE ERADISCARDSTATUSID
			)]);

			$erarecord->{operation} = 'Update';
			$erarecord->{MANUALFLAG} = 'Y';
			$erarecord->{ERADISCARDSTATUSREASONID} = '';
			$erarecord->{ERADISCARDSTATUSID} = '';
			$erarecord->{ASSIGNMENTGROUPCLASSID} = '';
			$erarecord->{APPLIED} = 'SYSDATE';
			$erarecord->{APPLIEDBY} = $args->{USERNAME};
			$erarecord->{DISCARDED} = '';
			$erarecord->{DISCARDEDBY} = '';
			ProcessForm('ERARECORD', $dbh, $args->{USERNAME}, $erarecord, [qw(
				APPLIED APPLIEDBY MANUALFLAG ERADISCARDSTATUSREASONID ERADISCARDSTATUSID
				ASSIGNMENTGROUPCLASSID DISCARDED DISCARDEDBY
			)]);
		}
		return;
	}
	my %kickreasoncategory = SQLHash(
		qq[
			select
				kickreasoncategory.*
			from
				kickreasoncategory
			where
				kickreasoncategory.id = ?
		], $dbh, $args->{KICKREASONCATEGORYID}
	);
	
	my %unpostabletype = SQLHash(
		qq[
			select
				eradiscardstatusreason.*
			from
				eradiscardstatusreason
			where
				eradiscardstatusreason.id = ?
		], $dbh, $args->{UNPOSTABLETYPEID}
	);
	
	confess "no such unpostable type: '$args->{UNPOSTABLETYPEID}'" unless $unpostabletype{ID};
	
	$kickreasoncategory{NAME} ||= "Unknown Payer";

	map { $_->{PAYMENT} = -$_->{PAYMENT} } grep { $_->{REVERSALFLAG} eq 'Y' } @{$args->{ERARECORDS}};

	my $sum = SumCurrency( map { $_->{PAYMENT} } @{$args->{ERARECORDS}});

	my $unpostableid = Unpostable::Create($dbh, {
		AMOUNT => $sum,
		POSTDATE => Today(),
		POSTEDBY => $args->{USERNAME},
		PAYMENTBATCHID => $args->{PAYMENTBATCHID},
		UPDATETARGET => 'ADDTOUNPOSTABLE',
		UNPOSTABLETYPEID => $unpostabletype{ID},
		KICKREASONCATEGORYID => $kickreasoncategory{ID},
		NOTE => "Automatic unpostable for $kickreasoncategory{NAME} $unpostabletype{NAME}.",
	});

	if ($unpostabletype{ID} eq 'CAPCHECK') {
	
		# the first and last of the month of the PAYDATE
		my $todate = my $fromdate = AthenaDate::GetLastDateOfMonth( { DATE => $args->{PAYDATE} });
		$fromdate =~ s/\/..\//\/01\//;

		# the CAPPAYMENT will not be provider- or department-specific
		# we identify the payer with the KRC name, in the NOTES field
		my %cappayment = BusCall::Claim::Add($dbh, {
			USERNAME => $args->{USERNAME},
			AMOUNT => $sum,
			PAYMENTBATCHID => $args->{PAYMENTBATCHID},
			FROMDATE => $fromdate,
			TODATE => $todate,
			POSTDATE => Today(),
			PROVIDERIDS => [ '_ALL_' ],
			DEPARTMENTIDS => [ '_ALL_' ],
			UNPOSTABLEID => $unpostableid,
			NOTES => $kickreasoncategory{NAME},
		});

		# close the CAPCHECK
		Unpostable::Update($dbh, {
			ID => $unpostableid,
			POSTDATE => Today(),
			UNPOSTABLEEVENTID => 131,
		});
	}

	foreach my $erarecord (@{$args->{ERARECORDS}}) {

		$erarecord->{operation} = 'Update';
		$erarecord->{APPLIED} = Today();
		$erarecord->{APPLIEDBY} = $args->{USERNAME};

		ProcessForm('ERARECORD', $dbh, $args->{USERNAME}, $erarecord, [ 'APPLIED', 'APPLIEDBY' ]);
	}
	
	return $unpostableid;
}


#########################################################################################
# ProcessManualChargeMatch
#
# Description:
# 	- When a user manually matches a charge to an erarecord, this function performs
#	  the steps necessary to match the erarecord to the charge in the database
#
# Parameters:
#	$dbh:  application database handle object
#		ID - ID of the erarecord that is being matched
#		CHARGEID - ID of the charge the erarecord has been matched to
#		MATCHINGSCORE
#		(optional)
#		TREATASTERTIARYYN
#		
# Return Value:
#	(none)
#########################################################################################
sub ProcessManualChargeMatch {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args, ['MATCH_INFO']);
	my $matchinfo = $args->{MATCH_INFO};
	AssertRequiredFields($matchinfo, ['ID', 'CHARGEID']);
	my $practiceid = $matchinfo->{MATCHCONTEXTID} || GetPracticeID();
	my ($erarecordtable, $transactiontable)
		= BusCall::PracticeStructure::GetFullyQualifiedTablenames($practiceid, $dbh, 'ERARECORD', 'TRANSACTION');

	my @sqlcols = qw(
		CHARGEID
		CLAIMID
		TRANSFERTYPE
		MANUALFLAG
		MATCHINGSCORE
		MATCHED
		MATCHEDBY
		APPLIED
		APPLIEDBY
		ACTION
		ERADISCARDSTATUSREASONID
		ERADISCARDSTATUSID
		ASSIGNMENTGROUPCLASSID
		PATIENTINSURANCEID
		TREATASTERTIARYYN
	);

	# some general preparation
	my $sql = "
		select erarecord.*,kickreason_name(erarecord.kickreasonid) kickreason
		from $erarecordtable
		where id=?";
	my %erarecord = SQLHash($sql,$dbh,$matchinfo->{ID});
	my $insurancepackagename = SQLValues("select name||' ['||id||']' from insurancepackage where id=?",$dbh,$erarecord{INSURANCEPACKAGEID});
	$matchinfo->{ACTION} = $erarecord{ACTION};

	$sql = "
		select parentchargeid,claimid,transfertype,patientid
		from $transactiontable
		where type in ('CHARGE','TRANSFERIN')
			and parentchargeid = ?
			and transfertype = ?
	";
	my %parentcharge = SQLHash($sql,$dbh,$matchinfo->{PARENTCHARGEID},$matchinfo->{TRANSFERTYPE});
	my $parentchargeid = $parentcharge{PARENTCHARGEID} || $matchinfo->{PARENTCHARGEID};
	$matchinfo->{CLAIMID} = $parentcharge{CLAIMID} if ($parentcharge{CLAIMID});
	$matchinfo->{PATIENTID} = $parentcharge{PATIENTID} if ($parentcharge{PATIENTID});
	$matchinfo->{TRANSFERTYPE} = $parentcharge{TRANSFERTYPE} if ($parentcharge{TRANSFERTYPE});
	$matchinfo->{operation} = 'Update';
	$matchinfo->{POSTDATE} = Today();
	# CLAIMLEVELREMIT means CHARGEID is only first of several charges
	$matchinfo->{CHARGEID} = ($matchinfo->{ACTION} eq 'CLAIMLEVELREMIT') ? '' : $parentchargeid;

	DBTransaction sub {
		my $selectedpatientinsuranceid = $matchinfo->{PATIENTINSURANCEID};

		if ($selectedpatientinsuranceid eq 'NEW') {
			my @previousunspecifiedpayor = SQLColumnValues("select id from patientinsurance where
				patientid = ? and insurancepackageid = ? and sequencenumber = ? and insuranceoverridename = ? order by id desc",
				$dbh,
				$matchinfo->{PATIENTID},
				$Insurance::UNSPECIFIEDREMITPACKAGEID,
				$matchinfo->{TRANSFERTYPE} eq 'p' ? '' : $matchinfo->{TRANSFERTYPE},
				$insurancepackagename || "OTHER INSURANCE"
			);
			
			if(@previousunspecifiedpayor) {
				$selectedpatientinsuranceid = $previousunspecifiedpayor[0];
			}
			else {
				$selectedpatientinsuranceid = BusCall::PatientInsurance::AddUnspecifiedRemitPayor($dbh, {
					PATIENTID => $matchinfo->{PATIENTID},
					TRANSFERTYPE => $matchinfo->{TRANSFERTYPE},
					PAYORNAME => $insurancepackagename || "other insurance",
					USERNAME => $Global::session{USERNAME},
				});
			}
		}

		# match the record & charge!
		$matchinfo->{MANUALFLAG} = '';
		$matchinfo->{ERADISCARDSTATUSREASONID} = '';
		$matchinfo->{ERADISCARDSTATUSID} = '';
		$matchinfo->{ASSIGNMENTGROUPCLASSID} = '';
		$matchinfo->{APPLIED} = '';
		$matchinfo->{APPLIEDBY} = '';
		$matchinfo->{MATCHED} = 'SYSDATE';
		$matchinfo->{MATCHEDBY} = 'ATHENA'; # the session user who caused the match is recorded in the nomatchingcharge unpostable
		$matchinfo->{PATIENTINSURANCEID} = $selectedpatientinsuranceid;

		ProcessForm($erarecordtable,$dbh,$Global::session{USERNAME},$matchinfo,\@sqlcols);
	};

	# Hydra 81579. Now that the charge is manually matched, we want to try and map
	# the (hitherto unmapped) kicks on this charge. We used to accomplish this by
	# rematching the entire batch, but it is much more efficient to instead call
	# MatchRecordToKick for all kicks on this charge (and leave other charges alone).

	# Copy the changes that we just made in the database.
	$erarecord{$_} = $matchinfo->{$_} foreach (@sqlcols);

	(my $recordwithkicks) = $self->GetErakickData($dbh, {
		ERARECORDID => $erarecord{ID},
		ERARECORDS  => [ \%erarecord ],
	});

	foreach my $erakick (@{$recordwithkicks->{KICKS}}) {
		$self->MatchRecordToKick($dbh,{
			ERARECORD => $recordwithkicks,
			ERAKICK   => $erakick,
			USERNAME  => $Global::session{USERNAME},
		});
	}
}

####################################################################################################
# MarkOffsettingBatchExceptionERARecordPairs
#
# Description:
#	Sometimes, in a batch, we receive BALANCEFORWARD and PROVIDERTAKEBACK batch exceptions for the
#	same payorcontrolnumber that sum to $0.  We are not very interested in them, so we will create
#	erarecords from them, but not unpostables.  Flag these records with SKIP to tell DiscardRecord
#	to mark them as applied/Y instead of discarded/D.
#
# Parameters:
#       $dbh: the database handle.
#       $args:
#	(required):
#		ERABATCH => a hashref of ERA batch information
#
# Return Value:
#	erabatch hash
####################################################################################################
sub MarkOffsettingBatchExceptionERARecordPairs {
	my ($self, $dbh, $args) = @_;

	my $erabatch = $args->{ERABATCH};
	my $kickreasoncategoryid = $erabatch->{KICKREASONCATEGORYID};
	my $eradiscardrules;

	my @erarecords = SQLTableHash("select * from erarecord where erabatchid = ?", $dbh, $erabatch->{ID});
	my @exceptions = SQLTableHash(
		qq[
			select
				erarecord.*
			from
				erarecord
			where
				erarecord.action = 'BATCHEXCEPTION'
				and erarecord.plbreasoncode in ('FB', 'WO', 'CS')
				and erarecord.payorcontrolnumber is not null
				and erarecord.applied is null
				and erarecord.erabatchid = ?
		], $dbh, $erabatch->{ID}
	);

	my @pcnnumbers = UniqueElements(map { $_->{PAYORCONTROLNUMBER} } @exceptions);

	$eradiscardrules = $self->GetERADiscardRules($dbh, { KICKREASONCATEGORYID => $kickreasoncategoryid }) if @exceptions;

	foreach my $pcnnumber (@pcnnumbers){
		my @records = grep { $_->{PAYORCONTROLNUMBER} eq $pcnnumber } @exceptions;
		my @records2 = @records;
		foreach my $record (@records) {
			next if $record->{SKIPLOOP};
			foreach my $record2 (@records2) {
				next if ($record2->{SKIPLOOP} || $record->{SKIPLOOP});
				if ($record->{PAYMENT} == -$record2->{PAYMENT}) {
					my ($ruleid1, $payercontrolnumberstripped1) = $self->GetERADiscardRuleIDAndPCNStripped($dbh, { RULES => $eradiscardrules, ERARECORD => $record});
					my ($ruleid2, $payercontrolnumberstripped2) = $self->GetERADiscardRuleIDAndPCNStripped($dbh, { RULES => $eradiscardrules, ERARECORD => $record2});
					if (!($ruleid1 || $ruleid2)) {
						$record->{OFFSET} = $record2->{OFFSET} = 1;
					}
					$record->{SKIPLOOP} = $record2->{SKIPLOOP} = 1;
				}
			}
		}
	}

	ProcessTable($dbh, {
		USERNAME => 'ATHENA',
		TABLENAME => 'ERARECORD',
		OPERATION => 'Update',
		TABLEROWS => [map { {
			ID => $_->{ID},
			APPLIED => 'SYSDATE',
			APPLIEDBY => 'ATHENA',
			DISCARDED => '',
			DISCARDEDBY => '',
			MANUALFLAG => 'Y',
		} } (grep { $_->{OFFSET} } @exceptions)],
		SYSDATEHACK => 1,
	}) if (grep { $_->{OFFSET} } @exceptions);

}

#########################################################################################
# GetERADiscardRuleIDAndPCNStripped
#
# Description:
#	- Applies the rules to erarecord and if the rule satisfies for a record,
#	  it identies the Payorcontrolnumberstripped(actual PCN to be matched
#	  between unpostables) and ruleid
#
# Parameters:
#	(required)
#		RULES - ERA Discard rules(based on either KRCID or ERA Engine)
#		ERARECORD - ERA Record for which the rule to be applied to identify
#		            payorcontrolnumberstripped and ruleid for unpostable.
#
# Return Value:
#	Rule id and payorcontrolnumber stipped
#
#########################################################################################
sub GetERADiscardRuleIDAndPCNStripped {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, [qw(
		RULES
		ERARECORD
	)]);

	my $rules = $args->{RULES};
	my $erarecord = $args->{ERARECORD};

	my %operatorsub = (
		'<'  => sub { return $_[0] < $_[1] },
		'>'  => sub { return $_[0] > $_[1] },
		'='  => sub { return $_[0] == $_[1] },
		'>=' => sub { return $_[0] >= $_[1] },
		'<=' => sub { return $_[0] <= $_[1] },
		'<>' => sub { return $_[0] != $_[1] },
	);

	foreach my $rule (@$rules) {
		my ($operator, $operand) = $rule->{AMOUNTTEST} =~ /^([<>!=]+)(\d+)$/;
		next unless ($operator && ($operand ne ''));

		my $pcnregex = $rule->{PCNMASK} ? qr/$rule->{PCNMASK}/ : qr/.*/;
		my $controlnumberregex = $rule->{CONTROLNUMBER} ? qr/$rule->{CONTROLNUMBER}/ : qr/.*/;
		my $procedurecoderegex = $rule->{PROCEDURECODE} ? qr/$rule->{PROCEDURECODE}/ : qr/.*/;
		my $claimstatuscoderegex = $rule->{CLAIMSTATUSCODE} ? qr/$rule->{CLAIMSTATUSCODE}/ : qr/.*/;

		my $matchmask = qr/$rule->{BATCHEXCEPTIONMATCHMASK}/;

		if ((($erarecord->{PLBREASONCODE} eq $rule->{PLBREASONCODE} && uc($rule->{PLBREASONCODE}) ne 'NULL') || (!$erarecord->{PLBREASONCODE} && uc($rule->{PLBREASONCODE}) eq 'NULL'))
			&& ($operatorsub{$operator}($erarecord->{PAYMENT}, $operand))
			&& ($erarecord->{PAYORCONTROLNUMBER} =~ /$pcnregex/)
			&& ($erarecord->{CONTROLNUMBER} =~ /$controlnumberregex/)
			&& ($erarecord->{PROCEDURECODE} =~ /$procedurecoderegex/)
			&& ($erarecord->{CLAIMSTATUSCODE} =~ /$claimstatuscoderegex/)){
			my $pcnstripped;
			($pcnstripped) = $erarecord->{PAYORCONTROLNUMBER} =~ /$matchmask/ if $rule->{BATCHEXCEPTIONMATCHMASK};
			$pcnstripped =~ s/^\s+|\s+$//g;

			return ($rule->{ID}, $pcnstripped, $rule->{ERADISCARDSTATUSREASONID});
		}
	}

	return;
}

#########################################################################################
# SetControlNumberForBatchExceptionRecord
#
# Description:
#	- As a part of claim association we need to set the controlnumber
#	  of batch exception record by matching with reversal readjudication
#	  records.
#
# Parameters:
#	(required)
#		ERARECORD - Batch Exception ERA Record for which controlnumber
#		            to be set
#
# Return Value:
#	Hashref containing:
#		Claimid [Id of the associated claim(from rev/readj erarecord)]
#		Controlnumber [in case of non-athena claim, we will be using it]
#		Matched - Whether associated record is matched
#		Transfertype
#		ERA Record ID - Associated ERA Record ID
#
#########################################################################################
sub SetControlNumberForBatchExceptionRecord {
	my ($self, $dbh, $args) = @_;

	my $erarecord = $args->{ERARECORD};
	my $username = $args->{USERNAME};
	my $pcn = $erarecord->{PAYORCONTROLNUMBER};
	my $ruleid = $args->{RULEID};

	my $claimid;
	my $controlnumber;
	my $transfertype;
	my $matched;
	my $discarded;

	my ($claimmatchcolumn, $claimmatchmask, $claimmatchexpression) = SQL::Select->new()->Select(
			"claimmatchcolumn",
			"claimmatchmask",
			"claimmatchexpression",
		)->From(
			"eradiscardruleview",
		)->Where(
			["id = ?", $ruleid],
			"deleted is null",
	)->Values($dbh);

	my ($claimmaskmatchonpcn) = $pcn =~ /$claimmatchmask/;

	$claimmatchexpression =~ s/\[\[CLAIMMATCHMASK\]\]/$claimmaskmatchonpcn/;

	return unless ($claimmatchcolumn && $claimmatchexpression);

	my $controlnumbersql = SQL::Select->new()->Select(
			"erarecord.id ERARECORDID",
			"erarecord.claimid",
			"erarecord.controlnumber",
			"erarecord.transfertype",
			"erarecord.matched",
			"erarecord.fromdate",
			"erarecord.discarded",
			"erarecord.patientfirstname",
			"erarecord.patientlastname",
			"erarecord.patientinsuranceidnumber",
		)->From(
			"erarecord",
		)->Where(
			"erarecord.action is null",
			["erarecord.erabatchid = ?", $erarecord->{ERABATCHID}],
			["erarecord.$claimmatchcolumn like ?", $claimmatchexpression],
	)->OrderBy(
		"erarecord.fromdate",
	);

	my $reversalcontrolnumbersql = $controlnumbersql->Clone();

	$reversalcontrolnumbersql->Where(
			"erarecord.reversalflag = 'Y'",
	);

	my $associatedrecord = ($reversalcontrolnumbersql->TableHash($dbh))[0];

	unless ($associatedrecord->{CONTROLNUMBER}) {
		$controlnumbersql->Where(
				"nvl(erarecord.reversalflag, 'x') <> 'Y'",
		);
		$associatedrecord = ($controlnumbersql->TableHash($dbh))[0];
	}

	# Set the payorcontrolnumber2 for Medicare A/B/DME for Balanceforward and Readj notice
	if ($associatedrecord->{CONTROLNUMBER}) {
		$self->SetPayorControlNumber2(
			$dbh,
			{
				CLAIMASSOCIATEDERARECORDID => $associatedrecord->{ERARECORDID},
				CLAIMASSOCIATEDCONTROLNUMBER => $associatedrecord->{CONTROLNUMBER},
				BATCHEXCEPTIONID => $erarecord->{ID},
				ERABATCHID => $erarecord->{ERABATCHID}
			}
		);

		# Set fields on BatchException ERA Record
		my @cols = qw(CONTROLNUMBER PATIENTFIRSTNAME PATIENTLASTNAME PATIENTINSURANCEIDNUMBER FROMDATE);
		if ((($erarecord->{NOTE} =~ /\(claim \d+V\d+\)\.$/s) && ($erarecord->{NOTE} !~ /\(claim $associatedrecord->{CONTROLNUMBER}\)\.$/s)) || ($erarecord->{NOTE} !~ /\(claim \d+V\d+\)\.$/s)) {
			push @cols, 'NOTE';
			$erarecord->{NOTE} =~ s/(\(claim \d+V\d+\))?\.$/ (claim $associatedrecord->{CONTROLNUMBER})./s;
		}

		$erarecord->{operation} = 'Update';
		$erarecord->{FROMDATE} = $associatedrecord->{FROMDATE};
		$erarecord->{CONTROLNUMBER} = $associatedrecord->{CONTROLNUMBER};
		$erarecord->{PATIENTFIRSTNAME} = $associatedrecord->{PATIENTFIRSTNAME};
		$erarecord->{PATIENTLASTNAME} = $associatedrecord->{PATIENTLASTNAME};
		$erarecord->{PATIENTINSURANCEIDNUMBER} = $associatedrecord->{PATIENTINSURANCEIDNUMBER};
		ProcessForm('ERARECORD', $dbh, $args->{USERNAME}, $erarecord, \@cols);
	}

	return HashSlice($associatedrecord, [qw(MATCHED CLAIMID DISCARDED ERARECORDID CONTROLNUMBER TRANSFERTYPE)]);

}

#########################################################################################
# SetPayorControlNumber2
#
# Description:
#	Set PayorControlNumber2 for batchexception and unpostable
#	based upon the claim association record
#	- Cuurently, this is done only for Medicares(Medicare A/B/DME)
#
# Parameters:
#	(required)
#		CLAIMID - Associated claimid with reversal/readjudication record
#		ERARECORDID - Batchexception record
#
# Return Value:
#	none
#########################################################################################
sub SetPayorControlNumber2 {
	my ($self, $dbh, $args) = @_;

	return;
}

#########################################################################################
# UpdateUnpostableFieldsUsingClaimID
#
# Description:
#	- Update the following fields of unpostable if we can identify claimid
#	  - Claimid
#	  - Provider ID
#	  - Provider Group ID
#	  - Medical Group ID
#
# Parameters:
#	(required)
#		CLAIMID - Claimid using which the other fields are identified
#		ERARECORDID - will be used to pull the unpostable to be updated.
#
# Return Value:
#	Updated Unpostable ID
#########################################################################################
sub UpdateUnpostableFieldsUsingClaimID {
	my ($self, $dbh, $args) = @_;

	AssertRequiredFields($args, ['CLAIMID', 'ERARECORDID']);

	my $claimid = $args->{CLAIMID};
	my $erarecordid = $args->{ERARECORDID};
	my $contextid = $args->{CONTEXTID} || GetPracticeID();
	my $medicalgroupid;

	my ($unpostableid, $unpostabletypeid) = SQL::Select->new()->Select(
			"id",
			"unpostabletypeid",
		)->From(
			"unpostable",
		)->Where(
			["unpostable.erarecordid = ?", $erarecordid],
			"unpostable.voided is null",
	)->Values($dbh);

	return unless $unpostableid;

	my ($providerid, $providergroupid) = SQL::Select->new()->Select(
			"claim.supervisingproviderid",
			"provider.providergroupid",
		)->From(
			"claim",
			"provider",
		)->Where(
			["claim.id = ?", $claimid],
			"claim.supervisingproviderid = provider.id",
	)->Values($dbh);

	if ($providerid) {
		require Unpostable::ProviderMedicalGroupLookup;
		$medicalgroupid = Unpostable::ProviderMedicalGroupLookup::GetMedicalGroupFromProviderID($dbh, {
			PROVIDERID => $providerid,
			CONTEXTID => $contextid,
		});
	}

	# update unpostable fields
	Unpostable::Update($dbh, {
		ID => $unpostableid,
		POSTDATE => AthenaToday(),
		PROVIDERID => $providerid,
		PROVIDERGROUPID => $providergroupid,
		MEDICALGROUPID => $medicalgroupid,
		UNPOSTABLETYPEID => $unpostabletypeid,
		CLAIMID => $claimid,
		USERNAME => $args->{USERNAME},
	});

	return $unpostableid;

}

#########################################################################################
# AddClaimAndUnpostableNotesUsingRuleID
#
# Description:
#	- Update the following fields of unpostable if we can identify claimid
#	  - Claimid
#	  - Provider ID
#	  - Provider Group ID
#	  - Medical Group ID
#
# Parameters:
#	(required)
#		CLAIMID - Claimid using which the other fields are identified
#		ERARECORDID - will be used to pull the unpostable to be updated.
#
# Return Value:
#	none
#########################################################################################
sub AddClaimAndUnpostableNotesUsingRuleID {
	my ($self, $dbh, $args) = @_;

	my $ruleid = $args->{RULEID};
	my $erarecord = $args->{ERARECORD};
	my $transfertype = $args->{TRANSFERTYPE};
	my $erarecordid = $erarecord->{ID};
	my $krcid = $erarecord->{KICKREASONCATEGORYID};
	my $claimid = $args->{CLAIMID};
	my $controlnumber = $args->{CONTROLNUMBER};
	my $username = $args->{USERNAME};
	my $associatedrecordid = $args->{ASSOCIATEDRECORDID};
	my $unpostabletypeid = $args->{UNPOSTABLETYPEID};
	my $matched = $args->{MATCHED};
	my $discarded = $erarecord->{DISCARDED};
	my $practiceid = GetPracticeID();

	my ($unpostableid, $unpostablestatusid, $paymentbatchid) = SQL::Select->new()->Select(
		"unpostable.id",
		"unpostable.unpostablestatusid",
		"unpostable.originalpaymentbatchid",
	)->From(
		"unpostable",
	)->Where(
		["unpostable.erarecordid = ?", $erarecordid],
		"unpostable.voided is null",
	)->Values($dbh);

	return if ($associatedrecordid && !$matched && !$discarded);

	my $claimnote;
	my $unpostablenote;

	# Make sure to check associated record discarded as RTFS while choosing athena Vs non-athena note.
	my $isrtfs = SQLValues("select 1 from unpostable where unpostabletypeid = 'ROUTETOOTHER' and erarecordid = ?", $dbh, $associatedrecordid);

	# If the associated record is RTFS use non-athena pcn note else use athena pcn note
	my $type = $isrtfs ? 'nonathenapcnnoteid' : 'athenapcnnoteid';

	my $sql = SQL::Select->new()->Select(
		)->From(
			"pcnnote",
			"eradiscardruleview",
		)->Where(
			["eradiscardruleview.id = ?", $ruleid],
			"eradiscardruleview.$type = pcnnote.id(+)",
			"pcnnote.deleted is null",
			"eradiscardruleview.deleted is null",
		);

	# Capture the Payorname from KRC. If not present, just use 'Payor'
	my $krcname = SQLValues("select name from kickreasoncategory where id = ?",$dbh, $krcid);
	$krcname =~ s/^X12:\w+ (.+?)( Specific)?$/$1/;
	my $payorname = $krcname;
	$payorname ||= 'Payer';

	if ($matched) {
		($claimnote, $unpostablenote) = $sql->Select(
			"pcnnote.creationclaimnote",
			"decode(eradiscardruleview.unpostablenoteoverride, null, pcnnote.creationunpostablenote, eradiscardruleview.unpostablenoteoverride)",
		)->Values($dbh);
		$claimnote = $unpostablenote unless ($claimnote);

		# Replace the magic words from the template with relevant values
		# Like : [[PAYORNAME]] by the actual payor name
		# [[ATHENACLAIMID]] with claimid
		if ($payorname) {
			$claimnote =~ s/\[\[PAYORNAME\]\]/$payorname/g;
			$unpostablenote =~ s/\[\[PAYORNAME\]\]/$payorname/g;
		}
		if ($claimid) {
			my $basehref = BusCall::PracticeStructure::GetBaseHREF($dbh, $practiceid);
			$unpostablenote =~ s/\[\[ATHENACLAIMID\]\]/<a href=$basehref\/billing\/hcfa.esp?ID=$claimid>${claimid}V$practiceid<\/a>/g;
		}
		$claimnote =~ s/\[\[CONTROLNUMBER\]\]/$controlnumber/g if defined $controlnumber;
	}
	else {
		($unpostablenote) = $sql->Select(
			"decode(eradiscardruleview.unpostablenoteoverride, null, pcnnote.creationunpostablenote, eradiscardruleview.unpostablenoteoverride)",
		)->Values($dbh);
		$unpostablenote =~ s/\[\[ATHENACLAIMID\]\]/unknown/;
		$unpostablenote =~ s/\[\[PAYORNAME\]\]/$payorname/g;
		$unpostablenote =~ s/\[\[CONTROLNUMBER\]\]/$controlnumber/g if defined $controlnumber;
	}

	# Replace any magic words with 'UNKNOWN'(incase if something cant be replaced with actual values)
	$unpostablenote =~ s/\[\[[A-Z]+\]\]/UNKNOWN/gs;

	# Update the notes for Unpostable
	Unpostable::AddNote($dbh, {
		UNPOSTABLEID => $unpostableid,
		NOTE => $unpostablenote,
		USERNAME => $username,
	}) if $unpostableid;

	if ($matched && $claimid) {
		my $kickcode;
		# Add kickcode to the claimnote if the unpostabletype is either READJNOTICE or REFUNACK
		if ($unpostabletypeid =~ /^(READJNOTICE|REFUNDACK)$/) {
			$kickcode = ($unpostabletypeid eq 'READJNOTICE') ? 'READJUDICATION' : 'REFUNDACK';
		}
		$self->ApplyClaimNote($dbh, {
			USERNAME => $username,
			CLAIMID => $claimid,
			POSTDATE => AthenaToday(),
			UNPOSTABLEID => $unpostableid,
			UNPOSTABLECONTEXTID => $practiceid,
			TRANSFERTYPE => $transfertype,
			ACTION => 'POSTINGRULE',
			PAYMENTBATCHID => $paymentbatchid,
			KICKREASONCATEGORYID => $krcid,
			NOTE => "<br>$claimnote",
			IMAGEFILEPAGENUMBER => $erarecord->{IMAGEFILEPAGENUMBER},
			ATHENAKICKCODE => $kickcode,
		});
	}

	return $unpostablestatusid eq 'CLOSED' ? '' : $unpostableid; # If Unpostable is closed don't return it's id

}

#########################################################################################
# GlobalRTFSExclusions
#
# Description:
#	Exclusions for Global RTFS
#	Hydra 1073062 - NOPOTENTIALCHARGE: Discard NMCP Auto as RTFS Auto (with exceptions)
#
# Parameters:
#	$dbh
#	ERARECORD	hashref
#
# Return Value:
#       1 if all conditions are passed
#########################################################################################

sub GlobalRTFSExclusions {
	my ($self, $dbh, $args) = @_;
	AssertRequiredFields($args, ['ERARECORD']);

	my $erarecord = $args->{ERARECORD};
	my $partialposttoggle = $args->{PARTIALPOSTTOGGLE};
	my $paymentbatchrouteid = $args->{PAYMENTBATCHROUTEID};

	my $contextid = SessionInfo($dbh)->{context};
	my $eraformatid = $self->{PAYMENTBATCHROUTE}->{ERAFORMATID};
	return if $eraformatid eq 'ABP';

	my $plbreasoncode = $erarecord->{PLBREASONCODE};
	return if $plbreasoncode;

	my $krcid = $self->{KICKREASONCATEGORY}->{ID};
	my @krcidstoexclude = SQLColumnValues("select id from kickreasoncategory where deleted is null and (name like '%Workers'' Comp%' or name like '%Workers Comp%' or name like '%WC%' or name like '%MVA%' or name like '%Legal%' or name like '%Vision%')", $dbh);
	return if (InList($krcid, @krcidstoexclude));

	my $contronumber = $erarecord->{CONTROLNUMBER};
	my $procedurecode = $erarecord->{PROCEDURECODE};
	my $claimid = $erarecord->{CLAIMID};

	if ($args->{CLAIMID} && !$claimid) { 
	   $claimid = $args->{CLAIMID};
	}

	my $patientid = $erarecord->{PATIENTID};
	$patientid = SQLValues("select patientid from claim where id = ?", $dbh, $claimid) if ($claimid && !$patientid);
    
	# CRZS-14502; Exclude E control nbrs from RTFS if there is a match
	my %defaultconf = (
		enabled => 0,
                contexts => [],
	);
	my $confdata = Athena::Conf::AthenaNet::AthenaXConf()->get("rollout.colpci.COLPCI_3372_EXCLUDE_E_CONTROLNUMBER") // \%defaultconf;
        my $confcontexts = (defined $confdata->{contexts}) ? $confdata->{contexts} : $defaultconf{contexts};
	my $excludeectrlnbrenabled = (defined $confdata->{enabled}) ? $confdata->{enabled} : $defaultconf{enabled};

	if ( $excludeectrlnbrenabled && InList($contextid, @{$confcontexts}) && $erarecord->{CONTROLNUMBER} =~ /^E(\d+)$/ && $claimid ) {
		my $econtrolnbrcheck =  SQL::Select->new(
		)->Select(
			'1',
		)->From(
			"CLIENTRECORDNUMBER",
			"RECORDNUMBERCATEGORY",
			"ERARECORD",
		)->Where(
			"CLIENTRECORDNUMBER.RECORDNUMBERCATEGORYID = RECORDNUMBERCATEGORY.ID",
			"RECORDNUMBERCATEGORY.NAME = 'Epic ID #'",
			"CLIENTRECORDNUMBER.CLIENTRECORDNUMBER = ERARECORD.CONTROLNUMBER",
			["CLIENTRECORDNUMBER.PATIENTID = ?", $patientid],
			["ERARECORD.ID = ?" , $erarecord->{ID}],
		)->Values($dbh);
		return if $econtrolnbrcheck;
		# return 2 if CSN number not matched, handled only for Aetna  
		return 2 if $partialposttoggle && $paymentbatchrouteid == 12;  
	}

	my $extrafieldidentifiercontext;
    	my @practiceids;
	my $toggleenabled;
	my $conf=Athena::Conf::AthenaNet::Rollout('colpci');
    	my $extrafieldidentifier = $conf->{COLPCI_262_EXTRAFIELD_IDENTIFIER};
    	if ($extrafieldidentifier) {
       		@practiceids =@{$conf->{colpci262extrafieldidentifier_contexts} || []};
       		if ((scalar @practiceids) > 1) {
            		$extrafieldidentifiercontext = (InList($contextid, @practiceids)) ? 1 : 0;
      	 }
    		}
	$toggleenabled= ($extrafieldidentifier && $extrafieldidentifiercontext) ? 1 : 0;
    	if ($toggleenabled && $contronumber !~ /^\d+[vV]\d+$/ && $claimid ) {
		my $extrafieldcheck = SQL::Select->new(
		)->Select(
			'1',
		)->From(
			'extraclaimfield'
		)->Where(
			["extraclaimfield.claimid = ? ", $claimid],
			"extraclaimfield.fieldname = 'CLIENTSYSTEMIDENTIFIER'",
			["extraclaimfield.data like ? ", "\%$contronumber"],
		)->Values($dbh);

		return if $extrafieldcheck;
		# return 2 if CSN number not matched, handled only for Aetna  
		return 2 if $partialposttoggle && $paymentbatchrouteid == 12;  

	}

	if ($contronumber !~ /^$claimid(?:$contextid)?$|^$patientid[aA]?$contextid$|^$patientid$|^\D+$|^0+$|^\d+V[a-zA-Z0-9]+$/
		&& $procedurecode !~ /^0+$|^[GT]/
		) {
			return 1;
	}
	return;
}

#########################################################################################
# MatchBasedOnMemberId
# 	Matches the charges and claims using the claim id that has been received through
# 	remittance and makes use of the member id based logic for the insurance matching
#
# Parameters:
# 	Required:
# 		CLAIMID: ID of the claim for which match has to be performed
# 		CHAGRES: List of remittance charges that has been receied in the remittance
#
# Return Value:
# 	None.
# 	Matched details would be set in the respective charges
#########################################################################################
sub MatchBasedOnMemberId {
	my ($self, $dbh, $args) = @_;
	#	warn sprintf "[PROOF:MBI:BEGIN] claimId=%s charges=%d", ($claimid//''), scalar(@{$charges});
	AssertRequiredFields($args, ['CLAIMID', 'CHARGES']);

	my $claimid = $args->{CLAIMID};
	my $charges = $args->{CHARGES};

	# Check if all charges within the claim has been matched, if matched do nothing and return
	my @unmatchedcharges = grep { $_->{MATCHED} eq '' } @{$charges};
	if (scalar @unmatchedcharges <= 0) {
		return;
	}

	my $pcninfo = BusCall::Remittance::GetPCNInfo($dbh, {
		CLAIMID => $claimid,
		FETCHALLINSURANCES => 1,
	});

	if ($pcninfo->{STATUS} ne 'SUCCESS' || scalar @{$pcninfo->{RESULT}} != 1) {
		warn sprintf "[PROOF:MBI:NO-CLAIMINFO] claimId=%s status=%s resultCount=%d",
        ($claimid//''), ($pcninfo->{STATUS}//'undef'), scalar(@{$pcninfo->{RESULT}//[]});
		return;
	}

	if (scalar @{$pcninfo->{RESULT}[0]->{CLAIMS}} != 1) {
		warn sprintf "[PROOF:MBI:MULTI-CLAIMS] claimId=%s count=%d",
        ($claimid//''), scalar(@{$pcninfo->{RESULT}[0]->{CLAIMS}//[]});
		return;
	}

	my $claim = $pcninfo->{RESULT}[0]->{CLAIMS}[0];

	my @transactionstomatch = _GetTransactionsToMatch({
		CLAIM => $claim,
	});

	if (scalar @transactionstomatch == 0) {
		return;
	}

	my @matchedcharges;
	foreach my $charge (@{$charges}) {
		if ($charge->{MATCHED}) {
			warn sprintf "[PROOF:MBI:TRY] rec=%s proc=%s subproc=%s amt=%s from=%s to=%s",
        ($charge->{ID}//''), ($charge->{PROCEDURECODE}//''), ($charge->{SUBMITTEDPROCEDURECODE}//''),
        ($charge->{AMOUNT}//''), ($charge->{FROMDATE}//''), ($charge->{TODATE}//'');
			next;
		}

		my $matchedtransaction = _MatchTransaction({
			CHARGE => $charge,
			TRANSACTIONS => \@transactionstomatch,
		});

		warn sprintf "[PROOF:MBI:TXNS] claimId=%s txnCount=%d", ($claimid//''), scalar(@transactionstomatch);
		if ($matchedtransaction) {
        warn sprintf "[PROOF:MBI:TXN-MATCH] rec=%s txnProc=%s txnAmt=%s txnStart=%s txnEnd=%s txnChargeId=%s hasChargeId=%s",
            ($charge->{ID}//''), ($matchedtransaction->{PROCEDURECODE}//''), ($matchedtransaction->{AMOUNT}//''),
            ($matchedtransaction->{STARTDATE}//''), ($matchedtransaction->{ENDDATE}//''),
            ($matchedtransaction->{CHARGEID}//''), (exists $matchedtransaction->{CHARGEID} ? 1 : 0);
    } else {
        warn sprintf "[PROOF:MBI:TXN-NOMATCH] rec=%s", ($charge->{ID}//'');
    }
		if ($matchedtransaction && _SameDOS({CHARGE => $charge, TRANSACTION => $matchedtransaction})) {
			$charge->{CHARGEID} = $matchedtransaction->{CHARGEID};
			warn sprintf "[PROOF:MBI:CHARGEID:WRITE] rec=%s chargeid=%s sourceField=%s",
            ($charge->{ID}//''), ($charge->{CHARGEID}//''), (exists $matchedtransaction->{CHARGEID} ? 'CHARGEID' : 'MISSING');
			push @matchedcharges, $charge;
		} else {
        warn sprintf "[PROOF:MBI:SKIP-DOS] rec=%s", ($charge->{ID}//'');
    }
	}

	if (scalar @matchedcharges >= 1) {
		my $insurances = $pcninfo->{RESULT}[0]->{PATIENTINSURANCES};
		my $matchedinsurance = $self->MatchInsuranceUsingMemberId($dbh, {
			CLAIM => $claim,
			PATIENTINSURANCES => $insurances,
			CHARGE => $matchedcharges[0],
		});

		if ($matchedinsurance) {
			warn sprintf "[PROOF:MBI:INS-MATCH] piid=%s xfer=%s ip=%s",
            ($matchedinsurance->{PIID}//''), ($matchedinsurance->{TRANSFERTYPE}//''),
            ($matchedinsurance->{INSURANCEPACKAGE}{IPNAME}//'');
			foreach my $matchedcharge (@matchedcharges) {
				$matchedcharge->{PATIENTINSURANCEID} = $matchedinsurance->{PIID};
				$matchedcharge->{TRANSFERTYPE} = $matchedinsurance->{TRANSFERTYPE};
				$matchedcharge->{MATCHED} = 'SYSDATE';
				$matchedcharge->{MATCHEDBY} = $args->{USERNAME};
				$matchedcharge->{NOTE} .= " [PARTIALPTINSIDMATCH]";
				warn sprintf "[PROOF:MBI:FINALIZE] rec=%s matched=1 pi=%s xfer=%s chargeid=%s",
                ($matchedcharge->{ID}//''), ($matchedcharge->{PATIENTINSURANCEID}//''),
                ($matchedcharge->{TRANSFERTYPE}//''), ($matchedcharge->{CHARGEID}//'');
			}
		}
		else {
			warn "[PROOF:MBI:NO-INS] clearing chargeid for matched txn charges";
			# Unfortunately there was no match found, so reset the matched charges by removing the chargeid that was set
			foreach my $matchedcharge (@matchedcharges) {
				warn sprintf "[PROOF:MBI:CHARGEID:CLEAR] rec=%s reason=no-insurance-match", ($matchedcharge->{ID}//'');
				$matchedcharge->{CHARGEID} = '';
			}
		}
	}
}

sub _MatchTransaction {
	my ($args) = @_;

	my $charge = $args->{CHARGE};
	my $transactions = $args->{TRANSACTIONS};

	my $procedurecodetomatch = $charge->{SUBMITTEDPROCEDURECODE} ne ''
		? $charge->{SUBMITTEDPROCEDURECODE}
		: $charge->{PROCEDURECODE};
	if ($procedurecodetomatch eq '') {
		return;
	}

	my @matchingtransactions = _GetMatchingTransactions({
		TRANSACTIONS => $transactions,
		PROCEDURECODE => $procedurecodetomatch,
	});

	if (scalar @matchingtransactions <= 0) {
		return;
	}

	if (scalar @matchingtransactions == 1) {
		return $matchingtransactions[0];
	}

	# Resolve using charge amount if we have more than 1 matching transaction
	my @resolvedtransactions = grep { $_->{AMOUNT} eq $charge->{AMOUNT} } @matchingtransactions;

	if (scalar @resolvedtransactions == 1) {
		return $resolvedtransactions[0];
	}

	return;
}

sub _GetMatchingTransactions {
	my ($args) = @_;

	my $transactions = $args->{TRANSACTIONS};
	my $procedurecode = $args->{PROCEDURECODE};

	my @matchingtransactions = grep {$_->{PROCEDURECODE} eq $procedurecode} @{$transactions};

	if (scalar @matchingtransactions <= 0) {
		@matchingtransactions = _GetMatchingTransactionsUsingCPTModifiers({
			TRANSACTIONS => $transactions,
			PROCEDURECODE => $procedurecode,
		});
	}

	if (scalar @matchingtransactions <= 0) {
		@matchingtransactions = _GetMatchingTransactionsUsingCPT({
			TRANSACTIONS => $transactions,
			PROCEDURECODE => $procedurecode,
		});
	}

	return @matchingtransactions;
}

sub _GetMatchingTransactionsUsingCPT {
	my ($args) = @_;

	my $procedurecode = $args->{PROCEDURECODE};
	my $transactions = $args->{TRANSACTIONS};

	my $procedurecodewithmodifiers = _ExtractProcedureCodeAndModifier({
		PROCEDURECODE => $procedurecode,
	});

	my @matchingtransactions;

	foreach my $transaction (@{$transactions}) {
		if ($transaction->{PROCEDURECODE} eq '') {
			next;
		}

		my $txnproccodewithmodifiers = _ExtractProcedureCodeAndModifier({
			PROCEDURECODE => $transaction->{PROCEDURECODE},
		});

		if ($procedurecodewithmodifiers->{PROCEDURECODE} eq $txnproccodewithmodifiers->{PROCEDURECODE}) {
			push @matchingtransactions, $transaction;
		}
	}
	return @matchingtransactions;
}

sub _GetMatchingTransactionsUsingCPTModifiers {
	my ($args) = @_;

	my $procedurecode = $args->{PROCEDURECODE};
	my $transactions = $args->{TRANSACTIONS};

	my $procedurecodewithmodifiers = _ExtractProcedureCodeAndModifier({
		PROCEDURECODE => $procedurecode,
	});

	my @matchingtransactions;

	foreach my $transaction (@{$transactions}) {
		if ($transaction->{PROCEDURECODE} eq '') {
			next;
		}

		my $txnproccodewithmodifiers = _ExtractProcedureCodeAndModifier({
			PROCEDURECODE => $transaction->{PROCEDURECODE},
		});

		if ($procedurecodewithmodifiers->{PROCEDURECODE} eq $txnproccodewithmodifiers->{PROCEDURECODE}
			 	&& defined $procedurecodewithmodifiers->{MODIFIERS} && defined $txnproccodewithmodifiers->{MODIFIERS}
				&& scalar @{$procedurecodewithmodifiers->{MODIFIERS}} >= 0
				&& ListsSubsetOf($procedurecodewithmodifiers->{MODIFIERS}, $txnproccodewithmodifiers->{MODIFIERS})) {
			push @matchingtransactions, $transaction;
		}
	}
	return @matchingtransactions;
}

sub _ExtractProcedureCodeAndModifier {
	my ($args) = @_;

	my @procedurecodewithmodifier = split(/,/, $args->{PROCEDURECODE});

	my $length = scalar @procedurecodewithmodifier;
	my @modifiers = @procedurecodewithmodifier[1..$length-1];
	return {
		PROCEDURECODE => $procedurecodewithmodifier[0],
		MODIFIERS => \@modifiers,
	};
}

sub _GetTransactionsToMatch {
	my ($args) = @_;

	my $claim = $args->{CLAIM};
	my @transactionstomatch;

	if (scalar @{$claim->{PRIMARYCHARGES}} > 0) {
		push @transactionstomatch, @{$claim->{PRIMARYCHARGES}};
	}
	if (scalar @{$claim->{SECONDARYCHARGES}} > 0) {
		push @transactionstomatch, @{$claim->{SECONDARYCHARGES}};
	}
	if (scalar @{$claim->{PRIMARYTRANSFERINS}} > 0) {
		push @transactionstomatch, @{$claim->{PRIMARYTRANSFERINS}};
	}

	return @transactionstomatch;
}

sub _SameDOS {
	my ($args) = @_;
	my $charge = $args->{CHARGE};
	my $matchedtransaction = $args->{TRANSACTION};

	return ($charge->{FROMDATE} && $charge->{FROMDATE} eq $matchedtransaction->{STARTDATE}
		&& $charge->{TODATE} && $charge->{TODATE} eq $matchedtransaction->{ENDDATE});
}


#########################################################################################
# MatchInsuranceUsingMemberId
# 	Identifies the insurance and transfertype using the member ID that has been
# 	received in the remittance
#
# Parameters:
# 	Required:
# 		CLAIM: Hashref of claim detail that has been received through claim info
# 			api
# 		CHARGE: Hashsref of charge data (any charge that has been matched for the
# 			claim in consideration)
#
# Return Value:
# 	Insurance hash rf if there is a match found using member id
# 	Else none
#########################################################################################
sub MatchInsuranceUsingMemberId {
	my ($self, $dbh, $args) = @_;

	my $claim = $args->{CLAIM};
	my $charge = $args->{CHARGE};

	if (!$charge->{KICKREASONCATEGORYID}) {
		return;
	}

	if (!$charge->{CORRECTEDINSURANCEIDNUMBER} && !$charge->{PATIENTINSURANCEIDNUMBER} && !$charge->{SUBSCRIBERINSURANCEIDNUMBER}) {
		return;
	}

	my @matchedinsurances = _GetMatchedInsurances($args);

	if (scalar @matchedinsurances <= 0) {
		return;
	}

	return _ResolveInsurance({
		CLAIM => $claim,
		INSURANCES => \@matchedinsurances
	});
}

sub _ResolveInsurance {
	my ($args) = @_;
	my $claim = $args->{CLAIM};
	my $matchedinsurances = $args->{INSURANCES};

	my @primaryinsurance = grep { $_->{PIID} eq $claim->{PRIMARYINSURANCEID} } @{$matchedinsurances};
	my @secondaryinsurance = grep { $_->{PIID} eq $claim->{SECONDARYINSURANCEID} } @{$matchedinsurances};

	# Insurance is matched to both primary and secondary which means the insurance that we identify may be wrong,
	# so considering it as non-matched and will let manual user take the action on them
	if (scalar @primaryinsurance >= 1 && scalar @secondaryinsurance >=1) {
		return;
	}

	# This is just a sanity check, as we wouldn't get more than one insurance matched with either primary or secondary,
	# as currently a claim can have only one primary and one secondary insurance mapped to it
	if (scalar @primaryinsurance > 1 || scalar @secondaryinsurance > 1) {
		return;
	}

	my $matchedinsurance;
	if (scalar @primaryinsurance == 1) {
		$primaryinsurance[0]->{TRANSFERTYPE} = '1';
		$matchedinsurance = $primaryinsurance[0];
	}

	if (scalar @secondaryinsurance == 1) {
		$secondaryinsurance[0]->{TRANSFERTYPE} = '2';
		$matchedinsurance = $secondaryinsurance[0];
	}

	# Exclude BCBS insurances from the matching as it has routing logics and would most probably fail in this approach
	if ($matchedinsurance && $matchedinsurance->{INSURANCEPACKAGE}->{IPNAME} =~ /bcbs|blue/i) {
		return;
	}

	return $matchedinsurance;
}

sub _GetMatchedInsurances {
	my ($args) = @_;

	my $patientinsurances = $args->{PATIENTINSURANCES};
	my $charge = $args->{CHARGE};
	my @matchedinsurances;
	foreach my $insurance (@{$patientinsurances}) {
		my $idnumber = $insurance->{IDNUMBER};
		if (_MemberIdCheck({ IDNUMBER => $idnumber, REMITIDNUMBER => $charge->{CORRECTEDINSURANCEIDNUMBER}})
			|| _MemberIdCheck({IDNUMBER => $idnumber, REMITIDNUMBER => $charge->{PATIENTINSURANCEIDNUMBER}})
			|| _MemberIdCheck({IDNUMBER => $idnumber, REMITIDNUMBER => $charge->{SUBSCRIBERINSURANCEIDNUMBER}})
		) {
			if ($insurance->{PIID} && $insurance->{INSURANCEPACKAGE}->{KICKREASONCATEGORYID}
				&& $insurance->{INSURANCEPACKAGE}->{KICKREASONCATEGORYID} eq $charge->{KICKREASONCATEGORYID}
			) {
				push @matchedinsurances, $insurance;
			}
		}
	}

	return @matchedinsurances;
}

sub _MemberIdCheck {
	my ($args) = @_;
	my $idnumber = $args->{IDNUMBER};
	my $remitidnumber = $args->{REMITIDNUMBER};

	if (!$idnumber || !$remitidnumber || $remitidnumber =~ /^XXX/) {
		return;
	}

	($idnumber) =~ s/[^a-zA-Z0-9,]//g;
	($remitidnumber) =~ s/[^a-zA-Z0-9,]//g;

	return (($idnumber && length($idnumber) > 3 && index($remitidnumber, $idnumber) != -1)
		|| ($remitidnumber && length($remitidnumber)> 3 && index($idnumber, $remitidnumber) != -1));
}

################################################################################
#_Log
#Description :
#	- Add Scribe Logs
################################################################################
sub _Log {
	my ($msg) = @_;
	my $printmsg = "[" . POSIX::strftime("%m/%d/%Y %H:%M:%S", localtime()) . "] $msg\n";
	my $logs = AthenaScribe->new(
		category => "matchlog",
		logidentifier => "matchlog",
	);
	$logs->log(
		message => $printmsg,
		level => 'info',
	);
	return;
}

1;
