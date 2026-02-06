# DML-UPDATE-STEPS

* create a file in the below location and with next `BI-WEEKLY` date in the directyory name:

dabhinab@preprod512511:~/dabhinab_streams$ cat techops/collector/dailydml/20251008_145035_ERABATCH_dml.pl
my ($patchdate) = $patchfilenodir =~ /^(\d+).*_dml[.]pl/;
my $patchuser = "DBA_PATCH_$patchdate";

%dxl = (
  PRACTICEDXL => [
    {
      MESSAGE => 'COLPCI-5221 & COLPCI-5290: Update for PAYMENTBATCHES 25120A30195 & 25460A30195',
      OWNEREMAIL => 'dabhinab@athenahealth.com,SECOLPCIDev@athenahealth.com',
      PRACTICEID => '30195',
      DML => qq{
        update
          ERARECORD
        set
          CHARGEID = '1036533',
          CLAIMID = '567243',
          MATCHED = '',
          MATCHEDBY = '',
          ERADISCARDSTATUSREASONID = '',
          ERADISCARDSTATUSID = ''
        where
          ID in (145035)
      },
    },
  ],
);

