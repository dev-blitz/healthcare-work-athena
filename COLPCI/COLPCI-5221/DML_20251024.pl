my ($patchdate) = $patchfilenodir =~ /^(\d+).*_dml[.]pl/;
my $patchuser = "DBA_PATCH_$patchdate";

%dxl = (
  PRACTICEDXL => [
    {
      # DML for COLPCI-5221
      MESSAGE => 'COLPCI-5221: Update for PAYMENTBATCH: 25120A30195',
      OWNEREMAIL => 'dabhinab@athenahealth.com,SECOLPCIDev@athenahealth.com',
      PRACTICEID => '30195',
      DML => qq{
        update
          ERARECORD
        set
          CHARGEID = '76582',
          CLAIMID = '15030',
          MATCHED = '',
          MATCHEDBY = '',
          lastmodified = sysdate,
          lastmodifiedby = '$patchuser'
        where
          ID in (142955)
            and MATCHED is not null
      },
    },
    {
      # DML for COLPCI-5290
      MESSAGE => 'COLPCI-5290: Update for PAYMENTBATCH: 25460A30195',
      OWNEREMAIL => 'dabhinab@athenahealth.com,SECOLPCIDev@athenahealth.com',
      PRACTICEID => '30195',
      DML => qq{
        update
          ERARECORD
        set
          CHARGEID = '1036516',
          CLAIMID = '567228',
          MATCHED = '',
          MATCHEDBY = '',
          lastmodified = sysdate,
          lastmodifiedby = '$patchuser'
        where
          ID in (145020)
            and MATCHED is not null
      },
    },
    {
      # DML for COLPCI-5290
      MESSAGE => 'COLPCI-5290: Update for PAYMENTBATCH: 25460A30195',
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
          lastmodified = sysdate,
          lastmodifiedby = '$patchuser'
        where
          ID in (145035)
            and MATCHED is not null
      },
    },
  ],
);