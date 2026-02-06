# FWRAPPEALFAIL validation

* *The value is stored in the **KICKREASONID** attribute of the claim note when applying a new claim note:*

    ```perl

        $self->ApplyClaimNote($dbh, {
            CLAIMID => $claimid,
            TRANSFERTYPE => $transfertype,
            KICKREASONID => $fwrappealfail,
            PAYMENTBATCHID => $args->{PAYMENTBATCHID},
        });
    ```

* Here, ***$fwrappealfail*** is retrieved from the kickreason table where kickcode = `FWRAPPEALFAIL`.
* *SQL query* to find the ***kick-reason-id*** from the DB:

    ```SQL
        SELECT
            id
        FROM
            kickreason
        WHERE
            kickreason.kickcode = 'FWRAPPEALFAIL'
        AND kickreason.deleted is null
        AND kickreason.kickreasoncategoryid = 0;
    ```

* This means ***BDEFWRAPPEALFAIL*** is stored under the KICKREASONID attribute in the claim.
