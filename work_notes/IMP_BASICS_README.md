# basic details about *context-id* & *claim-id*

* `claim-id`V`context-id` is the criteria for searching
* 4290- `context_id`
* ATHENA10275 - `context_id`

        if `claim-id` is not available, then we can find the files by searching it with 0V`context_id`

* `preprocess_script`- perl ~/dabhinab_streams/prod/scripts/app/practice/preprocess_erabatches.pl MTEST59 ATHENA31775 --erabatchids 363
* change the *`context`* in `Database`:
    1. \u <context-id>
* change the *`environment`* in `Database`
    1. `example`: **\u 31775@dtest59**
* *<era-batch-id>R<context-id>* is the criteria for searching

