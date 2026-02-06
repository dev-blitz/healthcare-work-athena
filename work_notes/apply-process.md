###apply-process

# schemas in 'athena'

- practice
  - in `athena` each practice is maintained as a *database-instance*, this is known as `practice-schema`
- queue
  - Its neighter  transactional data, nor master data,oOnce the job is done, whatever data that will come here will be wiped off.
- root
  - the common things common for all the practices are maintained in the `root`
  - everything will be copied to the practice-schemas
  - `practice` can access root, cannot write on root
  - in the DB, we can identify it as `ATHENA1`
- rootnosnap
  - contents will never be copied to the practice-schemas
  - contents can never be accessed from the practise-schamas
  - can be identified in the DB as `ATHENA1NOSNAP`
