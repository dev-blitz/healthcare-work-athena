# test-notes [COLPCI-4876-RT-code_fix-3997]

* `context-id`: 10275

* `Gear` -> `Practice Manager` -> `ERA File Search` -> fetch the file with ERA Batch ID -> `Download`

* `ERA-batch-id`: 962467

* earlier files could only be downloaded if the Toggle was `OFF` and produced the ***error***:

`Error`:

        Died at /home/larunkumar/larunkumar_streams/prod/htdocs/practiceadmin/erafilesearch.esp line 84.
        , /home/larunkumar/larunkumar_streams/prod/perllib/Athena/Apache/ASP.pm line 1538

* removed the toggle, now the file can be normally downloaded after fetching it with `era-batch-id` and then downloading it.

* the file can now be downloaded with special character without any error.
