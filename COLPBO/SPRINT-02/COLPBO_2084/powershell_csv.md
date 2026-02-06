the below command was utilized to filter the large CSV file and store the result in some other file
Import-Csv 'C:\Users\dabhinab\Downloads\COLPBO-2084-FINAL-TRIAL.csv' |
Where-Object { $_.transfertype -eq 1 -and $_.failurereason -eq "" -and $_.partialpostsignedoff -eq "" -and $_.distinctparentidcount -eq 1 -and $_.earlierbatchcount -eq 0 -and $_.fqhc_context -eq "" -and $_.appliedby -eq "ATHENA" } |
Select-Object -First 10000 |
Export-Csv 'C:\Users\dabhinab\Downloads\Filtered-Results.csv' -NoTypeInformation

