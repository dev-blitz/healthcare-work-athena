#DESCRIPTION
As a developer, I want to find examples for these special handling scenarios and prepare files and create scenario in the RRT.

##Special handling - ***FWRAPPEALFAIL***
###Description - 
If the *claim* received a prior *BDE denial* for the *current IRC*, 
An *appeal* was submitted since the prior BDE, 
and current BDE occurred at least seven days since the appeal

Total claim charge is less than $5,000. 
The patient insurance ID on the appeal note is associated with the same insurance reporting category as both BDE notes, add claimnote with 
*BDEFWRAPPEALFAIL*.;

##Function Name -
    *ERA::Apply::_AppliedClaimCheckClaimStatus*
