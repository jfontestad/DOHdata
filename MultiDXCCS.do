/************************************************************************************
Program Name: MultiDXCCS.do 
Description : Assign Multi-Level Diagnosis CCS category arrays (L1dccs# L2dccs# L3dccs# L4dccs#) from Stata. 
Developed   : By Bob Houchens
Updated     : By David Ross on 10/27/2009.
************************************************************************************/

* Generate a unique identifier
*
egen _obs = seq()
*
* Reshape the data into long format with one observation per diagnosis
*Lin: since only dx1 is used, long and wide formats are the same
*reshape long dx, i(_obs) j(dxnum)

*
* Generate a temporary diagnosis variable that will be reformatted by the clean function in preparation for the merge
*
generate _dx = dx
*
* Check the validity of the diagnosis
*
capture: icd9 check _dx, generate(invalid)
*
* replace invalid temporary diagnoses in preparation for the clean function
*
replace _dx="0000" if invalid > 0 & invalid < 10
drop invalid
*
* Format the temporary diagnosis with a decimal to match the format in singleDXCCS.  Sort by formatted diagnosis.
*
icd9 clean _dx, dots
sort _dx
compress
*
* Merge the multi-level CCS category variables (L1dccs L2dccs L3dccs L4dccs) that match the temporary diagnosis
*
merge _dx using multiDXCCS, nokeep
*
* Drop temporary variables and put data in original shape
*
drop _merge _dx
reshape wide dx L1dccs L2dccs L3dccs L4dccs, i(_obs) j(dxnum)
drop _obs

