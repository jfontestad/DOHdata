/************************************************************************************
Program Name: MultiDXCCSLoad.do 
Description : Load MultiDXCCS.dta file used to assign Multi-Level DXCCS categories from Stata. 
Developed   : By David Ross on 10/26/2009.
************************************************************************************/

local fyear     "2012"
local tool      MultiDXCCS
local prefix    ccs_multi_dx_tool_
local ivar      dx
local ovar      L1dccs L2dccs L3dccs L4dccs

if `"`ivar'"' == "pr" local icd9f icd9p
else local icd9f icd9

* Remove first line from file
infix str lineA    1-200    ///
      str lineB  201-400    ///
      using `prefix'`fyear'.csv in 2/L, clear
outfile lineA lineB using `prefix'`fyear'Stata.csv, runtogether replace

infile str6    _`ivar'       ///
       str2    L1dccs        ///
       str100  L1Description ///
       str5    L2dccs        ///
       str100  L2Description ///
       str8    L3dccs        ///
       str100  L3Description ///
       str11   L4dccs        ///
       str100  L4Description ///
       using `prefix'`fyear'Stata.csv, clear

replace L1dccs = trim(L1dccs)
replace L2dccs = trim(L2dccs)
replace L3dccs = trim(L3dccs)
replace L4dccs = trim(L4dccs)

`icd9f' clean _`ivar', dots
sort _`ivar'
keep _`ivar' `ovar'
save `tool', replace

