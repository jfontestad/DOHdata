*creating multi-level dx CCS, for preparing Vista data
set more off
clear

*create multiDXCCS.dta file
*do multiDXCCSLoad		/*use ccs_multi_dx_too_2012.csv, generated MultiDXCCS.dta*/
*=================================
*-----Recode dx1 from ICD-9 codes to CCS codes-----
use V:\DATA\HOSP\Stata\data\spub`1', clear
keep if COUNTYRES=="17"
keep SEQ_NO DIAG1
rename SEQ_NO seq_no
rename DIAG1 dx
gen dxnum=1
do multiDXCCS
order seq_no dx1 L1dccs1 L2dccs1 L3dccs1 L4dccs1

gen double L1dccsx=real(L1dccs1)+10
gen L2dccs_p2=real(substr(L2dccs1,-2,.))
replace L2dccs_p2=L2dccs_p2*10 if L2dccs_p2<1
gen double L2dccsx=L1dccsx*100+L2dccs_p2
gen L3dccs_p3=real(substr(L3dccs1,-2,.))
replace L3dccs_p3=L3dccs_p3*10 if L3dccs_p3<1
gen double L3dccsx=L2dccsx*100+int(L3dccs_p3)
gen L4dccs_p4=real(substr(L4dccs1,-1,.))
gen double L4dccsx=L3dccsx*10+L4dccs_p4

replace L1dccsx=L1dccsx*100000
replace L2dccsx=L2dccsx*1000
replace L3dccsx=L3dccsx*10
format L1dccsx L2dccsx L3dccsx L4dccsx %12.0f
compress
save temp_m_dx1CCS`1', replace

*======creating stacked file=====
forvalue i=1/4 {
	use temp_m_dx1CCS`1', clear
	keep seq_no L`i'dccsx
	drop if L`i'dccsx==.
	rename L`i'dccsx subset
	sort seq_no
	save temp_mdxlev`i'_`1', replace	
}

*-----Recode Ecodes-----
use V:\DATA\HOSP\Stata\data\spub`1', clear
keep if COUNTYRES=="17"
keep SEQ_NO ECODECAL
rename SEQ_NO seq_no
gen ecode=substr(ECODECAL,2,4)
gen ecodex=real(substr(ecode,1,3))
gen persinj=real(substr(ecode,4,1))

do cr_ecode1
sort seq_no
compress
save temp_m_dxecode`1', replace

keep seq_no cause
drop if cause==.
rename cause subset
sort seq_no
save temp_ecause`1', replace

use temp_m_dxecode`1', clear
keep seq_no mvcper
drop if mvcper==.
rename mvcper subset
sort seq_no
save temp_mvcper`1', replace

use temp_m_dxecode`1', clear
keep seq_no inj
drop if inj==.
rename inj subset
sort seq_no
save temp_inj_intent`1', replace

use temp_m_dxecode`1', clear
keep seq_no inj1
drop if inj1==.
rename inj1 subset
sort seq_no
save temp_inj1_`1', replace

use temp_m_dxecode`1', clear
keep seq_no inj2
drop if inj2==.
rename inj2 subset
sort seq_no
save temp_inj2_`1', replace

use temp_m_dxecode`1', clear
keep seq_no inj3
drop if inj3==.
rename inj3 subset
sort seq_no
save temp_inj3_`1', replace

use temp_m_dxecode`1', clear
keep seq_no inj4
drop if inj4==.
rename inj4 subset
sort seq_no
save temp_inj4_`1', replace

use temp_m_dxecode`1', clear
keep seq_no inj5
drop if inj5==.
rename inj5 subset
sort seq_no
save temp_inj5_`1', replace

do cr_multi1b `1'
*do cr_multi1c `1'	/*the old ACS conditions are not used */
do cr_multi1d `1'	/*PQI indicators are run using WinQI*/

*=====append dx level data, special dx data, and ecode data=====
use temp_mdxlev1_`1', clear
append using temp_mdxlev2_`1'
append using temp_mdxlev3_`1'
append using temp_mdxlev4_`1'
append using temp_ecause`1'
append using temp_mvcper`1'
append using temp_inj_intent`1'
append using temp_inj1_`1'
append using temp_inj2_`1'
append using temp_inj3_`1'
append using temp_inj4_`1'
append using temp_inj5_`1'
append using temp_sp9250_`1'
append using temp_sp9371_`1'
append using temp_sp9372_`1'

*append using acsx_`1'
*append using acsall_`1'
*append using acs23_`1'
*append using acs24_`1'

append using temp_pqi_indiv_`1'
append using temp_pqi90_`1'
append using temp_pqi91_`1'
append using temp_pqi92_`1'

sort seq_no
save temp_mdxlevels`1', replace

*============================================
*-----Recode Demographics-----
*match with UW file to get person ID for undplicated analysis
use V:\DATA\HOSP\Stata\data\spub`1', clear
keep if COUNTYRES=="17"
keep SEQ_NO ZIPCODE AGE SEX
rename SEQ_NO seq_no
rename ZIPCODE zipcode
rename AGE age
merge 1:1 seq_no using V:\DATA\HOSP\Stata\UW\uw8709
tab1 _merge
drop if _merge==2
gen seq_no2=real(seq_no)
replace uwid=seq_no2 if _merge==1
drop pat_pgap pat_tot pat_sgap match pat_num _merge seq_no2

recode age 0=0 1/4=104 5/9=509 10/14=1014 15/17=1517 18/19=1819 20/24=2024 25/29=2529 ///
 30/34=3034 35/39=3539 40/44=4044 45/49=4549 50/54=5054 55/59=5559 60/64=6064 65/69=6569 ///
 70/74=7074 75/79=7579 80/84=8084 85/120=8599 .=9999, gen(agegrp)
 
gen sex=.
replace sex=1 if SEX=="M"
replace sex=2 if SEX=="F" 
drop SEX
sort seq_no
compress
save temp_chars`1'pop, replace

merge 1:m seq_no using temp_mdxlevels`1'
keep if _merge==3
drop _merge
compress
save temp_chars`1'a, replace

*=====aggregate data=====
local vistalist year place race sex agegrp subset count undupl

*-----1. aggregate by sex and age, all subset, all places-----
use temp_chars`1'pop, clear
gen one=1
sort uwid
by uwid: gen idseq=_n
recode idseq (1=1) (*=.)
sort sex agegrp
by sex agegrp: gen seq=_n
by sex agegrp: egen count=count(one)
by sex agegrp: egen undupl=count(idseq)
keep if seq==1
gen year=`1'
gen place=0
gen race=0
gen subset=0

keep `vistalist'
order `vistalist'
save temp_chars`1'a1, replace

*-----2. Aggregate by sex, age, and subsets-----
use temp_chars`1'a, clear
gen one=1
sort uwid subset
by uwid subset: gen idseq=_n
recode idseq (1=1) (*=.)
sort sex agegrp subset
by sex agegrp subset: gen seq=_n
by sex agegrp subset: egen count=count(one)
by sex agegrp subset: egen undupl=count(idseq)
keep if seq==1
gen year=`1'
gen place=0
gen race=0

keep `vistalist'
order `vistalist'
save temp_chars`1'a2, replace

*-----3. aggregate by sex, age, zipcodes, for ALL SUBSETS-----
use temp_chars`1'pop, clear
gen one=1
sort uwid
by uwid: gen idseq=_n
recode idseq (1=1) (*=.)
sort sex agegrp zipcode
by sex agegrp zipcode: gen seq=_n
by sex agegrp zipcode: egen count=count(one)
by sex agegrp zipcode: egen undupl=count(idseq)
keep if seq==1
gen year=`1'
gen place=real(zipcode)
gen race=0
gen subset=0

keep `vistalist'
order `vistalist'
save temp_chars`1'a3, replace

*-----4. aggregate by sex, age, zipcodes, and subsets-----
use temp_chars`1'a, clear
gen one=1
sort uwid subset
by uwid subset: gen idseq=_n
recode idseq (1=1) (*=.)
sort sex agegrp zipcode subset
by sex agegrp zipcode subset: gen seq=_n
by sex agegrp zipcode subset: egen count=count(one)
by sex agegrp zipcode subset: egen undupl=count(idseq)
keep if seq==1
gen year=`1'
gen place=real(zipcode)
gen race=0

keep `vistalist'
order `vistalist'
save temp_chars`1'a4, replace

*-----append KC and ZIP code level data-----
use temp_chars`1'a1, clear
append using temp_chars`1'a2
append using temp_chars`1'a3
append using temp_chars`1'a4
saveold chars`1'z, replace

shell erase temp_*.dta











