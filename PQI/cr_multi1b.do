*Coding for special categories

use V:\DATA\HOSP\Stata\data\spub`1', clear
keep if COUNTYRES=="17"
keep SEQ_NO DIAG1-DIAG9 STATUS ADM_TYPE ADM_SRC PROC1-PROC6 DRG
rename SEQ_NO seq_no
gen adm_type=real(ADM_TYPE)
gen drgx=real(DRG)
gen year=`1'

gen PROC7=""
gen PROC8=""
gen PROC9=""

forvalue i=1/9 {
	rename DIAG`i' dx`i'
	gen dx`i'x=real(substr(dx`i',1,3))
	gen dx`i'y=real(dx`i')
	rename PROC`i' proc`i'
	gen proc`i'x=real(substr(proc`i',1,2))
	gen proc`i'y=real(proc`i')
}

gen dmx=0
replace dmx=1 if dx1x==250

gen dmx_amp=0
forvalue i=2/9 {
	replace dmx_amp=1 if dx`i'x==250 & proc`i'y>=8410 & proc`i'y<=8419
}


gen childbirth=0
replace childbirth=1 if inlist(drgx,370,371,372,373,374,375) & year<2008
replace childbirth=1 if inlist(drgx,765,766,767,768,774,775) & year>=2008
tab1 childbirth

save temp_spall_`1', replace

*-------diabetes---------
use temp_spall_`1', clear
keep if dmx==1
keep seq_no
gen subset=9250
sort seq_no
save temp_sp9250_`1', replace

*-------childbirth---------
use temp_spall_`1', clear
keep if childbirth==1
keep seq_no
gen subset=9371
sort seq_no
save temp_sp9371_`1', replace

*-------Not Childbirth---------
use temp_spall_`1', clear
keep if childbirth==0
keep seq_no
gen subset=9372
sort seq_no
save temp_sp9372_`1', replace
