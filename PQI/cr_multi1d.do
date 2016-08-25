*Coding for PQI indicators and composite
set more off
use V:\DATA\HOSP\Stata\data\spub`1', clear
*use V:\DATA\HOSP\Stata\data\spub2010, clear
keep if COUNTYRES=="17"
keep SEQ_NO DIAG1-DIAG9 ECODECAL STATUS AGE SEX ADM_TYPE ADM_SRC PROC1-PROC6 DRG MDC DIS_DATE
rename SEQ_NO seq_no
gen adm_type=real(ADM_TYPE)
gen drgx=real(DRG)
gen mdc=real(MDC)
rename AGE age
drop ADM_TYPE DRG MDC

gen year=real(substr(DIS_DATE,4,4))
gen dqtr=real(substr(DIS_DATE,1,2))

gen PROC7=""
gen PROC8=""
gen PROC9=""

drop if age<18 & mdc~=14

forvalue i=1/9 {
	rename DIAG`i' dx`i'
	gen dx`i'x=real(substr(dx`i',1,3))
	gen dx`i'y=real(dx`i')
	rename PROC`i' proc`i'
	gen proc`i'x=real(substr(proc`i',1,2))
	gen proc`i'y=real(proc`i')
}

gen ecode=substr(ECODECAL,2,4)
gen ecodex=real(substr(ecode,1,3))

/* Gastroenteritis */
recode dx1y (00861/00867 00869 0088 0090/0093 5589=1) (*=0), gen(acpgasd)


*--------------------------------------------------------------
*#1. Diabetes with Short Term Complications - Principle dx code
recode dx1y (25010/25013 25020/25023 25030/25033=1) (*=0), gen(acdiasd)


*#2. Perforated Appendix (Outcome of Interest) */
gen acsappd=0
forvalue i=1/9 {
replace acsappd=1 if inlist(dx`i'y,5400,5401)
}

 /* Appendicitis (Population at Risk) */
gen acsap2d=0
forvalue i=1/9 {
replace acsap2d=1 if inlist(dx`i'y,5400,5401,5409,541)
}

*#3. Diabetes with Long Term Complications
recode dx1y (25040/25043 25050/25053 25060/25063 25070/25073 25080/25083 25090/25093=1) (*=0), gen(acdiald)

*#5. COPD (#1)
recode dx1y (4910 4911 49120 49121 4918/4920 4928 494 4940 4941 496=1) (*=0), gen(accopdd)

*#5. Asthma
recode dx1y (49300/49302 49310/49312 49320/49322 49381 49382 49390/49392=1) (*=0), gen(acsastd)

/* EXCLUDE: CYSTIC FIBROSIS AND ANOMALIES OF RESPIRATORY SYSTEM */
recode dx1y (27700/27703 27709 51661/51664 51669 74721 7483/7485 74860 74861 47869 7488 7489 7503 7593 7707=1) (*=0), gen(respan)

gen accopdd2=0
forvalue i=2/9 {
replace accopdd2=1 if inlist(dx`i'y,4910,4911,49120,49121,4918,4919,4920,4928,494,4940,4941,496)
}

/* COPD (#2) */
gen accpd2d=0
replace accpd2d=1 if inlist(dx1y,4660,490)

/*#7. Hypertension */
gen acshypd=0
replace acshypd=1 if inlist(dx1y,4010,4019,40200,40210,40290,40300,40310,40390,40400,40410,40490)

/* Exclude: Stage I-IV Kidney Disease */
gen acshy2d=0
forvalue i=1/9 {
replace acshy2d=1 if inlist(dx`i'y,40300,40310,40390,40400,40410,40490)
}

/* EXCLUDE: CHRONIC RENAL FAILURE */
gen crenlfd=0
forvalue i=1/9 {
	replace crenlfd=1 if inlist(dx`i'y,40300,40301,40310,40311,40390,40391,40400,40401,40402,40403, ///
	40410,40411,40412,40413,40490,40491,40492,40493,585,5855,5856)
}
 
/* Renal Failure */
recode dx1y (5845/5849 586 9975=1) (*=0), gen(physidb)

/* ICD-9-CM Procedure codes: */
gen acshypp=0
replace acshypp=1 if inlist(proc1y,3895,3927,3929,3942,3943,3993,3994)

/* Exclude: Cardiac Procedures */
gen acscarp=0
forvalue i=1/6 {
	recode proc`i'y (0050/0054 0056 0057 0066 1751 1752 1755 3500 3501/3514 3520/3528 3531/3535 3539 3541 3542  ///
	3550/3555 3560/3563 3570/3573 3581/3584 3591/3599 3601/3607 3609/3617 3619 362 363 3631/3634 3639 3691 3699  ///
	3731/3737 3741 375 3751/3755 3760/3765 3677 3770/3783 3785/3787 3789 3794/3798 3826=1) (*=0), gen(acscarp`i')
	replace acscarp=1 if acscarp`i'==1
}

*#8. CHF */
recode dx1y (39891 40201 40211 40291 40401 40403 40411 40413 40491 40493 4280 4281 42820/42823 42830/42833 42840/42843 4289=1) (*=0), gen(acschfd)
recode dx1y (39891 4280 4281 42820/42823 42830/42833 42840/42843 4289=1) (*=0), gen(acsch2d)

*#9. Liveborn (Populaton at Risk)           */
gen livebnd=0
replace livebnd=1 if inlist(dx1,"V3000","V3001","V3100","V3101","V3200")
replace livebnd=1 if inlist(dx1,"V3201","V3300","V3301","V3400","V3401","V3500")
replace livebnd=1 if inlist(dx1,"V3501","V3600","V3601","V3700","V3701","V3900","V3901")

gen v29d=0
replace v29d=1 if inlist(dx1,"V290","V291","V293","V298","V299")

gen liveb2d=0
replace liveb2d=1 if inlist(dx1,"V301","V302","V311","V312","V321","V322")
replace liveb2d=1 if inlist(dx1,"V331","V332","V341","V342","V351","V352")
replace liveb2d=1 if inlist(dx1,"V361","V362","V371","V372","V391","V392")

*#10. Dehydration */
recode dx1y (2765 27650/27652=1) (*=0), gen(acsdehd)
gen acsdehd2=0
forvalue i=2/9 {
replace acsdehd2=1 if inlist(dx`i'y,2765,27650,27651,27652)
}

*#11. Bacterial Pneumonia */
recode dx1y (481 4822 48230/48232 48239 48241 48242 4829 4830 4831 4838 485 486=1) (*=0), gen(acsbacd)

/* Exclude: Sickle Cell      */
gen acsba2d=0
forvalue i=1/9 {
replace acsba2d=1 if inlist(dx`i'y,28241,28242,28260,28261,28262,28263,28264,28268,28269)
}

/* HYPEROSMOLALITY AND /OR HYPERNATREMIA   */
gen hyperid=0
replace hyperid=1 if dx1y==2760

/*#12. Urinary Infection */
recode dx1y (59010 59011 5902 5903 59080 59081 5909 5950 5959 5990=1) (*=0), gen(acsutid)

/* EXCLUDE: IMMUNOCOMPROMISED */
gen immunid=0
forvalue i=1/9 {
	replace immunid=1 if inlist(dx`i'y,042,1363,1992,23873,23876,23877,23879,260,261,262,27900,27901,27902,27903,27904,27905, ///
	27906,27909,27910,27911,27912,27913,27919,2792,2793,2794,27941,27949,27950,27951,27952,27953,2798,2799,28409,2841,28411, ///
	28412,28419,2880,28800,28801,28802,28803,28809,2881,2882,2884,28850,28851,28859,28953,28983,40301,40311,40391,40402,40403, ///
	40412,40413,40492,40493,5793,585,5855,5856,9968,99680,99681,99682,99683,99684,99685,99686,99687,99688,99689)
replace immunid=1 if dx`i'=="V420"  /* KIDNEY REPLACED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V421"  /* HEART REPLACED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V426"  /* LUNG REPLACED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V427"  /* LIVER REPLACED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V428"  /* OTHER SPECIFIED ORGAN OR TISSUE */
replace immunid=1 if dx`i'=="V4281" /* BONE MARROW SPECIFIED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V4282" /* PERIPHERAL STEM CELLS REPLACED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V4283" /* PANCREAS REPLACED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V4284" /* INTESTINES REPLACE BY TRANSPLANT */
replace immunid=1 if dx`i'=="V4289" /* OTHER REPLACED BY TRANSPLANT */
replace immunid=1 if dx`i'=="V451 " /* RENAL DIALYSIS STATUS */
replace immunid=1 if dx`i'=="V4511" /* RENAL DIALYSIS STATUS */
replace immunid=1 if dx`i'=="V560 " /* RENAL DIALYSIS ENCOUNTER */
replace immunid=1 if dx`i'=="V561 " /* FT/ADJ XTRCORP DIAL CATH */
replace immunid=1 if dx`i'=="V562 " /* FIT/ADJ PERIT DIAL CATH */
}

 /* EXCLUDE: IMMUNOCOMPROMISED */
 gen immunip=0
 forvalue i=1/6 {
 replace immunip=1 if inlist(proc`i'y,0018,335,3350,3351,3352,336,375,3751,410,4100,4101,4102,4103,4104,4105,4106,4107,4108,4109, ///
  5051,5059,5280,5281,5282,5283,5285,5286,5569)
}

 /* EXCLUDE: IMMUNOCOMPROMISED */
 /* DIAGNOSTIC RELATED GROUPS (DRGS): */
recode drg (488 489 490=1) (*=0), gen(immundr)
 
 /* EXCLUDE: KIDNEY OR URINARY TRACT DISORDER */
gen kidney=0
forvalue i=1/9 {
replace kidney=1 if inlist(dx`i'y,59000,59001,59370,59371,59372,59373,7530,75310,75311,75312,75313,75314,75135,75136,75317, ///
 75319,75320,75321,75322,75323,75329,7533,7534,7535,7536,7538,7539)
}

 /*#13. Angina*/
recode dx1y (4111 41181 41189 4130 4131 4139=1) (*=0), gen(acsangd)

 /*#14. Diabetes uncontrolled     */
recode dx1y (25002 25003=1) (*=0), gen(acdiaud)

 /* #16. Lower extremity amputation */
gen acsleap=0
forvalue i=1/9 {
replace acsleap=1 if inlist(proc`i'y,8410,8411,8412,8413,8414,8415,8416,8417,8418,8419)
}

 /* Include only: Diabetes */
gen acslead=0
forvalue i=1/9 {
replace acslead=1 if inlist(dx`i'y,25000,25001,25002,25003,25010,25011,25012,25013,25020,25021,25022,25023,25030, ///
 25031,25032,25033,25040,25041,25042,25043,25050,25051,25052,25053,25060,25061,25062,25063,25070,25071,25072,25073, ///
 25080,25081,25082,25083,25090,25091,25092,25093)
}

  /* Exclude: Trauma */
gen aclea2d=0
forvalue i=1/9 {
replace aclea2d=1 if inlist(dx`i'y,8950,8951,8960,8961,8962,8963,8970,8971,8972,8973,8974,8975,8976,8977)
}

 /* EXCLUDE: TOE AMPUTATION PROCEDURE   */
gen toeamip=0
forvalue i=1/6 {
replace toeamip=1 if proc`i'y==8411 
}

*----------------------------------

gen icdver=.
replace icdver=17 if year==2000 & dqtr<4
replace icdver=18 if year==2000 & dqtr==4
replace icdver=18 if year==2001 & dqtr<4
replace icdver=19 if year==2001 & dqtr==4
replace icdver=19 if year==2002 & dqtr<4
replace icdver=20 if year==2002 & dqtr==4
replace icdver=20 if year==2003 & dqtr<4
replace icdver=21 if year==2003 & dqtr==4
replace icdver=21 if year==2004 & dqtr<4
replace icdver=22 if year==2004 & dqtr==4
replace icdver=22 if year==2005 & dqtr<4
replace icdver=23 if year==2005 & dqtr==4
replace icdver=23 if year==2006 & dqtr<4

replace icdver=24 if year==2006 & dqtr==4
replace icdver=24 if year==2007 & dqtr<4
replace icdver=25 if year==2007 & dqtr==4
replace icdver=25 if year==2008 & dqtr<4

replace icdver=26 if year==2008 & dqtr==4
replace icdver=26 if year==2009 & dqtr<4
replace icdver=27 if year==2009 & dqtr==4
replace icdver=27 if year==2010 & dqtr<4

replace icdver=28 if year==2010 & dqtr==4
replace icdver=28 if year==2011 & dqtr<4
replace icdver=29 if year==2011 & dqtr==4
replace icdver=29 if year==2012 & dqtr<4
replace icdver=29 if year==2012 & dqtr==4
replace icdver=29 if year>2012

*-----------------------------------------------------
*-----Coding the PQI indicators-----
*-----------------------------------------------------
gen pqi1=acdiasd
gen pqi2=.
replace pqi2=0 if acsap2d==1
replace pqi2=1 if acsappd==1
replace pqi2=. if mdc==14

gen pqi3=acdiald
gen pqi5=0
replace pqi5=1 if accopdd==1 | (accpd2d==1 & accopdd2==1) | acsastd==1
replace pqi5=. if age<40
gen pqi7=acshypd
replace pqi7=. if acshy2d==1 & acshypp==1
replace pqi7=. if acscarp==1
gen pqi8=0
replace pqi8=1 if (icdver<=19 & acschfd==1)

replace pqi8=1 if (icdver>=20 & acsch2d==1)
replace pqi8=. if acscarp==1
gen pqi10=0
replace pqi10=1 if acsdehd==1 | (acsdehd2==1 & (hyperid==1 | acpgasd==1 | physidb==1))
replace pqi10=. if crenlfd==1

gen pqi11=acsbacd
replace pqi11=. if acsba2d==1
replace pqi11=. if immunid==1 | immunip==1

gen pqi12=acsutid
replace pqi12=. if immunid==1 | immunip==1 | kidney==1

gen pqi13=acsangd
replace pqi13=. if acscarp==1
gen pqi14=acdiaud
gen pqi15=acsastd
replace pqi15=. if respan==1
replace pqi15=. if age>=40

gen pqi16=0
replace pqi16=1 if acsleap==1 & acslead==1
replace pqi16=. if mdc==14
replace pqi16=. if aclea2d==1
replace pqi16=. if toeamip==1

label var pqi1 "DIABETES SHORT TRM COMPLICATN"
label var pqi2 "PERFORATED APPENDIX"
label var pqi3 "DIABETES LONG TERM COMPLICATN"
label var pqi5 "COPD OR ASTHMA IN OLDER ADULTS"
label var pqi7 "HYPERTENSION"
label var pqi8 "CONGESTIVE HEART FAILURE"
label var pqi10 "DEHYDRATION"
label var pqi11 "BACTERIAL PNEUMONIA"
label var pqi12 "URINARY INFECTION"
label var pqi13 "ANGINA"
label var pqi14 "DIABETES-UNCONTROLLED"
label var pqi15 "ADULT ASTHMA"
label var pqi16 "LOWER EXTREMITY AMPUTATION"

egen pqi90=rsum(pqi1 pqi3 pqi5 pqi7 pqi8 pqi10 pqi11 pqi12 pqi13 pqi14 pqi15 pqi16)
egen pqi91=rsum(pqi10 pqi11 pqi12)
egen pqi92=rsum(pqi1 pqi3 pqi5 pqi7 pqi8 pqi13 pqi14 pqi15 pqi16)


*-----Exclude transfers-----
local pqlist pqi1 pqi2 pqi3 pqi5 pqi7 pqi8 pqi10 pqi11 pqi12 pqi13 ///
 pqi14 pqi15 pqi16 pqi90 pqi91 pqi92

 foreach var of varlist `pqlist' {
	replace `var'=. if ADM_SRC=="4" | ADM_SRC=="5" | ADM_SRC=="6"
	replace `var'=. if age<18
}

recode pqi90 pqi91 pqi92 (2=1)

compress
save temp_pqi`1', replace

*--------------------------------------
use temp_pqi`1'

rename pqi1 pqiseq1
rename pqi3 pqiseq2
rename pqi5 pqiseq3
rename pqi7 pqiseq4
rename pqi8 pqiseq5
rename pqi10 pqiseq6
rename pqi11 pqiseq7
rename pqi12 pqiseq8
rename pqi13 pqiseq9
rename pqi14 pqiseq10
rename pqi15 pqiseq11
rename pqi16 pqiseq12

forvalue i=1/12 {
	preserve
	keep if pqiseq`i'==1
	keep seq_no
	gen subset=30000+`i'
	sort seq_no
	save temp_pqiseq`i'_`1', replace
	restore
}

use temp_pqiseq1_`1', clear
forvalue i=2/12 {
	append using temp_pqiseq`i'_`1'
}

sort seq_no
save temp_pqi_indiv_`1', replace

sort seq_no
by seq_no: gen dupid=_n
keep if dupid==1
drop dupid 

replace subset=30090
sort seq_no
save temp_pqi90_`1', replace

use temp_pqi_indiv_`1', clear
keep if inlist(subset,30006,30007,30008)

sort seq_no
by seq_no: gen dupid=_n
keep if dupid==1
drop dupid 
replace subset=30091
sort seq_no
save temp_pqi91_`1', replace

use temp_pqi_indiv_`1', clear
keep if inlist(subset,30001,30002,30003,30004,30005,30009,30010,30011,30012)

sort seq_no
by seq_no: gen dupid=_n
keep if dupid==1
drop dupid 

replace subset=30092
sort seq_no
save temp_pqi92_`1', replace
