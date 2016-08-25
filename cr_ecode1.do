*Determine if record has an external cause of injury
gen EXTINJ=0
replace EXTINJ=1 if (ecodex>=800 & ecodex <=869) 
replace EXTINJ=1 if (ecodex>=880 & ecodex <=929) 
replace EXTINJ=1 if (ecodex>=950 & ecodex <=999)
replace EXTINJ=2 if (ecodex>=870 & ecodex <=879)
replace EXTINJ=2 if (ecodex>=930 & ecodex <=949)
tab1 EXTINJ, m
keep if EXTINJ>0

*Note: cause and mvcper are not mutually exclusive
gen cause=0
gen mvtc=0			
gen mvcper=0		
gen ntb=0
gen othpedal=0
gen pedalall=0
gen pedest=0
gen othpedst=0
gen ot=0
gen othtrans=0
gen natenv=0
gen bs=0
gen bitestng=0
gen overexer=0
gen firearm=0
gen poison=0
gen fall=0
gen suffoc=0
gen drowning=0
gen burn=0
gen fire=0
gen struck=0
gen mach=0
gen other=0
gen fireburn=0
gen cut=0
gen nec=0
gen notspec=0
gen medcare=0

replace mvtc=1 if (ecodex>=810 & ecodex<=819) | ecode=="9585" | ecode=="9685" | ecode=="9985"		/*MV traffic*/
replace mvcper=11 if (ecodex>=810 & ecodex<=819) & (persinj==0 | persinj==1)						/*MVT Occupant*/
replace mvcper=12 if (ecodex>=810 & ecodex<=819) & (persinj==2 | persinj==3)						/*MVT Motorcyclist*/
replace mvcper=13 if (ecodex>=810 & ecodex<=819) & persinj==6										/*MVT Pedal cyclist*/
replace mvcper=14 if (ecodex>=810 & ecodex<=819) & persinj==7										/*MVT Pedestrian*/
replace mvcper=15 if (ecodex>=810 & ecodex<=819) & persinj==9										/*MVT Unspecified*/
replace mvcper=16 if (ecodex>=810 & ecodex<=819) & (persinj==4 | persinj==5 | persinj==8)			/*MVT Other*/
replace mvcper=16 if mvtc==1 & mvcper==0

replace ntb=1 if (ecodex>=800 & ecodex<=807) & persinj==3
replace ntb=2 if (ecodex>=820 & ecodex<=825) & persinj==6
replace ntb=3 if ecode=="8261" | ecode=="8269"
replace ntb=4 if (ecodex>=827 & ecodex<=829) & persinj==1
replace othpedal=1 if ntb>=1 & ntb<=4
replace pedalall=1 if mvcper==3 | othpedal==1
replace pedest=1 if (ecodex>=800 & ecodex<=807) & persinj==2
replace pedest=2 if (ecodex>=820 & ecodex<=825) & persinj==7
replace pedest=3 if (ecodex>=826 & ecodex<=829) & persinj==0
replace othpedst=1 if pedest==1 | pedest==2 | pedest==3
replace ot=1 if (ecodex>=800 & ecodex<=807) & (persinj==0 | persinj==1 | persinj==8 | persinj==9)
replace ot=2 if ecodex==826 & (persinj>=2 & persinj<=8)
replace ot=3 if ((ecodex>=827 & ecodex<=829) & (persinj>=2 & persinj<=9)) | ecodex==831 | (ecodex>=833 & ecodex<=845)
replace ot=4 if (ecodex>=820 & ecodex<=825) & ((persinj>=0 & persinj<=5) | (persinj==8 | persinj==9))
replace ot=5 if ecode=="9586" | ecode=="9886"
replace othtrans=1 if ot>0 & ot<=5
replace natenv=1 if (ecodex>=900 & ecodex<=909) | ecode=="9280" | ecode=="9281"  | ecode=="9282" | ecode=="9583" | ecode=="9883"
replace bs=1 if (ecode>="9050" & ecode<="9056") | ecode=="9059"
replace bs=2 if (ecode>="9060" & ecode<="9065") | ecode=="9069"
replace bitestng=1 if bs<=2

replace overexer=1 if ecodex==927
replace firearm=1 if (ecode>="9220" & ecode<="9223") | ecode=="9228" | ecode=="9229" | ///
 (ecode>="9550" & ecode<="9554") | (ecode>="9650" & ecode<="9654") | ecodex==970 | (ecode>="9850" & ecode<="9854")

replace poison=1 if (ecodex>=850 & ecodex<=869) | (ecodex>=950 & ecodex<=952) | ///
 ecodex==962 | ecodex==972 | (ecodex>=980 & ecodex<=982) 

replace fall=1 if (ecodex>=880 & ecodex<=886) | ecodex==888 | ecodex==957 | ecode=="9681" | ecodex==987
replace suffoc=1 if ecodex==911 | ecodex==912 | ecodex==913 | ecodex==953 | ecodex==963 | ecodex==983

replace drowning=1 if ecodex==830 | ecodex==832 | ecodex==910 | ecodex==954 | ecodex==964 | ecodex==984
replace fire=1 if (ecodex>=890 & ecodex<=899) | ecode=="9581" | ecode=="9680" | ecode=="9881"
replace burn=1 if ecodex==924 | ecode=="9582" | ecode=="9587" | ecodex==961 | ecode=="9683" | ecode=="9882" | ecode=="9887"
replace fireburn=1 if fire==1 | burn==1
replace cut=1 if ecodex==920 | ecodex==956 | ecodex==966 | ecodex==974 | ecodex==986
replace struck=1 if ecodex==916 | ecodex==917 | ecode=="9682" | ecode=="9600" | ecodex==973 | ecodex==975
replace mach=1 if ecodex==919

replace other=1 if (ecodex>=846 & ecodex<=848) | ecodex==914 | ecodex==915 | ecodex==918 | ecodex==921 ///
 | ecode=="9224" | ecodex==923 | ecodex==925 | ecodex==926 | ecode=="9283" | (ecode>="9290" & ecode<="9295") ///
 | ecode=="9555" | ecode=="9556" | ecode=="9559" | ecode=="9580" | ecode=="9584" | ecode=="9601" ///
 | (ecode>="9655" & ecode<="9659") | ecodex==967 | (ecode=="9684" | ecode=="9686" | ecode=="9687") ///
 | ecodex==971 | ecodex==978 | (ecodex>=990 & ecodex<=994) | ecodex==996 | ecode=="9970" | ecode=="9971" | ecode=="9972" ///
 | ecode=="9855" | ecode=="9856" | ecode=="9880" | ecode=="9884"

replace nec=1 if ecode=="9288" | ecode=="9298" | ecode=="9588" | ecodex==959 | ecode=="9688" | ecodex==969 ///
 | ecodex==995 | ecode=="9978" | ecodex==977 | ecodex==998 | ecodex==999 | ecode=="9888" | ecodex==989
replace notspec=1 if ecodex==887 | ecode=="9289" | ecode=="9299" | ecode=="9589" | ecode=="9689" | ecodex==976 | ecode=="9979" | ecode=="9889"

replace cause=10 if mvtc==1
replace cause=20 if firearm==1
replace cause=21 if poison==1
replace cause=22 if fall==1
replace cause=23 if suffoc==1
replace cause=24 if drowning==1
replace cause=25 if fire==1 | burn==1
replace cause=26 if cut==1
replace cause=27 if struck==1
replace cause=28 if ecodex==919
replace cause=29 if othpedal==1
replace cause=30 if othpedst==1
replace cause=31 if othtrans==1
replace cause=32 if natenv==1
replace cause=33 if overexer==1
replace cause=34 if other==1
replace cause=35 if nec==1
replace cause=36 if notspec==1

replace medcare=1 if (ecodex>=870 & ecodex<=879) | (ecodex>=930 & ecodex<=949)
recode cause 0=.
replace cause=800+cause
recode mvcper 0=.
replace mvcper=800+mvcper

*======Manner-Intent coding=====
gen inj=0
replace inj=1 if (ecodex>=800 & ecodex<=869) | (ecodex>=880 & ecodex<=929)
replace inj=2 if (ecodex>=950 & ecodex<=959)
replace inj=3 if (ecodex>=960 & ecodex<=969)
replace inj=4 if (ecodex>=970 & ecodex<=978) | (ecodex>=990 & ecodex<=999)
replace inj=5 if (ecodex>=980 & ecodex<=989)
recode inj 0=.
replace inj=900+inj

gen inj1=.		/*Unintentional Injuries*/
replace inj1=8011 if cause==810 & inj==901
replace inj1=8021 if cause==820 & inj==901
replace inj1=8031 if cause==821 & inj==901
replace inj1=8041 if cause==822 & inj==901
replace inj1=8051 if cause==823 & inj==901
replace inj1=8061 if cause==824 & inj==901
replace inj1=8071 if cause==825 & inj==901
replace inj1=8081 if cause==826 & inj==901
replace inj1=8091 if cause==827 & inj==901
replace inj1=8101 if cause==828 & inj==901
replace inj1=8111 if cause==829 & inj==901
replace inj1=8121 if cause==830 & inj==901
replace inj1=8131 if cause==831 & inj==901
replace inj1=8141 if cause==832 & inj==901
replace inj1=8151 if cause==833 & inj==901
replace inj1=8161 if cause==834 & inj==901
replace inj1=8171 if cause==835 & inj==901
replace inj1=8181 if cause==836 & inj==901

gen inj2=.	/*Self-Inflicted*/
replace inj2=8012 if cause==810 & inj==902
replace inj2=8022 if cause==820 & inj==902
replace inj2=8032 if cause==821 & inj==902
replace inj2=8042 if cause==822 & inj==902
replace inj2=8052 if cause==823 & inj==902
replace inj2=8062 if cause==824 & inj==902
replace inj2=8072 if cause==825 & inj==902
replace inj2=8082 if cause==826 & inj==902
replace inj2=8092 if cause==827 & inj==902
replace inj2=8102 if cause==828 & inj==902
replace inj2=8112 if cause==829 & inj==902
replace inj2=8122 if cause==830 & inj==902
replace inj2=8132 if cause==831 & inj==902
replace inj2=8142 if cause==832 & inj==902
replace inj2=8152 if cause==833 & inj==902
replace inj2=8162 if cause==834 & inj==902
replace inj2=8172 if cause==835 & inj==902
replace inj2=8182 if cause==836 & inj==902

gen inj3=.	/*Assault*/
replace inj3=8013 if cause==810 & inj==903
replace inj3=8023 if cause==820 & inj==903
replace inj3=8033 if cause==821 & inj==903
replace inj3=8043 if cause==822 & inj==903
replace inj3=8053 if cause==823 & inj==903
replace inj3=8063 if cause==824 & inj==903
replace inj3=8073 if cause==825 & inj==903
replace inj3=8083 if cause==826 & inj==903
replace inj3=8093 if cause==827 & inj==903
replace inj3=8103 if cause==828 & inj==903
replace inj3=8113 if cause==829 & inj==903
replace inj3=8123 if cause==830 & inj==903
replace inj3=8133 if cause==831 & inj==903
replace inj3=8143 if cause==832 & inj==903
replace inj3=8153 if cause==833 & inj==903
replace inj3=8163 if cause==834 & inj==903
replace inj3=8171 if cause==835 & inj==903
replace inj3=8181 if cause==836 & inj==903

gen inj4=.	/*Legal Intervention*/
replace inj4=8014 if cause==810 & inj==904
replace inj4=8024 if cause==820 & inj==904
replace inj4=8034 if cause==821 & inj==904
replace inj4=8044 if cause==822 & inj==904 
replace inj4=8054 if cause==823 & inj==904
replace inj4=8064 if cause==824 & inj==904
replace inj4=8074 if cause==825 & inj==904
replace inj4=8084 if cause==826 & inj==904
replace inj4=8094 if cause==827 & inj==904
replace inj4=8104 if cause==828 & inj==904
replace inj4=8114 if cause==829 & inj==904
replace inj4=8124 if cause==830 & inj==904
replace inj4=8134 if cause==831 & inj==904
replace inj4=8144 if cause==832 & inj==904
replace inj4=8154 if cause==833 & inj==904
replace inj4=8164 if cause==834 & inj==904 
replace inj4=8174 if cause==835 & inj==904
replace inj4=8184 if cause==836 & inj==904

gen inj5=.	/*Undetermined*/
replace inj5=8015 if cause==810 & inj==905
replace inj5=8025 if cause==820 & inj==905
replace inj5=8035 if cause==821 & inj==905
replace inj5=8045 if cause==822 & inj==905
replace inj5=8055 if cause==823 & inj==905
replace inj5=8065 if cause==824 & inj==905
replace inj5=8075 if cause==825 & inj==905
replace inj5=8085 if cause==826 & inj==905
replace inj5=8095 if cause==827 & inj==905
replace inj5=8105 if cause==828 & inj==905
replace inj5=8115 if cause==829 & inj==905
replace inj5=8125 if cause==830 & inj==905
replace inj5=8135 if cause==831 & inj==905
replace inj5=8145 if cause==832 & inj==905
replace inj5=8155 if cause==833 & inj==905
replace inj5=8165 if cause==834 & inj==905 
replace inj5=8175 if cause==835 & inj==905
replace inj5=8185 if cause==836 & inj==905
