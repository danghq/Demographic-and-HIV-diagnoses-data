/*******************************************
                      STATA
********************************************/

clear all
program drop _all
capture log close
macro drop _all
set logtype text
set type double
set more off, perm
set matsize 11000

cd "`c(pwd)'"

global input = subinstr("`c(pwd)'", "...", "Data", .)
global output = subinstr("`c(pwd)'", "...", "\output", .)
global temp = subinstr("`c(pwd)'", "...", "\temp", .)
global dta = subinstr("`c(pwd)'", "...", "\dta", .)

gen community = ""
gen searchid = ""
gen hiv = ""
gen art = ""
gen chcdate = ""
gen trdate = ""	
gen braceletid = ""

save "$dta\demographic and HIV diagnoses_master.dta", replace

local communities Bugamba Bugono Bware Kadama Kameke Kamuge Kazo ///
Kisegi Kitare Kitwe Kiyeyi Kiyunga Magunga Merikit Mitooma Muyembe ///
Nankoma Nsiika Nsiinze	Nyamrisra Nyamuyanja Nyatoto Ogongo Ongo Othoro	///
Rubaare	Rugazi Ruhoko Rwashamaire Sena Sibuoche	TomMboya

foreach community in `communities' {
	forval i = 0/3 {
		import delimited using "$input\\`community'_`i'.csv", stringcols(_all) clear
		append using "$dta\demographic and HIV diagnoses_master.dta"
		replace community = "`community'_`i'" if community == ""
		sort community
		save "$dta\demographic and HIV diagnoses_master.dta", replace
	}
}

split community, p("_")
drop community
rename community1 community
rename community2 period

order community period

ds, has(type string) 
foreach var in `r(varlist)' {
destring `var', replace
}

* Reformat date
gen chcdate2 = date(chcdate, "YMD")
format chcdate2 %tdnn/dd/YY
drop chcdate
rename chcdate2 chcdate

gen trdate2 = date(trdate, "YMD")
format trdate2 %tdnn/dd/YY
drop trdate
rename trdate2 trdate

order community period searchid chcdate trdate braceletid
sort _all

tostring braceletid, replace

compress
save "$dta\demographic and HIV diagnoses_master.dta", replace


*Import viral loads dataset
import delimited using "$input\\ViralLoads.csv", stringcols(_all) clear
destring vl, replace
rename vl viralloads

* Reformat date
gen date2 = date(date, "YMD")
format date2 %tdnn/dd/YY
drop date
rename date2 viraldate

merge m:m braceletid using "$dta\demographic and HIV diagnoses_master.dta"
sort community searchid period
drop if _merge==1
drop _merge

order brace community period search chc trdate viralloads viraldate

* fill in missing age and gender
replace age = age[_n-1] if missing(age)
replace male = male[_n-1] if missing(male)

gen dup =.
replace dup = 1 if period == period[_n-1]
replace dup = 1 if period == period[_n+1] & period != period[_n-1]

preserve
keep if dup == 1
egen groupid = group(brace community period searchid)

gen viraldate_chcdate_diff = viraldate-chcdate
by groupid, sort: egen min_diff = min(viraldate_chcdate_diff)

* drop if chcdate before viraldate
drop if viraldate_chcdate_diff<0
drop if viraldate_chcdate_diff!=min_diff & viraldate_chcdate_diff!=.

drop min_diff viraldate_
gen viraldate_trdate_diff = viraldate-trdate
drop if viraldate_trdate_diff<0
by groupid, sort: egen min_diff = min(viraldate_trdate_diff)
drop if viraldate_trdate_diff!=min_diff & viraldate_trdate_diff!=.

drop min_diff viraldate_ 
gen keep = 1
tempfile temp
save `temp', replace
restore

merge m:1 * using `temp'

drop if dup == 1 & keep!=1

sort community searchid period

drop dup groupid keep

gen dup =.
replace dup = 1 if period == period[_n-1]
replace dup = 1 if period == period[_n+1] & period != period[_n-1]
drop if dup == 1
drop dup _merge

sort community searchid period
compress
save "$dta\demographic and HIV diagnoses_master.dta", replace
