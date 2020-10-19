# Description

APDE’s birth data are provided by [WA DOH Vital Statistics](https://www.doh.wa.gov/DataandStatisticalReports/HealthStatistics/Birth). Tabulated summaries of birth data for 1990 to the present are available on DOH’s [Commmunity Health Assessment Tool](https://secureaccess.wa.gov/doh/chat/) ([CHAT](https://secureaccess.wa.gov/doh/chat/)). This document covers the preparation and use of person level microdata which can be used for in-depth King County birth related analyses for 2003 through the present.

WA DOH published a [Birth Data File Technical Notes](https://www.doh.wa.gov/Portals/1/Documents/Pubs/422-160-BirthDataFileTechnicalNotes.pdf) document that might be of some help when using this data.

# Use cases

These data can be used for production of standardized estimates for larger scale projects (e.g., [adolescent birth rate in CHI](https://www.kingcounty.gov/depts/health/data/community-health-indicators/washington-state-vital-statistics-birth.aspx?shortname=Adolescent%20birth%20rate)) as well as customized data requests (e.g., birthrates in Seattle by ethnicity and hospital).

# Data Sharing

<table>
<thead>
<tr class="header">
<th>DSAs and amendments</th>
<th>File paths &amp; notes</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>Approved use cases</td>
<td><p>For standard public health activities, following all PHI protections.</p>
<p>DSA: <a href="file:///\\Phshare01\share\EPE_SHARE\Admin%20Support\Contracts\Data%20Sharing%20Agreements\DOH-CHARS,Birth,Death,BERD,Infantdeath\CHARS%20DSA%202023-07-06_APDE_3557.pdf">\\Phshare01\share\EPE_SHARE\Admin Support\Contracts\Data Sharing Agreements\DOH-CHARS,Birth,Death,BERD,Infantdeath\CHARS DSA 2023-07-06_APDE_3557.pdf</a></p>
<p>Appendix: <a href="file:///\\Phshare01\share\EPE_SHARE\Admin%20Support\Contracts\Data%20Sharing%20Agreements\DOH-CHARS,Birth,Death,BERD,Infantdeath\CHARS%20DSA%202023-07-06_APDE_3557_Appendix%20A_Blank.pdf">\\Phshare01\share\EPE_SHARE\Admin Support\Contracts\Data Sharing Agreements\DOH-CHARS,Birth,Death,BERD,Infantdeath\CHARS DSA 2023-07-06_APDE_3557_Appendix A_Blank.pdf</a></p></td>
</tr>
<tr class="even">
<td>IRB-approved research</td>
<td>Approval needed for verifiable research (Human Research Review Policy 03.001)</td>
</tr>
<tr class="odd">
<td>Small number suppression</td>
<td><p>Follow whichever guideline is most strict at the time of your analysis</p>
<p><a href="https://www.doh.wa.gov/Portals/1/Documents/1500/SmallNumbers.pdf">WA DOH small number suppression guidelines</a></p>

<p><a href="https://kc1.sharepoint.com/teams/PHc/datareq/_layouts/15/Doc.aspx?sourcedoc=%7B87129496-FD07-4DED-8B2E-8351FF20ECDA%7D&amp;file=APDE_SmallNumberUpdate.xlsx&amp;action=default&amp;mobileredirect=true&amp;cid=1af3200d-e226-44c3-9905-cb62c2b26285&amp;CT=1602868544706&amp;OR=Outlook-Body&amp;CID=187CB2DA-3CC8-44E7-A241-99EF084F4985&amp;wdLOR=c80047442-F8B0-49EC-A597-0B69910E2C40">APDE SharePoint suppression guidelines</a></p></td>
</tr>
<tr class="even">
<td>Other privacy/confidentiality requirements</td>
<td><p>From our suppression guidelines …</p>
<p>Washington State birth certificates contain two sections – the legal section is considered public and the medical section is confidential.</p>
<p>Both death and birth certificate data (statistical and personal identifiers) cannot be released without a Declaration of Commercial Purpose form (i.e. cannot be used for commercial purposes).</p></td>
</tr>
<tr class="odd">
<td>How to request access</td>
<td>Request from Mike Smyser or Eva Wong?</td>
</tr>
<tr class="even">
<td>Active directory mapping</td>
<td>??</td>
</tr>
</tbody>
</table>

# Data Processing / ETL

## Data inputs

| **Year(s)** | **Source**                                        | **Contact** | **Notes**                                                                                                                           |
| ----------- | ------------------------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| 2003-2016   | Bedrock system                                    | DOH         |                                                                                                                                     |
| 2017+       | Washington Health and Life Events System (WHALES) | DOH         | Change from Bedrock to WHALES means that some variables may have changed or APDE had to harmonize variables across the two systems. |

## Data outputs

<table>
<thead>
<tr class="header">
<th><strong>Data</strong></th>
<th><strong>Location</strong></th>
<th><strong>Last Update</strong></th>
<th><strong>APDE Staff Contact</strong></th>
<th><strong>Notes</strong></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>2003-2018 line level data</td>
<td>[KCITSQLPRPDBM50].[PH_APDEStore].[final].[bir_wa]</td>
<td>2020-03-06</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="even">
<td>QA results</td>
<td>[KCITSQLPRPDBM50].[PH_APDEStore].[metadata].[qa_bir]</td>
<td>2020-03-07</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="odd">
<td>Row counts for each QA</td>
<td>[KCITSQLPRPDBM50].[PH_APDEStore].[metadata].[qa_bir_values]</td>
<td>2020-03-07</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="even">
<td>Record raw data loading</td>
<td>[KCITSQLPRPDBM50]. [PH_APDEStore].[metadata].[etl_log]</td>
<td>?</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="odd">
<td>RAW 2017+ data</td>
<td>[KCITSQLPRPDBM50]. [PH_APDEStore].[load_raw].[bir_wa_2017_20xx]</td>
<td>?</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="even">
<td>RAW 2003-2016 data</td>
<td>[KCITSQLPRPDBM50]. [PH_APDEStore].[load_raw].[bir_wa_2003_2016]</td>
<td>?</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="odd">
<td>Geocoded 2017+</td>
<td>[KCITSQLPRPDBM50]. [PH_APDEStore].[load_raw].[bir_wa_geo_2017_20xx]</td>
<td>?</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="even">
<td>Geocoded 2003-2016</td>
<td>[KCITSQLPRPDBM50]. [PH_APDEStore].[load_raw].[bir_wa_geo_2003_2016]</td>
<td>?</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="odd">
<td>Xwalk apde, WHALES, Bedrock</td>
<td><p>[PH_APDEStore].[ref].[bir_field_name_map]</p>
<p>Same as</p>
<p>https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/ref/ref.bir_field_name_map.csv</p></td>
<td>?</td>
<td>Danny Colombara</td>
<td></td>
</tr>
<tr class="even">
<td>RAW data from DOH</td>
<td>\\phdata01\drof_data\doh data\Births\DATA\raw</td>
<td>?</td>
<td>Danny Colombara</td>
<td></td>
</tr>
</tbody>
</table>

## Protocol

  - [Clone this repository](https://github.com/PHSKC-APDE/DOHdata) from Git.

  - Review each piece of code before you run it\! This includes scripts that are called on by other scripts.

  - Your life will be much easier if you begin by running [master\_bir\_full.R](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/master/master_bir_full.R). This file will let run:
    
      - LOAD\_RAW processes for 2003-2016, both birth and geocoded data.
    
      - LOAD\_RAW processes for 2017+, both birth and geocoded data. This should automatically append the latest year’s data as long as it is saved in \\\\phdata01\\drof\_data\\doh data\\Births\\DATA\\raw and follows the same naming convention.
    
      - STAGE processes for birth and geocoded data
    
      - **Do not run the QA script** from here (unless you are 100% sure of what you are doing). It is **STRONGLY** recommended that your **run the QA script manually** so the quality can be assessed and problems fixed before loading to SQL.

  - Run [qa\_stage.bir\_wa.R](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/stage/qa_stage.bir_wa.R) section by section. **NOTE**, you need to manually download comparison data from CHAT to make sure that the new year of data is the same as that in CHAT. Follow the prior year’s examples in \\\\phdata01\\DROF\_DATA\\DOH DATA\\Births\\DATA\\BIRTH\\DOH\_Birth\_Tables\_Summary\_for\_Comparison and update file paths in this script as needed.

  - Once the data pass QA, transfer the SQL schema from stage to final by running [load\_final.birth\_wa.sql](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/final/load_final.birth_wa.sql)

  - Create a new data dictionary by running [create\_birth\_dictionary.R](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/final/create_birth_dictionary.R). Review the output and be sure to fill in any missing descriptions or important notes. Save it as DOHdata/ETL/birth/ref/ref\_bir\_user\_dictionary\_final.csv in your local repository.

  - Update the markdown for this [README](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/README.md), at the minimum update the dates and the APDE staff person.

  - Add, commit, and push your changes to Git (at the minimum, the dictionary should be updated).

# Data availability by year

  - This prepares structured person level data from 2003 onwards.

  - The data dictionary specifies which years are relevant for each variable.

  - If you need estimates prior to 2003, CHAT has data beginning in 1990. Our SQL server also has data going back to 1990 (\[PH\_APDEStore\].\[dbo\].\[wabir1990\_2002\]), but I (Danny) cannot vouch for it’s source, quality, etc.

# Data dictionary

If the protocol above is followed, the data dictionary should be updated every year and should be saved [here](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/ref/ref_bir_user_dictionary_final.csv). While you can [view the dictionary on GitHub](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/ref/ref_bir_user_dictionary_final.csv), downloading it will reveal additional columns that might be of use to you.

DOH issues dictionaries will be of some use to map encodings to human readable terms:

  - \\\\phdata01\\DROF\_DATA\\DOH DATA\\Births\\DATA\\BIRTH\\422-161-BirthStatisticalDictionaryCrosswalks\_Mod2018-1004.xlsx

  - \\\\phdata01\\DROF\_DATA\\DOH DATA\\Births\\SQLTEST\\BIRTH\_datadictionary.DOC

  - \\\\phdata01\\DROF\_DATA\\DOH DATA\\Births\\DATA\\BIRTH\\Birth Data Dictionary 1980\_2016\_mod2018-1011.docx

# Available analytic categories

Standard analytic variables are prefixed by ‘chi\_’:

| **varname**                | **description**                              | **value**                                                                                                                                                                               |
| -------------------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| chi\_edu\_mom\_8           | Mother's education                           | \<= 8th grade, 9th-12th grade, no diploma, Associate degree, Bachelor's degree, Doctorate or Professional degree, High school graduate or GED, Master's degree, Some college, no degree |
| chi\_geo\_big\_cities      | CHI geography: Big Cities                    | Auburn city, Bellevue city, Federal Way city, Kent city, Kirkland city, Renton city, Seattle city                                                                                       |
| chi\_geo\_coo\_places      | CHI geography: COO Places                    | Balance of King County, Rainier Valley, SeaTac/Tukwila, White Center                                                                                                                    |
| chi\_geo\_hra\_long        | CHI geograpny: HRA long names                | E.g., Auburn-North, Auburn-South, Ballard, Beacon Hill/Georgetown/South Park, Bear Creek/Carnation/Duvall                                                                               |
| chi\_geo\_hra\_short       | CHI geograpny: HRA short names               | E.g., Auburn-North, Auburn-South, Ballard, Beacon/Gtown/S.Park, Bear Creek/Carnation/Duvall                                                                                             |
| chi\_geo\_kc               | King County resident (mother)                | 0, 1                                                                                                                                                                                    |
| chi\_geo\_regions\_4       | CHI geography: Regions                       | East, North, Seattle, South                                                                                                                                                             |
| chi\_geo\_seattle          | CHI geography: Seattle flag                  | Not Seattle, Seattle                                                                                                                                                                    |
| chi\_geo\_wastate          | WA State resident (mother)                   | 0, 1                                                                                                                                                                                    |
| chi\_geo\_zip5             | Zip code (5 digit) of residence              | E.g., 90759, 97002, 97188, 98001, 98002                                                                                                                                                 |
| chi\_nativity              | Nativity                                     | Born in US, Foreign born                                                                                                                                                                |
| chi\_race\_6               | Race - 6 categories                          | AIAN, Asian, Black, Multiple, NHPI, White                                                                                                                                               |
| chi\_race\_7               | Race - 7 categories                          | AIAN, Asian, Black, Multiple, NHPI, Oth/unk, White                                                                                                                                      |
| chi\_race\_aic\_aian       | Race - AIAN alone or in combination          | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_asian      | Race - Asian alone or in combination         | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_asianother | Race - Asian - other alone or in combination | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_black      | Race - Black alone or in combination         | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_chinese    | Race - Chinese alone or in combination       | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_filipino   | Race - Filipino alone or in combination      | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_his        | Race - Hispanic alone or in combination      | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_indian     | Race - Asian Indian alone or in combination  | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_japanese   | Race - Japanese alone or in combination      | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_korean     | Race - Korean alone or in combination        | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_nhpi       | Race - NHPI alone or in combination          | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_vietnamese | Race - Vietnamese alone or in combination    | 0, 1                                                                                                                                                                                    |
| chi\_race\_aic\_wht        | Race - White alone or in combination         | 0, 1                                                                                                                                                                                    |
| chi\_race\_eth7            | Race/ethnicity - 7 categories                | AIAN, Asian, Black, Hispanic, Multiple, NHPI, White                                                                                                                                     |
| chi\_race\_eth8            | Race/ethnicity - 8 categories                | AIAN, Asian, Black, Hispanic, Multiple, NHPI, Oth/unk, White                                                                                                                            |
| chi\_race\_hisp            | Ethnicity                                    | 0, 1                                                                                                                                                                                    |
| chi\_sex                   | Sex of baby                                  | Female, Male                                                                                                                                                                            |

# Case definitions & concepts

There are too many to specify here. Generally, the data dictionary should be able to provide much of this. Many recodings are found in [ref.bin\_recodes\_simple.csv](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/ref/ref.bir_recodes_simple.csv). More complex case definitions will be found by searching in [load\_stage.bir\_wa.R](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/stage/load_stage.bir_wa.R).

The Kotelchuck index needs further explanation. In short, our Kotelchuck index differs slightly from that calculated by DOH. In 2019, APDE spent days trying to reconcile the results, including obtaining SQL code from DOH and SAS code from the originator of the Kotelchuck index. At the end of this effort it was decided that it was equally likely that the mistake was on the DOH end, so we retained the Kotelchuck index that is coded and described in [load\_stage.bir\_wa.R](https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/birth/stage/load_stage.bir_wa.R).

# Special considerations / notes

The birth data is systematically processed to create output for CHI Tableau visualizations. That process is entirely separate from this ETL process and is [documented in the CHI repository](https://github.com/PHSKC-APDE/chi/blob/master/birth/README.md).
