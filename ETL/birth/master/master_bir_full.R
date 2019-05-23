#### MASTER CODE TO RUN A FULL BIRTH DATA REFRESH
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### SET UP GLOBAL PARAMETERS AND CALL IN LIBRARIES ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code

bir_path <- "//phdata01/DROF_DATA/DOH DATA/Births"
db_apde <- dbConnect(odbc(), "PH_APDEStore50")


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/scripts_general/etl_log.R")


#### LOAD_RAW ####
#### Create tables ####
create_table_f(conn = db_apde,
               config_url = "",
               overall = T, ind_yr = T, overwrite = T)


#### Set up loading variables from files ####
# Pull in config file that defines variable types
table_config <- yaml::yaml.load(getURL("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2003_2016.yaml"))

# Recode list of variables to match format required for importing
col_type_list <- lapply(table_config$vars, function(x) {
  x <- str_replace_all(x, "INTEGER", "i")
  x <- str_replace_all(x, "VARCHAR\\(\\d\\)", "c")
  x <- str_replace_all(x, "NUMERIC", "d")
})

# Make specific change to BMI
names(col_type_list)[str_detect(names(col_type_list), "bmi")] <- "BMI"

# Set up fixed width files
# Note use of two filler fields that need to be removed
col_fixed_width <- c(certno_e = 10, certype = 1, dob_yr = 4, dob_mo = 2, 
                     FILLER = 2, 
                     birth_hr = 2, sex = 1, plural = 1, city_occ = 4, 
                     facility = 3, age_mom = 2, birplmom = 2, city_res = 4, 
                     cnty_res = 2, age_dad = 2, birpldad = 2, attclass = 2, 
                     race_dad = 1, race_mom = 1, raceccal = 1, educ_dad = 2, 
                     educ_mom = 2, lb_nl = 3, priorprg = 3, 
                     FILLER2 = 18, 
                     pnatalmo = 3, pnatalvs = 3, married = 1, wt_grams = 4, 
                     fac_type = 1, occm_dad = 3, occm_mom = 3, lb_nd = 3, fd_lt20 = 3, 
                     fd_ge20 = 3, apgar1 = 3, apgar5 = 3, order = 1, crt_clas = 2, 
                     ind_num = 3, trnsferm = 1, trnsferb = 1, malf_sam = 3, 
                     hisp_dad = 1, hisp_mom = 1, hispccal = 1, res_yprt = 2, 
                     res_mprt = 2, gest_est = 3, zipcode = 5, mrf1 = 2, mrf2 = 2, 
                     mrf3 = 2, mrf4 = 2, mrf5 = 2, mrf6 = 2, diabetes = 1, 
                     herpes = 1, smokenum = 3, drinknum = 3, wghtgain = 3, 
                     obproc1 = 1, obproc2 = 1, obproc3 = 1, obproc4 = 1, amnio1 = 1, 
                     amnio2 = 1, amnio3 = 1, dmeth1 = 1, dmeth2 = 1, dmeth3 = 1, 
                     dmeth4 = 1, complab1 = 2, complab2 = 2, complab3 = 2, 
                     complab4 = 2, complab5 = 2, complab6 = 2, complab7 = 2, 
                     abcond1 = 2, abcond2 = 2, abcond3 = 2, abcond4 = 2, abcond5 = 2, 
                     abcond6 = 2, malf1 = 2, malf2 = 2, malf3 = 2, malf4 = 2, 
                     malf5 = 2, malf6 = 2, malf7 = 2, st_occ = 2, st_res = 2, 
                     racecdes = 1, hispcdes = 1, carepay = 1, wic = 1, firsteps = 1, 
                     afdc = 1, localhd = 1, wghtpre = 3, ubleed1 = 1, ubleed2 = 1, 
                     ubleed3 = 1, smoking = 1, drinking = 1, 
                     FILLER3 = 8, 
                     lb_f_nl = 3, lb_f84 = 3, lb_p_nl = 3, lb_p84 = 3, 
                     lb_p_nd = 3, lb_f_nd = 3, fd_2036 = 3, fd_ge37 = 3, 
                     malficd1 = 2, malficd2 = 2, malficd3 = 2, malficd4 = 2, 
                     malficd5 = 2, malficd6 = 2, malficd7 = 2, malficd8 = 2, 
                     gestcalc = 2, gestflag = 1, 
                     mrfnon1 = 2, mrfnon2 = 2, mrfnon3 = 2, mrfnon4 = 2, 
                     mrfnon5 = 2, mrfnon6 = 2, mrfnon7 = 2, mrfnon8 = 2, mrfnon9 = 2, 
                     nchsnew = 1, dthflg = 1, 
                     ind_mo = 2, ind_yr = 4, llb_mo = 2, llb_yr = 4, 
                     lfd_mo = 2, lfd_yr = 4, mens_mo = 2, mens_da = 2, mens_yr = 4, 
                     loth_mo = 2, loth_yr = 4, lpp_mo = 2, lpp_yr = 4, 
                     bctrymom = 3, bctrydad = 3, geozip = 5, 
                     alive = 1, fac_int = 1, trib_res = 3, momh_no = 1, momh_mex = 1, 
                     momh_pr = 1, momh_cub = 1, momh_oth = 1, momr_wht = 1, 
                     momr_blk = 1, momr_ami = 1, momr_asi = 1, momr_chi = 1, 
                     momr_fil = 1, momr_jap = 1, momr_kor = 1, momr_vie = 1, 
                     momr_oas = 1, momr_haw = 1, momr_gua = 1, momr_sam = 1, 
                     momr_opi = 1, momr_oth = 1, dadh_no = 1, dadh_mex = 1, 
                     dadh_pr = 1, dadh_cub = 1, dadh_oth = 1, dadr_wht = 1, 
                     dadr_blk = 1, dadr_ami = 1, dadr_asi = 1, dadr_chi = 1, 
                     dadr_fil = 1, dadr_jap = 1, dadr_kor = 1, dadr_vie = 1, 
                     dadr_oas = 1, dadr_haw = 1, dadr_gua = 1, dadr_sam = 1, 
                     dadr_opi = 1, dadr_oth = 1, momle8ed = 1, dadle8ed = 1, 
                     otherout = 3, oth_mo = 2, oth_yr = 4, delivpay = 1, fpv_mo = 2, 
                     fpv_da = 2, fpv_yr = 4, lpv_mo = 2, lpv_da = 2, lpv_yr = 4, 
                     cigs_bef = 2, cigs_1st = 2, cigs_2nd = 2, cigs_3rd = 2, 
                     apgar10 = 3, circumf = 2, wghtdelv = 3, ht_ft = 1, ht_in = 2, 
                     breastfd = 1, wic_new = 1, hyperflg = 1, cephaflg = 1, 
                     prev_cno = 2, forcfail = 1, vacfail = 1, fet_pres = 1, dmethfin = 1, 
                     labchar1 = 2, labchar2 = 2, labchar3 = 2, labchar4 = 2, 
                     labchar5 = 2, labchar6 = 2, labchar7 = 2, minfect1 = 2, 
                     minfect2 = 2, minfect3 = 2, minfect4 = 2, minfect5 = 2, 
                     minfect6 = 2, minfect7 = 2, mmorbid1 = 2, mmorbid2 = 2, 
                     mmorbid3 = 2, mmorbid4 = 2, mmorbid5 = 2, 
                     labons1 = 2, labons2 = 2, labons3 = 2, 
                     downsflg = 1, chromflg = 1, moracsum = 2, faracsum = 2, 
                     BMI = 4, pnatfed = 3)

col_fixed_width <- list(certno_e = c(1, 10), certype = c(11, 11), dob_yr = c(12, 15), 
                        dob_mo = c(16, 17), birth_hr = c(20, 21), sex = c(22, 22), 
                        plural = c(23, 23), city_occ = c(24, 27), 
                        # cnty_occ = c(24, 25), 
                        facility = c(28, 30), age_mom = c(31, 32), birplmom = c(33, 34), 
                        city_res = c(35, 38), 
                        #cnty_res = c(35, 36), 
                        age_dad = c(39, 40), 
                        birpldad = c(41, 42), attclass = c(43, 44), race_dad = c(45, 45),
                        race_mom = c(46, 46), raceccal = c(47, 47), educ_dad = c(48, 49), 
                        educ_mom = c(50, 51), lb_nl = c(52, 54), priorprg = c(55, 57), 
                        pnatalmo = c(76, 78), pnatalvs = c(79, 81), married = c(82, 82), 
                        wt_grams = c(83, 86), fac_type = c(101, 101), occm_dad = c(102, 104), 
                        occm_mom = c(105, 107), lb_nd = c(108, 110), fd_lt20 = c(111, 113), 
                        fd_ge20 = c(114, 116), apgar1 = c(117, 119), apgar5 = c(120, 122), 
                        order = c(123, 123), crt_clas = c(124, 125), ind_num = c(134, 136), 
                        trnsferm = c(137, 137), trnsferb = c(138, 138), malf_sam = c(145, 147), 
                        hisp_dad = c(148, 148), hisp_mom = c(149, 149), hispccal = c(150, 150), 
                        res_yprt = c(151, 152), res_mprt = c(153, 154), gest_est = c(167, 169), 
                        zipcode = c(170, 174), 
                        mrf1 = c(179, 180), mrf2 = c(181, 182), mrf3 = c(183, 184), 
                        mrf4 = c(185, 186), mrf5 = c(187, 188), mrf6 = c(189, 190), 
                        diabetes = c(191, 191), herpes = c(192, 192), smokenum = c(193, 195), 
                        drinknum = c(196, 198), wghtgain = c(199, 201), 
                        obproc1 = c(202, 202), obproc2 = c(203, 203), obproc3 = c(204, 204), 
                        obproc4 = c(205, 205), 
                        amnio1 = c(206, 206), amnio2 = c(207, 207), amnio3 = c(208, 208), 
                        dmeth1 = c(209, 209), dmeth2 = c(210, 210), dmeth3 = c(211, 211), 
                        dmeth4 = c(212, 212), 
                        complab1 = c(213, 214), complab2 = c(215, 216), complab3 = c(217, 218), 
                        complab4 = c(219, 220), complab5 = c(221, 222), complab6 = c(223, 224), 
                        complab7 = c(225, 226), 
                        abcond1 = c(227, 228), abcond2 = c(229, 230), abcond3 = c(231, 232), 
                        abcond4 = c(233, 234), abcond5 = c(235, 236), abcond6 = c(237, 238), 
                        malf1 = c(239, 240), malf2 = c(241, 242), malf3 = c(243, 244), 
                        malf4 = c(245, 246), malf5 = c(247, 248), malf6 = c(249, 250), 
                        malf7 = c(251, 252), 
                        st_occ = c(253, 254), st_res = c(255, 256), racecdes = c(257, 257), 
                        hispcdes = c(258, 258), carepay = c(259, 259), wic = c(260, 260), 
                        firsteps = c(261, 261), afdc = c(262, 262), localhd = c(263, 263), 
                        wghtpre = c(264, 266), 
                        ubleed1 = c(267, 267), ubleed2 = c(268, 268), ubleed3 = c(269, 269), 
                        smoking = c(270, 270), drinking = c(271, 271), 
                        lb_f_nl = c(281, 283), lb_f84 = c(284, 286), 
                        lb_p_nl = c(287, 289), lb_p84 = c(290, 292), 
                        lb_p_nd = c(293, 295), lb_f_nd = c(296, 298), 
                        fd_2036 = c(299, 301), fd_ge37 = c(302, 304), 
                        malficd1 = c(305, 306), malficd2 = c(307, 308), malficd3 = c(309, 310), 
                        malficd4 = c(311, 312), malficd5 = c(313, 314), malficd6 = c(315, 316), 
                        malficd7 = c(317, 318), malficd8 = c(319, 320), 
                        gestcalc = c(337, 338), gestflag = c(339, 339), 
                        mrfnon1 = c(340, 341), mrfnon2 = c(342, 343), mrfnon3 = c(344, 345), 
                        mrfnon4 = c(346, 347), mrfnon5 = c(348, 349), mrfnon6 = c(350, 351), 
                        mrfnon7 = c(352, 353), mrfnon8 = c(354, 355), mrfnon9 = c(356, 357), 
                        nchsnew = c(358, 358), dthflg = c(359, 359), 
                        ind_mo = c(360, 361), ind_yr = c(362, 365), 
                        llb_mo = c(366, 367), llb_yr = c(368, 371), 
                        lfd_mo = c(372, 373), lfd_yr = c(374, 377), 
                        mens_mo = c(378, 379), mens_da = c(380, 381), mens_yr = c(382, 385), 
                        loth_mo = c(386, 387), loth_yr = c(388, 391), 
                        lpp_mo = c(392, 393), lpp_yr = c(394, 397), 
                        bctrymom = c(398, 400), bctrydad = c(401, 403), geozip = c(404, 408), 
                        alive = c(409, 409), fac_int = c(410, 410), trib_res = c(411, 413), 
                        momh_no = c(414, 414), momh_mex = c(415, 415), momh_pr = c(416, 416), 
                        momh_cub = c(417, 417), momh_oth = c(418, 418), momr_wht = c(419, 419), 
                        momr_blk = c(420, 420), momr_ami = c(421, 421), momr_asi = c(422, 422), 
                        momr_chi = c(423, 423), momr_fil = c(424, 424), momr_jap = c(425, 425), 
                        momr_kor = c(426, 426), momr_vie = c(427, 427), momr_oas = c(428, 428), 
                        momr_haw = c(429, 429), momr_gua = c(430, 430), momr_sam = c(431, 431), 
                        momr_opi = c(432, 432), momr_oth = c(433, 433), 
                        dadh_no = c(434, 434), dadh_mex = c(435, 435), dadh_pr = c(436, 436), 
                        dadh_cub = c(437, 437), dadh_oth = c(438, 438), dadr_wht = c(439, 439), 
                        dadr_blk = c(440, 440), dadr_ami = c(441, 441), dadr_asi = c(442, 442), 
                        dadr_chi = c(443, 443), dadr_fil = c(444, 444), dadr_jap = c(445, 445), 
                        dadr_kor = c(446, 446), dadr_vie = c(447, 447), dadr_oas = c(448, 448), 
                        dadr_haw = c(449, 449), dadr_gua = c(450, 450), dadr_sam = c(451, 451), 
                        dadr_opi = c(452, 452), dadr_oth = c(453, 453), 
                        momle8ed = c(454, 454), dadle8ed = c(455, 455), 
                        otherout = c(456, 458), oth_mo = c(459, 460), oth_yr = c(461, 464), 
                        delivpay = c(465, 465), 
                        fpv_mo = c(466, 467), fpv_da = c(468, 469), fpv_yr = c(470, 473), 
                        lpv_mo = c(474, 475), lpv_da = c(476, 477), lpv_yr = c(478, 481), 
                        cigs_bef = c(482, 483), cigs_1st = c(484, 485), cigs_2nd = c(486, 487), 
                        cigs_3rd = c(488, 489), 
                        apgar10 = c(490, 492), circumf = c(493, 494), wghtdelv = c(495, 497), 
                        ht_ft = c(498, 498), ht_in = c(499, 500), breastfd = c(501, 501), 
                        wic_new = c(502, 502), hyperflg = c(503, 503), cephaflg = c(504, 504), 
                        prev_cno = c(505, 506), forcfail = c(507, 507), vacfail = c(508, 508), 
                        fet_pres = c(509, 509), dmethfin = c(510, 510), 
                        labchar1 = c(511, 512), labchar2 = c(513, 514), labchar3 = c(515, 516), 
                        labchar4 = c(517, 518), labchar5 = c(519, 520), labchar6 = c(521, 522), 
                        labchar7 = c(523, 524), 
                        minfect1 = c(525, 526), minfect2 = c(527, 528), minfect3 = c(529, 530), 
                        minfect4 = c(531, 532), minfect5 = c(533, 534), minfect6 = c(535, 536), 
                        minfect7 = c(537, 538), 
                        mmorbid1 = c(539, 540), mmorbid2 = c(541, 542), mmorbid3 = c(543, 544), 
                        mmorbid4 = c(545, 546), mmorbid5 = c(547, 548), 
                        labons1 = c(549, 550), labons2 = c(551, 552), labons3 = c(553, 554), 
                        downsflg = c(555, 555), chromflg = c(556, 556), 
                        moracsum = c(557, 558), faracsum = c(559, 560), 
                        BMI = c(561, 564), pnatfed = c(565, 567))


#### Load data to R ####
# Find list of files with years 2003-2016 inclusive
bir_file_names <- list.files(path = file.path(bir_path, "DATA/raw"), pattern = "birth20(0[3-9]{1}|1[0-6]{1}).(asc|csv)$",
                                  full.names = T)
bir_names <- lapply(bir_file_names, function(x) 
  str_sub(x,
          start = str_locate(x, "birth20")[1], 
          end = str_locate(x, "birth20[0-9]{2}")[2])
  )

# Bring in data
bir_files <- lapply(bir_file_names, function(x) {
  # Remove pnatfed from list of variables if before 2011
  if (as.numeric(str_sub(x, -8, -5)) < 2011) {
    col_fixed_width <- col_fixed_width[str_detect(names(col_fixed_width), "pnatfed") == F]
    col_type_list <- col_type_list[str_detect(names(col_type_list), "pnatfed") == F]
  }
  
  if (str_detect(x, ".asc")) {
    vroom::vroom_fwf(file = x,
                     fwf_positions(
                       start = sapply(col_fixed_width, function(y) y[1]),
                       end = sapply(col_fixed_width, function(y) y[2]),
                       col_names = names(col_fixed_width)),
                     col_types = col_type_list,
                     n_max = 100)
  } else if (str_detect(x, ".csv")) {
    vroom::vroom(file = x, col_select = names(col_type_list),
                 col_types = col_type_list,
                 n_max = 100)
  }
})

# Rename list items
names(bir_files) <- bir_names

# Change BMI name back to lower case
bir_files <- lapply(bir_files, data.table::setnames, "BMI", "bmi")


#### Obtian batch ID for each file ####


# For each year:
# Get batch ID
# QA column order
# Add batch ID to file


# Combine and load to SQL

#### Load data to SQL ####


