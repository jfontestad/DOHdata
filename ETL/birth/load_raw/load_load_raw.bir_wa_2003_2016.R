#### CODE TO LOAD BIRTH DATA FROM THE BEDROCK SYSTEM (2003-2016)
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05


load_load_raw.bir_wa_2003_2016_f <- function(table_config_create = NULL,
                                             table_config_load = NULL,
                                             bir_path_inner = bir_path,
                                             conn = db_apde) {
  
  
  #### ERROR CHECKS ####
  if (is.null(table_config_create)) {
    print("No table creation config file specified, attempting default URL")
    
    table_config_create <- yaml::yaml.load(getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2003_2016.yaml"))
  }
  
  
  if (is.null(table_config_load)) {
    print("No table load config file specified, attempting default URL")
    
    table_config_load <- yaml::yaml.load(getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2003_2016.yaml"))
  }
  
  
  #### CALL IN FUNCTIONS IF NOT ALREADY LOADED ####
  if (exists("load_metadata_etl_log_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/scripts_general/etl_log.R")
  }
  
  
  #### SET UP LOADING VARIABLES FROM CONFIG FILES ####
  # Recode list of variables to match format required for importing
  col_type_list_2003_2016 <- lapply(table_config_create$vars, function(x) {
    x <- str_replace_all(x, "INTEGER", "i")
    x <- str_replace_all(x, "VARCHAR\\(\\d+\\)", "c")
    x <- str_replace_all(x, "CHAR\\(\\d+\\)", "c")
    x <- str_replace_all(x, "NUMERIC", "d")
    # Read dates in as characters and convert later
    x <- str_replace_all(x, "DATE", "c")
  })
  
  # Make specific change to BMI
  names(col_type_list_2003_2016)[str_detect(names(col_type_list_2003_2016), "bmi")] <- "BMI"
  # Remove fields that will go in the loaded table but aren't in all data files
  col_type_list_2003_2016 <- col_type_list_2003_2016[
    str_detect(names(col_type_list_2003_2016), "etl_batch_id") == F & 
      str_detect(names(col_type_list_2003_2016), "md4y") == F]
  
  
  #### SET UP FIXED WIDTH FILES ####
  col_fixed_width_2003_2016 <- list(certno_e = c(1, 10), certype = c(11, 11), dob_yr = c(12, 15), 
                                    dob_mo = c(16, 17), birth_hr = c(20, 21), sex = c(22, 22), 
                                    plural = c(23, 23), 
                                    city_occ = c(24, 27), cnty_occ = c(24, 25), 
                                    facility = c(28, 30), age_mom = c(31, 32), birplmom = c(33, 34), 
                                    city_res = c(35, 38), cnty_res = c(35, 36), 
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
                                    malficd1 = c(305, 308), malficd2 = c(309, 312), malficd3 = c(313, 316), 
                                    malficd4 = c(317, 320), malficd5 = c(321, 324), malficd6 = c(325, 328), 
                                    malficd7 = c(329, 332), malficd8 = c(333, 336), 
                                    gestcalc = c(337, 338), gestflag = c(339, 339), 
                                    mrfnon1 = c(340, 341), mrfnon2 = c(342, 343), mrfnon3 = c(344, 345), 
                                    mrfnon4 = c(346, 347), mrfnon5 = c(348, 349), mrfnon6 = c(350, 351), 
                                    mrfnon7 = c(352, 353), mrfnon8 = c(354, 355), mrfnon9 = c(356, 357), 
                                    nchsnew = c(358, 358), dthflg = c(359, 359), 
                                    ind_mo = c(360, 361), ind_yr = c(362, 365), 
                                    llb_mo = c(366, 367), llb_yr = c(368, 371), 
                                    lfd_mo = c(372, 373), lfd_yr = c(374, 377), 
                                    mens_mo = c(378, 379), mens_da = c(380, 381), mens_yr = c(382, 385), 
                                    #mensmd4y = c(378, 385),
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
                                    #fpvmd4y = c(466, 473),
                                    lpv_mo = c(474, 475), lpv_da = c(476, 477), lpv_yr = c(478, 481), 
                                    #lpvmd4y = c(474, 481),
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
  
  
  #### LOAD 2003-2016 DATA TO R ####
  # Find list of files with years 2003-2016 inclusive
  bir_file_names_2003_2016 <- list.files(path = file.path(bir_path_inner, "DATA/raw"), 
                                         pattern = "birth_20(0[3-9]{1}|1[0-6]{1}).(asc|csv)$",
                                         full.names = T)
  bir_names_2003_2016 <- lapply(bir_file_names_2003_2016, function(x)
    str_sub(x,
            start = str_locate(x, "birth_20")[1], 
            end = str_locate(x, "birth_20[0-9]{2}")[2])
  )
  
  # Bring in data
  bir_files_2003_2016 <- lapply(bir_file_names_2003_2016, function(x) {
    # Remove pnatfed from list of variables if before 2011
    if (as.numeric(str_sub(x, -8, -5)) < 2011) {
      col_fixed_width_2003_2016 <- col_fixed_width_2003_2016[str_detect(names(col_fixed_width_2003_2016), "pnatfed") == F]
      col_type_list_2003_2016 <- col_type_list_2003_2016[str_detect(names(col_type_list_2003_2016), "pnatfed") == F]
    }
    
    if (str_detect(x, ".asc")) {
      vroom::vroom_fwf(file = x,
                       fwf_positions(
                         start = sapply(col_fixed_width_2003_2016, function(y) y[1]),
                         end = sapply(col_fixed_width_2003_2016, function(y) y[2]),
                         col_names = names(col_fixed_width_2003_2016)),
                       col_types = col_type_list_2003_2016)
    } else if (str_detect(x, ".csv")) {
      vroom::vroom(file = x, col_select = names(col_type_list_2003_2016),
                   col_types = col_type_list_2003_2016)
    }
  })
  
  # Rename list items
  names(bir_files_2003_2016) <- bir_names_2003_2016
  
  # Change BMI name back to lower case
  bir_files_2003_2016 <- lapply(bir_files_2003_2016, data.table::setnames, "BMI", "bmi")
  
  
  #### BATCH ID FOR 2003-2016 ####
  batch_ids_2003_2016 <- lapply(bir_names_2003_2016, function(x) {
    # Set up variables
    table_name <- paste0("table_", str_sub(x, -4, -1))
    date_min_txt <- table_config_load[[table_name]][["date_min"]]
    date_max_txt <- table_config_load[[table_name]][["date_max"]]
    date_issue_txt <- table_config_load[[table_name]][["date_issue"]]
    date_delivery_txt <- table_config_load[[table_name]][["date_delivery"]]
    
    # Obtain batch ID for each file
    # Skip checking most recent entries
    current_batch_id <- load_metadata_etl_log_f(conn = conn,
                                                data_source = "birth",
                                                date_min = date_min_txt,
                                                date_max = date_max_txt,
                                                date_issue = date_issue_txt,
                                                date_delivery = date_delivery_txt,
                                                note = "Retroactively adding older years",
                                                auto_proceed = T)
    
    return(current_batch_id)
  })
  
  
  # Combine IDs with data frames in list
  bir_files_2003_2016 <- Map(cbind, bir_files_2003_2016, etl_batch_id = batch_ids_2003_2016)
  
  
  #### COMBINE 2003-2016 INTO A SINGLE DATA FRAME ####
  bir_2003_2016 <- bind_rows(bir_files_2003_2016)
  
  ### Add overlapping variables
  bir_2003_2016 <- bir_2003_2016 %>%
    mutate(mensmd4y = as.Date(paste(mens_yr, mens_mo, mens_da, sep = "-"), format = "%Y-%m-%d"),
           fpvmd4y = as.Date(paste(fpv_yr, fpv_mo, fpv_da, sep = "-"), format = "%Y-%m-%d"),
           lpvmd4y = as.Date(paste(lpv_yr, lpv_mo, lpv_da, sep = "-"), format = "%Y-%m-%d")
    )
  
  ### Reorder to match SQL table
  bir_2003_2016 <- bir_2003_2016 %>%
    select(certno_e:mens_yr, mensmd4y, loth_mo:fpv_yr, fpvmd4y, lpv_mo:lpv_yr, lpvmd4y,
           cigs_bef:bmi, pnatfed, etl_batch_id)
  
  
  #### LOAD 2003-2016 DATA TO SQL ####
  # Need to manually truncate table so can use overwrite = F below (so column types work)
  dbGetQuery(conn, glue_sql("TRUNCATE TABLE {`table_config_load$schema`}.{`table_config_load$table`}",
                            .con = conn))
  
  tbl_id_2013_2016 <- DBI::Id(schema = table_config_load$schema, table = table_config_load$table)
  dbWriteTable(conn, tbl_id_2013_2016, value = as.data.frame(bir_2003_2016),
               overwrite = F,
               append = T,
               field.types = paste(names(table_config_create$vars), 
                                   table_config_create$vars, 
                                   collapse = ", ", sep = " = "))
  

  #### COLLATE OUTPUT TO RETURN ####
  rows_to_load <- nrow(bir_2003_2016)
  batch_etl_id_min <- min(bir_2003_2016$etl_batch_id)
  batch_etl_id_max <- max(bir_2003_2016$etl_batch_id)
  
  
  #### ADD INDEX TO DATA ####
  if (!is.null(table_config_load$index) & !is.null(table_config_load$index_name)) {
    # Remove index from table if it exists
    # This code pulls out the clustered index name
    index_name <- dbGetQuery(conn, 
                             glue::glue_sql("SELECT DISTINCT a.index_name
                                                FROM
                                                (SELECT ind.name AS index_name
                                                  FROM
                                                  (SELECT object_id, name, type_desc FROM sys.indexes
                                                    WHERE type_desc = 'CLUSTERED') ind
                                                  INNER JOIN
                                                  (SELECT name, schema_id, object_id FROM sys.tables
                                                    WHERE name = {`table`}) t
                                                  ON ind.object_id = t.object_id
                                                  INNER JOIN
                                                  (SELECT name, schema_id FROM sys.schemas
                                                    WHERE name = {`schema`}) s
                                                  ON t.schema_id = s.schema_id) a",
                                            .con = conn,
                                            table = dbQuoteString(conn, table_config_load$table),
                                            schema = dbQuoteString(conn, table_config_load$schema)))[[1]]
    
    if (length(index_name) != 0) {
      dbGetQuery(conn,
                 glue::glue_sql("DROP INDEX {`index_name`} ON 
                                  {`table_config_load$schema`}.{`table_config_load$table`}", .con = conn))
    }
    
    index_sql <- glue::glue_sql("CREATE CLUSTERED INDEX [{`table_config_load$index_name`}] ON 
                              {`table_config_load$schema`}.{`table_config_load$table`}({index_vars*})",
                                index_vars = dbQuoteIdentifier(conn, table_config_load$index),
                                .con = conn)
    dbGetQuery(conn, index_sql)
  }
  
  
  output <- list(
    rows_to_load = rows_to_load,
    batch_etl_id_min = batch_etl_id_min,
    batch_etl_id_max = batch_etl_id_max
  )
  
  print(glue::glue("load_raw.bir_wa_2003_2016 loaded to SQL ({rows_to_load} rows)"))
  return(output)
  
}





