#### CODE TO LOAD GEOCODED BIRTH DATA FROM THE BEDROCK SYSTEM (2003-2016)
# Alastair Matheson, PHSKC (APDE)
#
# 2019-07


load_load_raw.bir_wa_geo_2003_2016_f <- function(table_config_create = NULL,
                                                 table_config_load = NULL,
                                                 bir_path_inner = bir_path_geo,
                                                 conn = db_apde) {
  
  
  #### ERROR CHECKS ####
  if (is.null(table_config_create)) {
    message("No table creation config file specified, attempting default URL")
    
    table_config_create <- yaml::yaml.load(getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_geo_2003_2016.yaml"))
  }
  
  
  if (is.null(table_config_load)) {
    message("No table load config file specified, attempting default URL")
    
    table_config_load <- yaml::yaml.load(getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_geo_2003_2016.yaml"))
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
  col_fixed_width_2003_2016 <- list(
    certno_e = c(1, 10), r_co = c(11, 12), fips_co = c(13, 15), geo_zip = c(17, 21), 
    tract90 = c(26, 32), blgrp90 = c(33, 33), r_zip = c(34, 38), 
    xtract90 = c(39, 44), xblgrp90 = c(45, 45), 
    latitude = c(46, 57), longitude = c(59, 69), 
    source = c(70, 89), score = c(90, 92), 
    tract00 = c(93, 99), blgrp00 = c(93, 100), scd = c(104, 108), 
    tract10d = c(118, 125), bgp10d = c(118, 125), fips = c(118, 129))
  
  
  #### LOAD 2003-2016 DATA TO R ####
  # Find list of files with years 2003-2016 inclusive
  bir_file_names_2003_2016 <- list.files(path = file.path(bir_path_inner, "raw"), 
                                         pattern = "birth_20(0[3-9]{1}|1[0-6]{1})_geo.(asc|csv)$",
                                         full.names = T)
  bir_names_2003_2016 <- lapply(bir_file_names_2003_2016, function(x)
    str_sub(x,
            start = str_locate(x, "birth_20")[1], 
            end = str_locate(x, "birth_20[0-9]{2}_geo")[2])
  )
  
  # Bring in data
  bir_files_2003_2016 <- lapply(bir_file_names_2003_2016, function(x) {
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
  print("Combining years into a single file")
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
  print("Loading data to SQL")
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




