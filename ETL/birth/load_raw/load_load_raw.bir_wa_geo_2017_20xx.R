#### CODE TO LOAD BIRTH DATA FROM THE WHALSE SYSTEM (2017-onwards)
# Alastair Matheson, PHSKC (APDE)
#
# 2019-06


load_load_raw.bir_wa_geo_2017_20xx_f <- function(table_config_load = NULL,
                                                 bir_path_inner = bir_path,
                                                 conn = db_apde) {
  
  
  #### ERROR CHECKS ####
  if (is.null(table_config_load)) {
    print("No table load config file specified, attempting default URL")
    
    table_config_load <- yaml::yaml.load(getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_geo_2017_20xx.yaml"))
  }
  
  
  #### CALL IN FUNCTIONS/TABLES IF NOT ALREADY LOADED ####
  if (exists("load_metadata_etl_log_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/scripts_general/etl_log.R")
  }
  
  if (exists("bir_field_map") == F) {
    bir_field_map <- vroom::vroom("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_field_name_map.csv")
  }
  
  
  #### SET UP LOADING VARIABLES FROM CONFIG FILES ####
  # Recode list of variables to match format required for importing
  col_type_list_2017_20xx <- lapply(table_config_load$vars, function(x) {
    x <- str_replace_all(x, "INTEGER", "i")
    x <- str_replace_all(x, "VARCHAR\\(\\d+\\)", "c")
    x <- str_replace_all(x, "CHAR\\(\\d+\\)", "c")
    x <- str_replace_all(x, "NUMERIC", "d")
    # Read dates in as characters and convert later
    x <- str_replace_all(x, "DATE", "c")
  })
  
  
  # Remove fields that will go in the loaded table but aren't in all data files
  col_type_list_2017_20xx <- col_type_list_2017_20xx[str_detect(names(col_type_list_2017_20xx), "etl_batch_id") == F]
  
  # Rename fields to match what is actually loaded from the csv
  names(col_type_list_2017_20xx) <- bir_field_map$field_name_whales[match(names(col_type_list_2017_20xx), 
                                                                                bir_field_map$field_name_apde)]
  

  #### LOAD 2017-20xx DATA TO R ####
  # Find list of files with years 2017-20xx inclusive
  bir_file_names_2017_20xx <- list.files(path = file.path(bir_path_inner, "raw"), 
                                         pattern = "birth_20(1[7-9]{1}|2[0-9]{1})_geo.(asc|csv|xls|xlsx)$",
                                         full.names = T)
  
  # Add check to make sure some files are found
  if(length(bir_file_names_2017_20xx) == 0) {
    stop("No files found to load. Check path and file names.")
  }
  
  bir_names_2017_20xx <- lapply(bir_file_names_2017_20xx, function(x)
    str_sub(x,
            start = str_locate(x, "birth_20")[1], 
            end = str_locate(x, "birth_20[0-9]{2}")[2])
  )
  
  # Bring in data
  bir_files_2017_20xx <- lapply(bir_file_names_2017_20xx, function(x) {
    vroom::vroom(file = x,
                 col_types = col_type_list_2017_20xx,
                 .name_repair = ~ janitor::make_clean_names(., case = "snake"))
  })
  
  # Rename list items
  names(bir_files_2017_20xx) <- bir_names_2017_20xx
  
  
  #### BATCH ID FOR 2017-20xx ####
  batch_ids_2017_20xx <- lapply(bir_names_2017_20xx, function(x) {
    # Set up variables
    table_name <- paste0("table_", str_sub(x, -4, -1))
    date_min_txt <- table_config_load[[table_name]][["date_min"]]
    date_max_txt <- table_config_load[[table_name]][["date_max"]]
    date_issue_txt <- table_config_load[[table_name]][["date_issue"]]
    date_delivery_txt <- table_config_load[[table_name]][["date_delivery"]]
    note_txt <- table_config_load[[table_name]][["note"]]
    
    # Obtain batch ID for each file
    # Skip checking most recent entries
    current_batch_id <- load_metadata_etl_log_f(conn = conn,
                                                data_source = "birth_geo",
                                                date_min = date_min_txt,
                                                date_max = date_max_txt,
                                                date_issue = date_issue_txt,
                                                date_delivery = date_delivery_txt,
                                                note = note_txt,
                                                auto_proceed = F)
    
    return(current_batch_id)
  })
  
  
  # Combine IDs with data frames in list
  bir_files_2017_20xx <- Map(cbind, bir_files_2017_20xx, etl_batch_id = batch_ids_2017_20xx)
  
  
  #### COMBINE 2017-20xx INTO A SINGLE DATA FRAME ####
  message("Combining years into a single file")
  bir_2017_20xx <- bind_rows(bir_files_2017_20xx)
  
  ## Check snake_case matches what is expected for SQL table
  if (min(names(bir_2017_20xx[!names(bir_2017_20xx) %in% c("etl_batch_id")])
          %in% bir_field_map$field_name_apde) == 0) {
    stop("There is an error in the fields names of the 2017_20xx combined data.")
  }

  #### LOAD 2017-20xx DATA TO SQL ####
  message("Loading data to SQL")
  tbl_id_2017_20xx <- DBI::Id(schema = table_config_load$schema, table = table_config_load$table)
  dbWriteTable(conn, tbl_id_2017_20xx, value = as.data.frame(bir_2017_20xx), 
               overwrite = T,
               append = F,
               field.types = unlist(table_config_load$vars))
  
  
  
  # 
  # bir_2017_20xx_names <- as.list(names(temp))
  # 
  # test_len <- sapply(bir_2017_20xx_names, function(x) {
  #   max_len <- max(nchar(temp[[x]]), na.rm = T)
  # 
  # })
  # 
  # spec_len <- data.frame(field = names(table_config_load$vars)[names(table_config_load$vars) != "etl_batch_id"],
  #                        type = unlist(table_config_load$vars)[names(table_config_load$vars) != "etl_batch_id"],
  #                        max_len = test_len) %>%
  #   mutate(char_det = str_detect(type, "CHAR"),
  #          spec_len = as.numeric(case_when(
  #            char_det ~ str_sub(type,
  #                               str_locate(type, "\\(")[,1]+1,
  #                               str_locate(type, "\\)")[,1]-1)
  #          ))
  #   )
  # 
  # spec_len %>% filter(max_len > spec_len)
  # 
  # 
  # print(names(dbGetQuery(conn, "SELECT TOP (0) * FROM load_raw.bir_wa_geo_2017_20xx")))
  
  
  #### COLLATE OUTPUT TO RETURN ####
  rows_to_load <- nrow(bir_2017_20xx)
  batch_etl_id_min <- min(bir_2017_20xx$etl_batch_id)
  batch_etl_id_max <- max(bir_2017_20xx$etl_batch_id)
  
  
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
  
  print(glue::glue("load_raw.bir_wa_geo_2017_20xx loaded to SQL ({rows_to_load} rows)"))
  return(output)
  
}
