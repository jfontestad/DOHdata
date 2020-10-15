# Header ----
    # Author: Danny Colombara
    # Date: October 13, 2020
    # Purpose: Create data dictionary for final birth data on SQL
    # Notes:
    #
    #

# Set up ----
    rm(list=ls())
    pacman::p_load(data.table, rads)
    options("scipen"=10) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL


# Use RADS to pull all birth data ----
    birth <- get_data_birth(year = 2003:2018, kingco = T)
    birth <- sql_clean(birth, stringsAsFactors = T)


# Create data dictionary ----
    ## Identify column type ----
        temp.vartype <- data.table(varname = names(sapply(birth, class)), vartype = sapply(birth, class))
        temp.binary <- data.table(varname = names(sapply(birth,function(x) { length(unique(na.omit(x))) == 2 })), binary = sapply(birth,function(x) { length(unique(na.omit(x))) == 2 }))
        dict <- merge(temp.vartype, temp.binary, by = "varname")
        dict[binary == "TRUE", vartype := paste0("binary (", vartype, ")")]
        dict[, binary := NULL]
        dict[, varname := factor(varname, levels = names(birth))]
        setorder(dict, varname)
    
    ## Extract values for each var ----
        for(i in seq(1, ncol(birth), 1) ){
          print(names(birth)[i])
          dict[varname == names(birth)[i] & length(unique( birth[[names(birth)[i]]] )) <= 31, value := paste(sort(unique(birth[[names(birth)[i]]])), collapse = ", ") ] # only keep up to 31 unique values
          dict[varname == names(birth)[i] & length(unique( birth[[names(birth)[i]]] )) > 31, value := paste("E.g.,", paste(sort(unique(birth[[names(birth)[i]]]))[1:5], collapse = ", ") ) ] # just a sample when > 31 values
        }
        
    ## Identify years available ----
        mygrid <- setDT(expand.grid(varname = names(birth), cal_year = seq(min(birth$chi_year), max(birth$chi_year), 1))) # 1 year per each unique variable in birth
        res <- birth[, lapply(.SD, function(x) sum(!is.na(x))), .SDcols = names(birth), by = chi_year] # creates wide table
        res <- melt(res, id.vars = 'chi_year', variable.name = 'varname', value.name = "count", variable.factor = F) # reshape long
        mygrid <- merge(mygrid, res, all.x = T, by.x = c('cal_year', 'varname'), by.y = c('chi_year', 'varname')) # table with number of non-missing values per year

        mygrid <- mygrid[count != 0] # drop all rows where was 100% missing
        
        mygrid <- mygrid[, .(years = rads::format_time(cal_year)), by = "varname"] # collapse years into easy to read format
        
        # merge years onto the dictionary
        dict <- merge(dict, mygrid, all = T)
        dict[is.na(years), years := "None"] # it is possible that there is a variable that is 100% missing every year
        
        
    ## Merge on existing dictionary type information ----
        # Add field map codings ----
          field_name_map <- fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_field_name_map.csv")
          sql_clean(field_name_map, stringsAsFactors = F)
          field_name_map <- field_name_map[!is.na(field_name_apde)]
          field_name_map[, c("field_number", "sql_format") := NULL]
          
          # fix known duplicates
            field_name_map[field_name_apde == "mother_residence_county_wa_code" & field_name_bedrock=="cnty_res", field_name_bedrock := "cnty_res, r_co"]
            field_name_map <- field_name_map[! (field_name_apde == "mother_residence_county_wa_code" & field_name_bedrock == "r_co")]          
            
          # Identify additional unknown duplicates
            if(nrow(field_name_map[duplicated(field_name_map$field_name_apde)]) > 0){
              stop(print(paste0("Deduplicate the following: ", field_name_map[duplicated(field_name_map$field_name_apde)]$field_name_apde)))
            }
          
          dict <- merge(dict, field_name_map, by.x = "varname", by.y = "field_name_apde", all.x = T, all.y = F)

        # Add info from simple recodes  ----
          recodes <- fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_recodes_simple.csv")
          sql_clean(recodes, stringsAsFactors = F)
          recodes[recode_type=="complex", notes := "complex recoding"]
          recodes[old_var %in% dict$field_name_whales, field_name_whales := old_var]            
          recodes[old_var %in% dict$field_name_bedrock, field_name_bedrock := old_var]            
          recodes <- unique(recodes[, .(varname = new_var, sourcevar = old_var, varlabel = var_label, field_name_whales.y = field_name_whales, field_name_bedrock.y = field_name_bedrock, notes)])
          
          # fix known duplicates
            recodes[, dup := .N, by = "varname"]
            recodes <- recodes[!(dup != 1 & is.na(sourcevar))]
            recodes <- recodes[!(dup != 1 & varname == sourcevar)]
            recodes[, dup := NULL]
            
          # Identify additional unknown duplicates
            if(nrow(recodes[duplicated(recodes$varname)]) > 0){
              stop(print(paste0("Deduplicate the following: ", recodes[duplicated(recodes$varname)]$varname)))
            }
          
          dict <- merge(dict, recodes, by = "varname", all.x = T, all.y = F)
          
          dict[is.na(field_name_whales) & !is.na(field_name_whales.y), field_name_whales := field_name_whales.y][, field_name_whales.y:=NULL]
          dict[is.na(field_name_bedrock) & !is.na(field_name_bedrock.y), field_name_bedrock := field_name_bedrock.y][, field_name_bedrock.y:=NULL]   
          
          dict[is.na(description) & !is.na(varlabel), description := varlabel]
          
        # Add info from existing dictionary ----
          existing <- fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref_bir_user_dictionary_final.csv") # use for notes and description data
          existing <- unique(existing[, .(varname, description.y = description, notes.y = notes)])
          # existing <- existing[varname %in% dict[is.na(description)]$varname]
          
          dict <- merge(dict, existing, by = "varname", all.x = T, all.y = F)
          
          dict[is.na(description) & !is.na(description.y), description := description.y]
          dict[is.na(notes) & !is.na(notes.y), notes := notes.y]
          dict[, c("description.y", "notes.y") := NULL]
          

        # Tidy ----
          dict[, sourcevar := paste(sourcevar, field_name_whales, field_name_bedrock, sep = ", "), by = "varname"][, sourcevar := gsub("NA, |, NA$", "", sourcevar)]

          dict <- dict[, .(varname, years, vartype, description, value, sourcevar, field_name_whales, field_name_bedrock, notes)]


# Write final output ----
   write.csv(dict, file = paste0("C:/code/DOHdata/ETL/birth/ref/ref_bir_user_dictionary_final.csv"), row.names = FALSE)
    
    

    
# the end ----    