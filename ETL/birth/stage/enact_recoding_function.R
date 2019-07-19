## Header ####
# Author: Danny Colombara
# 
# R version: 3.5.3
#
# Purpose: Use the recoding functions created by Daniel for HYS
# 
# Notes: Copied from https://github.com/PHSKC-APDE/svy_hys_new/blob/dev/util/enact_recoding.R on 7/19/2019
#        Dropped all code that was HYS specific
# 
#        This function takes the dataset, the year the dataset represents, and a series of recode instructions (via ...) and applies them
# 

enact_recoding = function(data, year, ..., ignore_case = T, hypothetical = F){
  
  #copy data here so the scope is protected, but so that we can also use scope jumping when applying recodes
  data = copy(data)
  data[, blankblank := NA] #for tricksy recodes
  
  #create a list of recodes
  dots = list(...)
  
  #check the dots
  classy = vapply(dots, class, 'a')
  if(any(!classy %in% c('list', 'recode_instruction'))){
    stop('At least one item passed through ... is not a list or a recode_instruction object')
  }
  
  #Unlist 1 level if necessary
  if(any(classy %in% 'recode_instruction')){
    dots = append(dots[classy == 'recode_instruction'], unlist(dots[classy == 'list'], recursive = F))
  }else{
    dots = unlist(dots, recursive = F)
  }
  
  classy = vapply(dots, class, 'a')
  
  if(!all(classy %in% c('recode_instruction'))){
    stop('At least one item passed through ... cannot be converted into a recode_instruction object')
  }
  
  #check text case
  if((length(unique(names(data))) != length(unique(tolower(names(data))))) & ignore_case){
    stop('Variable names in data are not unique after setting everything to lower case. Fix or run again with ignore_case = FALSE')
  }
  
  if(ignore_case){
    #dots
    for(i in seq(dots)){
      dots[[i]][['old_var']] = tolower(dots[[i]][['old_var']])
      dots[[i]][['new_var']] = tolower(dots[[i]][['new_var']])
    }
    
    setnames(data, tolower(names(data)))
  }
  
  #remove recodes that don't match the year
  btween  = vapply(dots, function(x) between(year, x$year_bound[1], x$year_bound[2]), FALSE)
  
  #list the variables about to be changes (e.g. old_var) that are not in names(data)
  if(hypothetical){
    oldvars = vapply(dots[btween], function(x) x$old_var, 'a')
    mis = setdiff(oldvars, names(data))
    return(mis)
  }
  
  
  for(dot in dots[btween]){
    #print(dot$old_var)
    val = tryCatch(apply_recode(data, year, dot, jump_scope = F, return_vector = T),
                   error = function(x){
                     message(dot$old_var)
                     stop(x)
                   },
                   warning = function(x){
                     message(paste(paste0(dot$old_var, ' -> ', dot$new_var), '|', x))
                   })
    
    set(data, NULL, dot$new_var, val)
  }
  
  #return the results
  return(data)
  
}