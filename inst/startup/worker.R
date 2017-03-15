#!/usr/bin/Rscript
args = commandArgs(trailingOnly=TRUE)

# test if there is at least one argument: if not, return an error
if (length(args)==0) {
  stop("At least one argument must be supplied (input file).n", call.=FALSE)
} else if (length(args)==1) {
  # default output file
  args[2] = "out.txt"
}

AZ_BATCH_TASK_WORKING_DIR <- args[1]
AZ_BATCH_TASK_ENV <- args[2]

setwd(AZ_BATCH_TASK_WORKING_DIR)

wd <- paste0(AZ_BATCH_TASK_ENV)
azbatchenv <- readRDS(wd)

for(package in azbatchenv$packages){
  library(package, character.only = TRUE)
}
parent.env(azbatchenv$exportenv) <- globalenv()

if(!is.null(azbatchenv$inputs)){
  options("az_config" = list(container = azbatchenv$inputs))
}

result <- lapply(azbatchenv$argsList, function(args){
  tryCatch({
    lapply(names(args), function(n)
      assign(n, args[[n]], pos=azbatchenv$exportenv))

    eval(azbatchenv$expr, azbatchenv$exportenv)
  }, error = function(e) {
    print(e)
  })
})

file_result_name <- strsplit(AZ_BATCH_TASK_ENV, "[.]")[[1]][1]
saveRDS(result, file = paste0(AZ_BATCH_TASK_WORKING_DIR, "/", file_result_name, "-result.rds"))

quit(save = "yes", status = 0, runLast = FALSE)
