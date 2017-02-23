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
N <- args[3]
JOB_ID <- args[4]

wd <- paste0(AZ_BATCH_TASK_WORKING_DIR, "/", AZ_BATCH_TASK_ENV)
azbatchenv <- readRDS(wd)

for(package in azbatchenv$packages){
  library(package, character.only = TRUE)
}
parent.env(azbatchenv$exportenv) <- globalenv()

results <- vector("list", N)
for(i in 1:N){
  task_result <- paste0(AZ_BATCH_TASK_WORKING_DIR, "/result/", JOB_ID, "-task", i, "-result.rds")
  results[i] <- readRDS(task_result)
}

file_result_name <- strsplit(AZ_BATCH_TASK_ENV, "[.]")[[1]][1]
saveRDS(results, file = paste0(AZ_BATCH_TASK_WORKING_DIR, "/", file_result_name, "-result.rds"))

quit(save = "yes", status = 0, runLast = FALSE)
