AZ_BATCH_TASK_WORKING_DIR <- args[1]
AZ_BATCH_TASK_ENV <- args[2]

SPLIT_FUNC <- args[2]
FILE_INPUT <- args[3]
FILE_EXTENSION <- args[4]
LEVEL <- args[4]

file_extension <- strsplit(FILE_INPUT, "[.]")[[1]];

df <- data.frame(Date=as.Date(character()),
                 File=character(),
                 User=character(),
                 stringsAsFactors=FALSE)

if(file_extension[-1] == "txt"){
  df <- read.table(paste0(AZ_BATCH_JOB_PREP_WORKING_DIR, '/', FILE_INPUT))
}

if(file_extension[-1] == "csv"){
  df <- read.csv(paste0(AZ_BATCH_JOB_PREP_WORKING_DIR, '/', FILE_INPUT))
}

eval(azbatchenv$expr, azbatchenv$exportenv)

wd <- paste0(AZ_BATCH_JOB_PREP_WORKING_DIR, "/environment.RData")
e <- readRDS(wd)

chunks <- e[[SPLIT_FUNC]](df)

for(i in 1:length(chunks)){
  file_name <- paste0(file_extension[1], "_", LEVEL, '_', i, '.', file_extension[-1])

  if(file_extension[-1] == "txt"){
    write.table(chunks[[i]], file=file_name)
  }
  else{
    write.csv(chunks[[i]], file=file_name)
  }

  cat(file_name)
  cat(';')
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

results <- list()
for(i in 1:N){
  task_result <- paste0(AZ_BATCH_TASK_WORKING_DIR, "/result/", JOB_ID, "-task", i, "-result.rds")
  results[[i]] <- readRDS(task_result)
}

file_result_name <- strsplit(AZ_BATCH_TASK_ENV, "[.]")[[1]][1]
saveRDS(results, file = paste0(AZ_BATCH_TASK_WORKING_DIR, "/", file_result_name, "-result.rds"))

quit(save = "yes", status = 0, runLast = FALSE)

