#!/usr/bin/Rscript
args = commandArgs(trailingOnly=TRUE)

# test if there is at least one argument: if not, return an error
if (length(args) == 0) {
  stop("At least one argument must be supplied (input file).n", call.=FALSE)
} else if (length(args)==1) {
  # default output file
  args[2] = "out.txt"
}

REPO <- args[1]

if(mode != "GITHUB"){
  for(i in 2:length(args)){
    install.packages(args[i], repos = REPO, dependencies = TRUE)
  }
}

if(mode == "GITHUB"){
  if (!require("devtools")) install.packages("devtools", dependencies = TRUE)

  library(devtools)

  for(i in 2:length(args)){
    install_github(args[i])
  }
}

quit(save = "yes", status = 0, runLast = FALSE)
