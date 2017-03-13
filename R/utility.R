.getInstallationCommand <- function(packages){
  installation <- ""

  for(package in packages){
    installation <- paste0(installation,
                           sprintf(" R -e \'install.packages(\\\"%s\\\", dependencies=TRUE)\'", package),
                           ";")
  }

  installation <- substr(installation, 1, nchar(installation) - 1)
}

.getGithubInstallationCommand <- function(packages){
  installation <- ""
  installation <- paste0(installation,
                         sprintf(" R -e \'install.packages(\"%s\", dependencies=TRUE)\'", "devtools"),
                         ";")

  if(length(packages) != 0){
      for(package in packages){
        installation <- paste0(installation,
                              sprintf(" R -e \'library(%s); install_github(\"%s\")\'", "devtools", package),
                              ";")
      }
  }

  installation <- substr(installation, 1, nchar(installation) - 1)
}

.linuxWrapCommands <- function(commands = c()){
  commandLine <- sprintf("/bin/bash -c \"set -e; set -o pipefail; %s wait\"", paste0(paste(commands, sep = " ", collapse = "; "),"; "))
}

getAutoscaleFormula <- function(formulaName, min, max){
  formulas <- names(AUTOSCALE_FORMULA)

  if(formulaName == formulas[1]){
    return(sprintf(AUTOSCALE_WEEKEND_FORMULA, min, max))
  }
  else if(formulaName == formulas[2]){
    return(sprintf(AUTOSCALE_WORKDAY_FORMULA, min, max))
  }
  else if(formulaName == formulas[3]){
    return(AUTOSCALE_MAX_CPU_FORMULA)
  }
  else if(formulaName == formulas[4]){
    return(sprintf(AUTOSCALE_QUEUE_FORMULA, min, max))
  }
  else{

  }
}
