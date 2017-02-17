AUTOSCALE_WORKDAY_FORMULA <- paste0(
  "$curTime = time();",
  "$workHours = $curTime.hour >= 8 && $curTime.hour < 18;",
  "$isWeekday = $curTime.weekday >= 1 && $curTime.weekday <= 5;",
  "$isWorkingWeekdayHour = $workHours && $isWeekday;",
  "$TargetDedicated = $isWorkingWeekdayHour ? %s:%s;")

AUTOSCALE_WEEKEND_FORMULA <- paste0(
  "$isWeekend = $curTime.weekday >= 6 && $curTime.weekday <= 7;",
  "$TargetDedicated = $isWeekend ? %s:%s;")

AUTOSCALE_MAX_CPU_FORMULA <- "$totalNodes =
(avg($CPUPercent.GetSample(TimeInterval_Minute * 60)) < 0.2) ?
($CurrentDedicated * 0.9) : $totalNodes;"

AUTOSCALE_QUEUE_FORMULA <- paste0(
  "$samples = $ActiveTasks.GetSamplePercent(TimeInterval_Minute * 15);",
  "$tasks = $samples < 70 ? max(0,$ActiveTasks.GetSample(1)) : max( $ActiveTasks.GetSample(1), avg($ActiveTasks.GetSample(TimeInterval_Minute * 15)));",
  "$targetVMs = $tasks > 0? $tasks:max(0, $TargetDedicated/2);",
  "$TargetDedicated = max(%s, min($targetVMs, %s));",
  "$NodeDeallocationOption = taskcompletion;"
)

AUTOSCALE_FORMULA = list("WEEKEND" = AUTOSCALE_WEEKEND_FORMULA,
                         "WORKDAY" = AUTOSCALE_WORKDAY_FORMULA,
                         "MAX_CPU" = AUTOSCALE_MAX_CPU_FORMULA,
                         "QUEUE" = AUTOSCALE_QUEUE_FORMULA)

generatePoolConfig <- function(
  batchName,
  batchKey,
  batchUrl,
  storageName = "",
  storageKey = "",
  data = "",
  packages = "",
  ...){
    if(!file.exists(paste0(getwd(), "/", "az_config.json"))){
      config <- list(batchName = batchName,
                     batchKey = batchKey,
                     batchUrl = batchUrl,
                     storageName = storageName,
                     storageKey = storageKey,
                     data = data,

                     vmSize = "STANDARD_A1",
                     numberOfNodes = 3,
                     packages = packages,
                     rootDirectory = getwd(),
                     verbose = FALSE)

      configJson <- jsonlite::toJSON(config, auto_unbox = TRUE, pretty = TRUE)
      write(configJson, file=paste0(config$rootDirectory, "/", "az_config.json"))

      print(sprintf("A config file has been generated %s", paste0(config$rootDirectory, "/", "az_config.json")))
    }
}

generatePoolConfig <- function(fileName, ...){
  args <- list(...)

  batchAccount <- ifelse(is.null(args$batchAccount), "batch_account_name", args$batchAccount)
  batchKey <- ifelse(is.null(args$batchKey), "batch_account_key", args$batchKey)
  batchUrl <- ifelse(is.null(args$batchUrl), "batch_account_url", args$batchUrl)

  storageName <- ifelse(is.null(args$storageName), "storage_account_name", args$storageName)
  storageKey <- ifelse(is.null(args$storageKey), "storage_account_key", args$storageKey)

  packages <- ifelse(is.null(args$packages), list(), args$packages)

  if(!file.exists(paste0(getwd(), "/", fileName))){
    config <- list(
      batchAccount = list(
        name = batchAccount,
        key = batchKey,
        url = batchUrl,
        pool = list(
          name = "myPoolName",
          vmSize = "STANDARD_A1",
          poolSize = list(
            minNodes = 3,
            maxNodes = 10,
            autoscaleFormula = "QUEUE"
          )
        ),
        rPackages = list(
          cran = list(
            source = "http://cran.us.r-project.org",
            name = c(
              "devtools",
              "httr"
            )
          ),
          github = c(
            "twitter/AnomalyDetection",
            "hadley/httr"
          )
        )
      ),
      storageAccount = list(
        name = storageName,
        key = storageKey
      ),
      settings = list(
        verbose = FALSE
      )
    )

    configJson <- jsonlite::toJSON(config, auto_unbox = TRUE, pretty = TRUE)
    write(configJson, file=paste0(getwd(), "/", fileName))

    print(sprintf("A config file has been generated %s. Please enter your Batch credentials.", paste0(getwd(), "/", fileName)))
  }
}

registerPool <- function(fileName = "az_config.json", fullName = FALSE, waitForPool = TRUE){
  setPoolOption(fileName, fullName)
  config <- getOption("az_config")
  pool <- config$batchAccount$pool

  response <- addPool(
    pool$name,
    pool$vmSize,
    autoscaleFormula = .getFormula(pool$poolSize$autoscaleFormula, pool$poolSize$minNodes, pool$poolSize$maxNodes),
    raw = TRUE,
    packages = config$batchAccount$rPackages$github)

  pool <- getPool(pool$name)

  if(grepl("The specified pool already exists.", response)){
    print("The specified pool already exists. Will use existing pool.")
  }
  else{
    if(waitForPool){
      waitForNodesToComplete(pool$id, 60000, targetDedicated = pool$targetDedicated)
    }
  }

  print("Your pool has been registered.")
  print(sprintf("Node Count: %i", pool$targetDedicated))
  return(getOption("az_config"))
}

destroyPool <- function(pool){
  deletePool(pool$batchAccount$pool$name)
}

setPoolOption <- function(fileName = "az_config.json", fullName = FALSE){
  if(fullName){
    config <- rjson::fromJSON(file=paste0(fileName))
  }
  else{
    config <- rjson::fromJSON(file=paste0(getwd(), "/", fileName))
  }

  options("az_config" = config)
}

getPoolWorkers <- function(poolId, ...){
  args <- list(...)
  raw <- !is.null(args$RAW)

  batchCredentials <- getBatchCredentials()

  nodes <- listPoolNodes(poolId)

  if(length(nodes$value) > 0){
    for(i in 1:length(nodes$value)){
      print(sprintf("Node: %s - %s - %s", nodes$value[[i]]$id, nodes$value[[i]]$state, nodes$value[[i]]$ipAddress))
    }
  }
  else{
    print("There are currently no nodes in the pool.")
  }

  if(raw){
    return(nodes)
  }
}

waitForNodesToComplete <- function(poolId, timeout, ...){
  print("Booting compute nodes. . . Please wait. . . There are currently no nodes in the pool. . .")

  args <- list(...)
  numOfNodes <- args$targetDedicated

  pb <- txtProgressBar(min = 0, max = numOfNodes, style = 3)
  prevCount <- 0
  timeToTimeout <- Sys.time() + timeout

  while(Sys.time() < timeToTimeout){
    nodes <- listPoolNodes(poolId)

    startTaskFailed <- TRUE

    if(!is.null(nodes$value) && length(nodes$value) > 0){
      nodeStates <- lapply(nodes$value, function(x){
          if(x$state == "idle"){
            return(1)
          }
          else if(x$state == "creating"){
            return(0.25)
          }
          else if(x$state == "starting"){
            return(0.50)
          }
          else if(x$state == "waitingforstarttask"){
            return(0.75)
          }
          else if(x$state == "starttaskfailed"){
            startTaskFailed <- FALSE
            return(0)
          }
          else{
            return(0)
          }
        })

      count <- sum(unlist(nodeStates))

      if(count > prevCount){
        setTxtProgressBar(pb, count)
        prevCount <- count
      }

      stopifnot(startTaskFailed)

      if(count == numOfNodes){
        return(0);
      }
    }

    setTxtProgressBar(pb, prevCount)
    Sys.sleep(30)
  }

  deletePool(poolId)
  stop("Timeout expired")
}


