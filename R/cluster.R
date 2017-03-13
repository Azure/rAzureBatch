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
    "$targetVMs = $tasks > 0? $tasks : max(0, $TargetDedicated/2) + 0.5;",
    "$TargetDedicated = max(%s, min($targetVMs, %s));",
    "$NodeDeallocationOption = taskcompletion;"
  )
  
  AUTOSCALE_FORMULA = list("WEEKEND" = AUTOSCALE_WEEKEND_FORMULA,
                           "WORKDAY" = AUTOSCALE_WORKDAY_FORMULA,
                           "MAX_CPU" = AUTOSCALE_MAX_CPU_FORMULA,
                           "QUEUE" = AUTOSCALE_QUEUE_FORMULA)
  
  waitForNodesToComplete <- function(poolId, timeout, ...){
    print("Booting compute nodes. . . ")
  
    args <- list(...)
  
    if(!args$targetDedicated){
      stop("Pool's current target dedicated not calculated. Please try again.")
    }
  
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
  
  
