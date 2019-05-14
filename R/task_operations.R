TaskOperations <- R6::R6Class("TaskOperations",
                              public = list(
                                path = "/jobs/%s/tasks",
                                url = NULL,
                                authentication = NULL,
                                client = NULL,
                                apiVersion = NULL,
                                initialize = function(client = NA, url = NA, authentication = NA, apiVersion) {
                                  self$url <- url
                                  self$authentication <- authentication
                                  self$client <- client
                                  self$apiVersion <- apiVersion
                                },
    add = function(jobId, taskId = "default", content = "parsed", ...){
      args <- list(...)
      environmentSettings <- args$environmentSettings
      resourceFiles <- args$resourceFiles
      commandLine <- args$commandLine
      dependsOn <- args$dependsOn
      outputFiles <- args$outputFiles
      exitConditions <- args$exitConditions
      containerSettings <- args$containerSettings

      if (is.null(commandLine)) {
        stop("Task requires a command line.")
      }

      body <- list(id = taskId,
                   commandLine = commandLine,
                   userIdentity = list(
                     autoUser = list(
                       scope = "pool",
                       elevationLevel = "admin"
                     )
                   ),
                   resourceFiles = resourceFiles,
                   environmentSettings = environmentSettings,
                   dependsOn = dependsOn,
                   outputFiles = outputFiles,
                   constraints = list(
                     maxTaskRetryCount = 3
                   ),
                   exitConditions = exitConditions,
                   containerSettings = containerSettings)

      body <- Filter(length, body)

      size <- nchar(rjson::toJSON(body, method = "C"))

      headers <- c()
      headers['Content-Length'] <- size
      headers['Content-Type'] <- "application/json;odata=minimalmetadata"

      request <- AzureRequestV2$new(
        method = "POST",
        path = sprintf(self$path, jobId),
        query = list("api-version" = self$apiVersion),
        headers = headers,
        body = body
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    addCollection = function(jobId, tasks, content = "parsed", ...){
      args <- list(...)
      body <- tasks
      body <- Filter(length, body)

      size <- nchar(rjson::toJSON(body, method = "C"))

      headers <- c()
      headers['Content-Length'] <- size
      headers['Content-Type'] <- "application/json;odata=minimalmetadata"

      request <- AzureRequestV2$new(
        method = "POST",
        path = paste0("/jobs/", jobId, "/addtaskcollection"),
        query = list("api-version" = apiVersion),
        headers = headers,
        body = body
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    get = function(jobId, taskId, content = "parsed", ...){
      query <- list("api-version" = apiVersion)

      request <- AzureRequestV2$new(
        method = "GET",
        path = paste0(sprintf(self$path, jobId), "/", taskId),
        query = query
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    list = function(jobId, content = "parsed", ...){
      args <- list(...)
      skipToken <- args$skipToken
      filter <- args$filter
      select <- args$select

      query <- list("api-version" = apiVersion)

      if (!is.null(skipToken)) {
        query <- list("api-version" = apiVersion,
                      "$skiptoken" = skipToken)
      }

      if (!is.null(filter)) {
        query[["$filter"]] <- filter
      }

      if (!is.null(select)) {
        query[["$select"]] <- select
      }

      request <- AzureRequestV2$new(
        method = "GET",
        path = paste0(sprintf(self$path, jobId)),
        query = query
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    }
  )
)
