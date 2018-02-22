FileOperations <- R6::R6Class(
    "FileOperations",
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
      getNodeFile =
        function(poolId,
                 nodeId,
                 filePath,
                 content = "parsed",
                 downloadPath = NULL,
                 overwrite = FALSE,
                 ...) {
          args <- list(...)

          verb <- "GET"
          if (!is.null(args$verb) && args$verb == "HEAD") {
            verb <- args$verb
          }

          if (!is.null(downloadPath)) {
            write <- httr::write_disk(downloadPath, overwrite)
          }
          else {
            write <- httr::write_memory()
          }

          progress <- NULL
          if (!is.null(args$progress)) {
            progress <- args$progress
          }

          request <- AzureRequest$new(
            method = verb,
            path = paste0("/pools/", poolId, "/nodes/", nodeId, "/files/", filePath),
            query = list("api-version" = apiVersion)
          )


          response <- self$client$execute(request)
          self$client$extractAzureResponse(response, content)
        },
      getTaskFile =
        function(jobId,
                 taskId,
                 filePath,
                 content = "parsed",
                 downloadPath = NULL,
                 overwrite = FALSE,
                 ...) {
          args <- list(...)

          verb <- "GET"
          if (!is.null(args$verb) && args$verb == "HEAD") {
            verb <- args$verb
          }

          progress <- NULL
          if (!is.null(args$progress)) {
            progress <- args$progress
          }

          if (!is.null(downloadPath)) {
            write <- httr::write_disk(downloadPath, overwrite)
          }
          else {
            write <- httr::write_memory()
          }

          request <- AzureRequest$new(
            method = verb,
            path = paste0("/jobs/", jobId, "/tasks/", taskId, "/files/", filePath),
            query = list("api-version" = apiVersion)
          )

          response <- self$client$execute(request)
          self$client$extractAzureResponse(response, content)
        }
    )
  )
