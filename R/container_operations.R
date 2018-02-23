ContainerOperations <- R6::R6Class("ContainerOperations",
    public = list(
      path = "/",
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
      listContainers = function(prefix = "", content = "parsed", ...) {
        query <- list('comp' = "list", 'prefix' = prefix)

        request <- AzureRequestV2$new(method = "GET",
                                    path = paste0(self$path),
                                    query = query)

        response <- self$client$execute(request)
        self$client$extractAzureResponse(response, content)
      },
      deleteContainer = function(containerName, content = "parsed", ...) {
        query <- list('restype' = "container")

        request <- AzureRequestV2$new(
          method = "DELETE",
          path = paste0(self$path, '/', containerName),
          query = query
        )

        response <- self$client$execute(request)
        self$client$extractAzureResponse(response, content)
      },
      createContainer =
        function(containerName, content = "parsed", ...) {
          query <- list('restype' = "container")

          request <- AzureRequestV2$new(
            method = "PUT",
            path = paste0(self$path, '/', containerName),
            query = query
          )

          response <- self$client$execute(request)
          self$client$extractAzureResponse(response, content)
        }
    )
)
