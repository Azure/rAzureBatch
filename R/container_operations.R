listContainers <- function(prefix = "", content = "parsed", ...) {
  query <- list('comp' = "list", 'prefix' = prefix)

  request <- AzureRequest$new(method = "GET",
                              path = paste0("/"),
                              query = query)

  callStorage(request, content, ...)
}

deleteContainer <- function(containerName, content = "parsed", ...) {
  query <- list('restype' = "container")

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/", containerName),
    query = query
  )

  callStorage(request, content, ...)
}

createContainer <-
  function(containerName, content = "parsed", ...) {
    query <- list('restype' = "container")

    request <- AzureRequest$new(
      method = "PUT",
      path = paste0("/", containerName),
      query = query
    )

    callStorage(request, content, ...)
  }
