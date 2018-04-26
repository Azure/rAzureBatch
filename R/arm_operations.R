getBatchAccount <- function(batchAccount,
                            resourceGroup,
                            subscriptionId,
                            servicePrincipal,
                            verbose = FALSE){
  url <- paste0("https://management.azure.com/subscriptions/", subscriptionId,
                "/resourceGroups/", resourceGroup,
                "/providers/Microsoft.Batch/batchAccounts/", batchAccount,
                "?api-version=2017-05-01")

  headers <- httr::add_headers(
    .headers = c(Host = "management.azure.com",
                 Authorization = servicePrincipal$checkAccessToken(),
                 `Content-type` = "application/json"))

  verboseObj <- NULL
  if (verbose) {
    verboseObj <- httr::verbose()
  }

  response <- httr::GET(
    url = url,
    headers,
    verboseObj,
    encode = "json"
  )

  httr::stop_for_status(response)
  httr::content(response)
}

getStorageKeys <- function(storageAccount,
                           resourceGroup,
                           subscriptionId,
                           servicePrincipal,
                           verbose = FALSE){
  url <- paste0("https://management.azure.com/subscriptions/", subscriptionId,
                "/resourceGroups/", resourceGroup,
                "/providers/Microsoft.Storage/storageAccounts/", storageAccount,
                "/listkeys?api-version=2016-01-01")

  headers <- httr::add_headers(
    .headers = c(Host = "management.azure.com",
                 Authorization = servicePrincipal$checkAccessToken(),
                 `Content-type` = "application/json"))

  verboseObj <- NULL
  if (verbose) {
    verboseObj <- httr::verbose()
  }

  response <- httr::POST(
    url = url,
    headers,
    verboseObj,
    encode = "json"
  )

  httr::stop_for_status(response)
  httr::content(response)
}
