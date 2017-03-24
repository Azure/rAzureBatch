storageVersion <- "2016-05-31"

getStorageCredentials <- function(configName = "az_config.json", ...){
  config <- getOption("az_config")

  if(!is.null(config) && !is.null(config$storageAccount)){
    storageAccount <- config$storageAccount
    credentials <- StorageCredentials$new(name=storageAccount$name, key=storageAccount$key)
  }
  else{
    config <- rjson::fromJSON(file = paste0(getwd(), "/", configName))
    credentials <- StorageCredentials$new(name=config$storageAccount$name, key=config$storageAccount$key)
  }

  credentials
}

StorageCredentials <- setRefClass("StorageCredentials",
                                  fields = list(name = "character", key = "character"),
                                  methods = list(
                                    signString = function(x){
                                      undecodedKey <- RCurl::base64Decode(key, mode="raw")
                                      newString<-RCurl::base64(
                                        digest::hmac(key=undecodedKey,
                                                     object=enc2utf8(x),
                                                     algo= "sha256", raw=TRUE)
                                      )
                                    }
                                  ))

callStorageSas <- function(request, credentials, body=NULL, sas_params){
  requestdate <- http_date(Sys.time())

  url <- sprintf("https://%s.blob.core.windows.net%s", credentials, request$path)

  headers <- request$headers
  headers['x-ms-date'] <- requestdate
  headers['x-ms-version'] <- storageVersion

  request$query <- append(request$query, sas_params)

  requestHeaders<-add_headers(.headers = headers)

  response <- ""
  config <- getOption("az_config")
  if(!is.null(config) && !is.null(config$settings)){
    verbose <- config$settings$verbose
  }
  else{
    verbose <- getOption("verbose")
  }

  if(verbose){
    print(headers)
    print(paste0("URL: ", url))

    response <- VERB(request$method, url, query = request$query, config = requestHeaders, body=body, verbose())
    cat(content(response, "text"), "\n")
  }
  else{
    response <- VERB(request$method, url, query = request$query, config = requestHeaders, body=body)
  }

  stop_for_status(response)
}

callStorage <- function(request, credentials, body=NULL, ...){
  args <- list(...)
  contentType = args$contentType

  stringToSign <- createSignature(request$method, request$headers)

  url <- sprintf("https://%s.blob.core.windows.net%s", credentials$name, request$path)
  requestdate <- http_date(Sys.time())

  headers <- request$headers
  headers['x-ms-date'] <- requestdate
  headers['x-ms-version'] <- storageVersion

  canonicalizedHeaders <- ""
  for(name in sort(names(headers))){
    if(grepl('x-ms', name)){
      canonicalizedHeaders <- paste0(canonicalizedHeaders, name,":", headers[name], "\n")
    }
  }

  canonicalizedResource <- paste0("/", credentials$name, request$path, "\n")
  for(name in sort(names(request$query))){
    canonicalizedResource <- paste0(canonicalizedResource, name,":", request$query[[name]], "\n")
  }

  canonicalizedResource <- substr(canonicalizedResource, 1, nchar(canonicalizedResource) - 1)

  stringToSign <- paste0(stringToSign, canonicalizedHeaders)
  stringToSign <- paste0(stringToSign, canonicalizedResource)

  # sign the request
  authString<-paste0("SharedKey ", credentials$name, ":", credentials$signString(stringToSign))

  headers['Authorization'] <- authString
  requestHeaders<-add_headers(.headers = headers, "User-Agent"="rAzureBatch/0.2.0")

  config <- getOption("az_config")
  if(!is.null(config) && !is.null(config$settings)){
    verbose <- config$settings$verbose
  }
  else{
    verbose <- getOption("verbose")
  }

  response <- ""
  if(verbose){
    print(paste0("Resource String: ", canonicalizedResource))
    print(stringToSign)
    print(headers)
    print(paste0("URL: ", url))

    response <- VERB(request$method, url, query = request$query, config = requestHeaders, body=body, verbose())
    cat(content(response, "text"), "\n")
  }
  else{
    response <- VERB(request$method, url, query = request$query, config = requestHeaders, body=body)
  }

  if(!is.null(contentType) && contentType){
    content(response, as = "text")
  }
  else{
    content(response)
  }
}

listBlobs <- function(containerName, sasToken = list()){
  storageCredentials <- getStorageCredentials()

  if(length(sasToken) == 0){
    sasToken <- constructSas("rl", "c", containerName, storageCredentials$key)
  }

  query <- list('restype' = "container", 'comp' = "list")

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName),
    query = query)

  if(length(sasToken) == 0){
    callStorage(request, storageCredentials)
  }
  else{
    callStorageSas(request, storageCredentials, sas_params=sasToken)
  }
}

listContainers <- function(){
  storageCredentials <- getStorageCredentials()

  query <- list('comp' = "list")

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/"),
    query = query)

  callStorage(request, storageCredentials)
}

deleteContainer <- function(containerName){
  storageCredentials <- getStorageCredentials()

  query <- list('restype' = "container")

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/", containerName),
    query = query)

  callStorage(request, storageCredentials)
}

createContainer <- function(containerName, ...){
  args <- list(...)

  raw <- FALSE
  if(!is.null(args$raw)){
    raw <- args$raw
  }

  storageCredentials <- getStorageCredentials()

  query <- list('restype' = "container")

  request <- AzureRequest$new(
    method = "PUT",
    path = paste0("/", containerName),
    query = query)

  callStorage(request, storageCredentials, contentType = raw)
}

deleteBlob <- function(containerName, blobName, sasToken = list(), ...){
  storageCredentials <- getStorageCredentials()

  if(length(sasToken) == 0){
    sasToken <- constructSas("2016-11-30", "d", "c", containerName, storageCredentials$key)
  }

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/", containerName, "/", blobName))

  callStorageSas(request, storageCredentials$name, sas_params = sasToken)
}

uploadBlob <- function(containerName, fileDirectory, sasToken = list(), ...){
  args <- list(...)

  if(!is.null(args$accountName)){
    name <- args$accountName
  }
  else{
    storageCredentials <- getStorageCredentials()
    name <- storageCredentials$name
  }

  if(length(sasToken) == 0){
    sasToken <- constructSas("rwcl", "c", containerName, storageCredentials$key)
  }

  fileSize <- file.size(fileDirectory)

  # file size is less than 64 mb
  if(fileSize < (1024 * 1024 * 64)){
    endFile <- upload_file(fileDirectory)

    headers <- c()
    headers['Content-Length'] <- fileSize
    headers['Content-Type'] <- endFile$type
    headers['x-ms-blob-type'] <- 'BlockBlob'

    blobName <- basename(fileDirectory)
    if(!is.null(args$remoteName)){
      blobName <- args$remoteName
    }

    request <- AzureRequest$new(
      method = "PUT",
      path = paste0("/", containerName, "/", blobName),
      headers = headers)

    callStorageSas(request, name, body=upload_file(fileDirectory), sas_params = sasToken)
  }
  else{
    uploadChunk(containerName, fileDirectory, sas_params = sasToken, ...)
  }
}

uploadChunk <- function(containerName, fileDirectory, sasToken = list(), ...){
  args <- list(...)
  storageCredentials <- getStorageCredentials()

  finfo <- file.info(fileDirectory)
  to.read <- file(fileDirectory, "rb")

  defaultSize <- 50000000
  numOfChunks <- ceiling(finfo$size / defaultSize)
  blockList <- c()

  filePath <- strsplit(fileDirectory, "/")
  filePath <- unlist(filePath)
  blobName <- filePath[length(filePath)]

  blobName <- basename(fileDirectory)
  if(!is.null(args$remoteName)){
    blobName <- args$remoteName
  }

  if(!is.null(args$parallelThreads)){
    parallelThreads <- args$parallelThreads
    doParallel::registerDoParallel(cores = parallelThreads)

    currentChunk <- 0
    while(currentChunk < numOfChunks){
      count <- 1
      if(currentChunk + parallelThreads >= numOfChunks){
        count <- numOfChunks - currentChunk
      }
      else{
        count <- parallelThreads
      }

      chunk <- readBin(to.read, raw(), n = defaultSize * count)

      results <- foreach(i = 0:(count - 1)) %dopar% {
        if(i == count - 1){
          data <- chunk[((i*defaultSize) + 1) :  length(chunk)]
        }
        else{
          data <- chunk[((i*defaultSize) + 1) :  ((i*defaultSize) + defaultSize)]
        }

        blockId <- currentChunk + i
        currLength <- 8 - nchar(blockId)

        for(j in 1:currLength)
        {
          blockId <- paste0(blockId, 0)
        }

        blockId <- RCurl::base64Encode(enc2utf8(blockId))

        headers <- c()
        headers['Content-Length'] <- as.character(length(data))
        headers['x-ms-blob-type'] <- 'BlockBlob'

        request <- AzureRequest$new(
          method = "PUT",
          path = paste0("/", containerName, "/", blobName),
          headers = headers,
          query=list('comp'="block",
                     'blockid'=blockId))

        if(length(sasToken) == 0){
          callStorage(request, storageCredentials, body = data, sas_params = sasToken)
        }
        else{
          callStorageSas(request, storageCredentials, body = data, sas_params = sasToken)
        }

        return(blockId)
      }

      for(j in 1:length(results)){
        blockList <- c(blockList, results[[j]])
      }

      currentChunk <- currentChunk + count
    }
  }
  else{

  }

  str <- ""
  for(i in 1:length(blockList))
  {
    str <- paste0(str, "<Latest>", blockList[i], "</Latest>")
  }

  body <- paste0("<BlockList>", str, "</BlockList>")
  body <- paste0("<?xml version='1.0' encoding='utf-8'?>", body)

  putBlockList(containerName, blobName, body)
}

putBlockList <- function(containerName, fileName, body){
  storageCredentials <- getStorageCredentials()

  headers <- c()
  headers['Content-Length'] <- nchar(body)
  headers['Content-Type'] <- 'text/xml'

  request <- AzureRequest$new(
    method = "PUT",
    path = paste0("/", containerName, "/", fileName),
    headers = headers,
    query=list('comp'="blocklist")
  )

  callStorage(request, storageCredentials, body = body)
}

getBlockList <- function(containerName, fileName){
  storageCredentials <- getStorageCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName, "/", fileName),
    query=list('comp'="blocklist",
               'blocklisttype'="all")
  )

  callStorage(request, storageCredentials)
}

uploadDirectory <- function(containerName, fileDirectory, ...){
  args <- list(...)
  if(is.null(args$storageCredentials)){
    storageCredentials <- getStorageCredentials()
  }
  else{
    storageCredentials <- args$storageCredentials
  }

  files = list.files(fileDirectory, full.names = TRUE, recursive = TRUE)
  fileName = list.files(fileDirectory, recursive = TRUE)

  for(i in 1:length(files))
  {
    uploadBlob(containerName, files[i], remoteName = fileName[i], ...)
  }
}

downloadBlob <- function(containerName, fileName, sasToken = list()){
  storageName <- ""
  if(!is.null(getOption("az_config")) && !is.null(getOption("az_config")$container)){
    storageName <- getOption("az_config")$container$name
    sasToken <- getOption("az_config")$container$sasToken
  }
  else{
    storageCredentials <- getStorageCredentials()
    storageName <- storageCredentials$name

    if(length(sasToken) == 0){
      sasToken <- constructSas("rwcl", "c", containerName, storageCredentials$key)
    }
  }

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName, "/", fileName))

  r <- callStorageSas(request, storageName, sas_params = sasToken)
  bin <- content(r, "raw")
  writeBin(bin, "temp.rds")
  readRDS("temp.rds")
}
