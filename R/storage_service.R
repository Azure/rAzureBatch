storageVersion <- "2015-12-11"

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
  requestdate <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
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

callStorage <- function(request, credentials, body=NULL){
  stringToSign <- createSignature(request$method, request$headers)

  requestdate <- format(Sys.time(), "%a, %d %b %Y %H:%M:%S %Z", tz="GMT")
  url <- sprintf("https://%s.blob.core.windows.net%s", credentials$name, request$path)

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
  requestHeaders<-add_headers(.headers = headers)

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

  stop_for_status(response)
}

listBlobs <- function(containerName, sasToken = list()){
  storageCredentials <- getStorageCredentials()

  if(length(sasToken) == 0){
    sasToken <- constructSas("2016-11-30", "rl", "c", containerName, storageCredentials$key)
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

createContainer <- function(containerName){
  storageCredentials <- getStorageCredentials()

  query <- list('restype' = "container")

  request <- AzureRequest$new(
    method = "PUT",
    path = paste0("/", containerName),
    query = query)

  callStorage(request, storageCredentials)
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

uploadData <- function(containerName, fileDirectory, sasToken = list(), ...){
  storageCredentials <- getStorageCredentials()

  if(length(sasToken) == 0){
    sasToken <- constructSas("2016-11-30", "rwcl", "c", containerName, storageCredentials$key)
  }

  #fix this later
  filePath <- strsplit(fileDirectory, "/")
  filePath <- unlist(filePath)
  lastWord <- filePath[length(filePath)]

  fileSize <- file.size(fileDirectory)

  # file size is less than 64 mb
  if(fileSize < (1024 * 1024 * 64)){
    endFile <- upload_file(fileDirectory)

    headers <- c()
    headers['Content-Length'] <- fileSize
    headers['Content-Type'] <- endFile$type
    headers['x-ms-blob-type'] <- 'BlockBlob'

    request <- AzureRequest$new(
      method = "PUT",
      path = paste0("/", containerName, "/", lastWord),
      headers = headers)

    callStorageSas(request, storageCredentials$name, body=upload_file(fileDirectory), sas_params = sasToken)
  }
  else{
    uploadChunk(containerName, fileDirectory, sas_params = sasToken)
  }
}

uploadChunk <- function(containerName, fileDirectory, sasToken = list()){
  storageCredentials <- getStorageCredentials()

  filePath <- strsplit(fileDirectory, "/")
  filePath <- unlist(filePath)
  blobName <- filePath[length(filePath)]

  fileSize <- file.size(fileDirectory)

  nchunks = ceiling(fileSize / 4000000)
  frame <- read.csv(fileDirectory)
  chunks <- split(frame, 1:nchunks)
  blockList <- c()

  i <- 1
  while(i <= nchunks){
    blockId <- i
    currLength <- 8 - nchar(blockId)

    for(j in 1:currLength)
    {
      blockId <- paste0(blockId, 0)
    }

    blockId <- RCurl::base64Encode(enc2utf8(blockId))

    file <- paste0(i, "_", blobName)
    write.csv(chunks[i], file=file)

    print(paste0("BlockId: ", blockId, "File: ", file))
    endFile <- upload_file(file)

    headers <- c()
    headers['Content-Length'] <- file.size(file)
    headers['Content-Type'] <- 'text/xml'
    headers['x-ms-blob-type'] <- 'BlockBlob'

    request <- AzureRequest$new(
      method = "PUT",
      path = paste0("/", containerName, "/", blobName),
      headers = headers,
      query=list('comp'="block",
                 'blockid'=blockId))

    if(length(sasToken) == 0){
      callStorage(request, storageCredentials, body = endFile, sas_params = sasToken)
    }
    else{
      callStorageSas(request, storageCredentials, body = endFile, sas_params = sasToken)
    }

    blockList <- c(blockList, blockId)
    i <- i + 1

    if (file.exists(file)) file.remove(file)
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
  print(body)

  request <- AzureRequest$new(
    method = "PUT",
    path = paste0("/", containerName, "/", fileName),
    headers = headers,
    query=list('comp'="blocklist")
  )

  callStorage(request, storageCredentials, body = body)
}

getBlobList <- function(containerName, fileName){
  storageCredentials <- getStorageCredentials()

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName, "/", fileName),
    headers = headers,
    query=list('comp'="blocklist",
               'blocklisttype'="all")
  )

  callStorage(request, storageCredentials)
}

uploadDirectory <- function(storageCredentials, containerName, fileDirectory){
  files = list.files(fileDirectory, full.names = TRUE)

  for(i in 1:length(files))
  {
    uploadData(storageCredentials, containerName, files[i])
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
      sasToken <- constructSas("2016-11-30", "rwcl", "c", containerName, storageCredentials$key)
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

splitChunks <- function(df, numChunks, pattern = "file", tmpdir = tempdir()){
  chunks <- split(df, 1:numChunks)

  i <- 1
  while(i <= numChunks){
    file <- tempfile(pattern = paste0(pattern, "-", i), fileext = ".csv")
    write.csv(chunks[i], file = file)
    i <- i + 1
  }
}

getChunks <- function(tmpdir = tempdir()){
  files <- list.files(tempdir(), pattern = ".csv", full.names = TRUE)
  results <- lapply(files, function(x){read.csv(x, check.names = FALSE)})
  return(results)
}
