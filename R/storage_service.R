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
                                      RCurl::base64(
                                        digest::hmac(key=undecodedKey,
                                                     object=enc2utf8(x),
                                                     algo= "sha256", raw=TRUE)
                                      )
                                    }
                                  ))

callStorageSas <- function(request, accountName, body = NULL, sas_params, ...){
  args <- list(...)

  requestdate <- httr::http_date(Sys.time())

  url <- sprintf("https://%s.blob.core.windows.net%s", accountName, request$path)

  headers <- request$headers
  headers['x-ms-date'] <- requestdate
  headers['x-ms-version'] <- storageVersion

  request$query <- append(request$query, sas_params)

  requestHeaders <- httr::add_headers(.headers = headers)

  response <- ""
  config <- getOption("az_config")

  verbose <- ifelse(!is.null(config) && !is.null(config$settings),
                    config$settings$verbose,
                    getOption("verbose"))

  verboseMode <- NULL

  if(verbose){
    print(headers)
    print(paste0("URL: ", url))
    #cat(content(response, "text"), "\n")

    verboseMode <- verbose()
  }

  write <- NULL
  if(!is.null(args$write)){
    write <- args$write
  }

  response <- httr::VERB(request$method,
                   url,
                   verboseMode,
                   write,
                   query = request$query,
                   config = requestHeaders,
                   body = body)

  httr::stop_for_status(response)
}

callStorage <- function(request, credentials, body = NULL, ...){
  args <- list(...)
  contentType = args$contentType

  stringToSign <- createSignature(request$method, request$headers)

  url <- sprintf("https://%s.blob.core.windows.net%s", credentials$name, request$path)
  requestdate <- httr::http_date(Sys.time())

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
  if(!is.null(names(request$query))){
    for(name in sort(names(request$query))){
      canonicalizedResource <- paste0(canonicalizedResource, name,":", request$query[[name]], "\n")
    }
  }

  canonicalizedResource <- substr(canonicalizedResource, 1, nchar(canonicalizedResource) - 1)

  stringToSign <- paste0(stringToSign, canonicalizedHeaders)
  stringToSign <- paste0(stringToSign, canonicalizedResource)

  # sign the request
  authString<-paste0("SharedKey ", credentials$name, ":", credentials$signString(stringToSign))

  headers['Authorization'] <- authString
  requestHeaders <- httr::add_headers(.headers = headers, "User-Agent"="rAzureBatch/0.2.0")

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

    response <- httr::VERB(request$method, url, query = request$query, config = requestHeaders, body=body, verbose())
    cat(httr::content(response, "text"), "\n")
  }
  else{
    response <- httr::VERB(request$method, url, query = request$query, config = requestHeaders, body=body)
  }

  if(!is.null(contentType) && contentType){
    httr::content(response, as = "text")
  }
  else{
    httr::content(response)
  }
}

listBlobs <- function(containerName, sasToken = NULL, ...){
  args <- list(...)

  if(!is.null(args$accountName)){
    name <- args$accountName
  }
  else{
    storageCredentials <- getStorageCredentials()
    name <- storageCredentials$name
  }

  query <- list('restype' = "container", 'comp' = "list")

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName),
    query = query)

  if(is.null(sasToken)){
    callStorage(request, storageCredentials)
  }
  else{
    callStorageSas(request, accountName = name, sas_params = sasToken)
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

deleteBlob <- function(containerName, blobName, sasToken = NULL, ...){
  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/", containerName, "/", blobName))

  if(!is.null(sasToken)){
    callStorageSas(request, args$accountName, sas_params = sasToken)
  }
  else {
    storageCredentials <- getStorageCredentials()
    callStorage(request, storageCredentials)
  }
}

uploadBlob <- function(containerName, fileDirectory, sasToken = NULL, parallelThreads = 1, ...){
  args <- list(...)

  if(!is.null(args$accountName)){
    name <- args$accountName
  }
  else{
    storageCredentials <- getStorageCredentials()
    name <- storageCredentials$name
  }

  if(file.exists(fileDirectory)){
    fileSize <- file.size(fileDirectory)
  }
  else if(file.exists(paste0(getwd(), "/", fileDirectory))){
    fileDirectory <- paste0(getwd(), "/", fileDirectory)
    fileSize <- file.size(fileDirectory)
  }
  else{
    stop("The given file does not exist.")
  }

  # file size is less than 64 mb
  if(fileSize < (1024 * 1024 * 64)){
    endFile <- httr::upload_file(fileDirectory)

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

    if(!is.null(sasToken)){
      callStorageSas(request, name, body = endFile, sas_params = sasToken)
    }
    else {
      callStorage(request, storageCredentials, body = endFile)
    }
  }
  else{
    if(!is.null(sasToken)){
      uploadChunk(containerName, fileDirectory, parallelThreads = parallelThreads, sasToken = sasToken, accountName = name)
    }
    else {
      uploadChunk(containerName, fileDirectory, parallelThreads = parallelThreads, ...)
    }
  }
}

uploadChunk <- function(containerName, fileDirectory, sasToken = NULL, ...){
  args <- list(...)
  if(!is.null(args$accountName)){
    name <- args$accountName
  }
  else{
    storageCredentials <- getStorageCredentials()
    name <- storageCredentials$name
  }

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

  pb <- txtProgressBar(min = 0, max = numOfChunks, style = 3)

  `%fun%` <- foreach::`%do%`
  parallelThreads <- 1
  if(!is.null(args$parallelThreads) && args$parallelThreads > 1){
    require(doParallel)
    parallelThreads <- args$parallelThreads
    registerDoParallel(parallelThreads)
    `%fun%` <- foreach::`%dopar%`
  }

  # Initialize the current indices for chunks and blockList
  currentChunk <- 0
  blockList <- ""

  while(currentChunk < numOfChunks){
    count <- 1
    if(currentChunk + parallelThreads >= numOfChunks){
      count <- numOfChunks - currentChunk
    }
    else{
      count <- parallelThreads
    }

    chunk <- readBin(to.read, raw(), n = defaultSize * count)
    accountName <- name

    results <- foreach::foreach(i = 0:(count - 1), .export = c("sasToken", "accountName")) %fun% {
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

      if(is.null(sasToken)){
        storageCredentials <- getStorageCredentials()
        callStorage(request, credentials = storageCredentials, body = data)
      }
      else{
        callStorageSas(request, accountName = accountName, body = data, sas_params = sasToken)
      }

      return(paste0("<Latest>", blockId, "</Latest>"))
    }


    if(!is.null(args$parallelThreads) && args$parallelThreads > 1){
      require(doParallel)
      doParallel::stopImplicitCluster()
      foreach::registerDoSEQ()
    }

    for(j in 1:length(results)){
      blockList <- paste0(blockList, results[[j]])
    }

    currentChunk <- currentChunk + count
    setTxtProgressBar(pb, currentChunk)
  }

  close(to.read)
  httpBodyRequest <- paste0("<BlockList>", blockList, "</BlockList>")
  httpBodyRequest <- paste0("<?xml version='1.0' encoding='utf-8'?>", httpBodyRequest)

  if(is.null(sasToken)){
    putBlockList(containerName, blobName, httpBodyRequest)
  }
  else{
    putBlockList(containerName, blobName, body = httpBodyRequest, sasToken = sasToken, accountName = name)
  }
}

putBlockList <- function(containerName, fileName, body, sasToken = NULL, ...){
  args <- list(...)

  if(is.null(args$accountName)){
    storageCredentials <- getStorageCredentials()
  }

  headers <- c()
  headers['Content-Length'] <- nchar(body)
  headers['Content-Type'] <- 'text/xml'

  request <- AzureRequest$new(
    method = "PUT",
    path = paste0("/", containerName, "/", fileName),
    headers = headers,
    query = list('comp'="blocklist")
  )

  if(!is.null(sasToken)){
    callStorageSas(request, accountName = args$accountName, sas_params = sasToken, body = body)
  }
  else {
    callStorage(request, storageCredentials, body)
  }
}

getBlockList <- function(containerName, fileName, sasToken = NULL, ...){
  if(!is.null(args$accountName)){
    name <- args$accountName
  }
  else{
    storageCredentials <- getStorageCredentials()
    name <- storageCredentials$name
  }

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName, "/", fileName),
    query=list('comp'="blocklist",
               'blocklisttype'="all")
  )

  if(!is.null(sasToken)){
    callStorageSas(request, name, sas_params = sasToken)
  }
  else {
    callStorage(request, storageCredentials)
  }
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

downloadBlob <- function(containerName,
                         blobName,
                         sasToken = NULL,
                         overwrite = FALSE,
                         ...){
  args <- list(...)

  if(!is.null(args$localDest)){
    write <- httr::write_disk(args$localDest, overwrite)
  }
  else {
    write <- httr::write_memory()
  }

  if(is.null(args$accountName)){
    storageCredentials <- getStorageCredentials()
  }

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName, "/", blobName))

  if(!is.null(sasToken)){
    callStorageSas(request, args$accountName, sas_params = sasToken, write = write)
  }
  else {
    callStorage(request, storageCredentials, write = write)
  }
}
