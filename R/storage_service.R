storageVersion <- "2016-05-31"

getStorageCredentials <-
  function(configName = "az_config.json", ...) {
    config <- getOption("az_config")

    if (!is.null(config) && !is.null(config$storageAccount)) {
      storageAccount <- config$storageAccount
      credentials <-
        StorageCredentials$new(name = storageAccount$name, key = storageAccount$key)
    }
    else{
      config <- rjson::fromJSON(file = paste0(getwd(), "/", configName))
      credentials <-
        StorageCredentials$new(name = config$storageAccount$name,
                               key = config$storageAccount$key)
    }

    credentials
  }

StorageCredentials <- setRefClass(
  "StorageCredentials",
  fields = list(name = "character", key = "character"),
  methods = list(
    signString = function(x) {
      undecodedKey <- RCurl::base64Decode(key, mode = "raw")
      RCurl::base64(digest::hmac(
        key = undecodedKey,
        object = enc2utf8(x),
        algo = "sha256",
        raw = TRUE
      ))
    }
  )
)

callStorageSas <- function(request, accountName, sasToken, ...) {
  args <- list(...)

  requestdate <- httr::http_date(Sys.time())

  url <-
    sprintf("https://%s.blob.core.windows.net%s",
            accountName,
            request$path)

  headers <- request$headers
  headers['x-ms-date'] <- requestdate
  headers['x-ms-version'] <- storageVersion

  request$query <- append(request$query, sasToken)

  requestHeaders <- httr::add_headers(.headers = headers)

  httpTraffic <- getOption("azureHttpTraffic")
  verbose <- getOption("azureVerbose")

  if (verbose) {
    print(headers)
    print(paste0("URL: ", url))
  }

  if (httpTraffic) {
    httpTraffic <- httr::verbose()
  }

  write <- NULL
  if (!is.null(args$write)) {
    write <- args$write
  }

  response <- httr::VERB(
    request$method,
    url,
    httpTraffic,
    write,
    query = request$query,
    config = requestHeaders,
    body = body
  )

  httr::stop_for_status(response)
}

prepareStorageRequest <- function(request, credentials) {
  requestdate <- httr::http_date(Sys.time())
  request$headers['x-ms-date'] <- requestdate
  request$headers['x-ms-version'] <- storageVersion

  authorizationHeader <-
    signAzureRequest(request, credentials$name, credentials$key, 'x-ms-')

  request$headers['Authorization'] <- authorizationHeader
  request$headers['User-Agent'] <-
    paste0("rAzureBatch/",
           packageVersion("rAzureBatch"))

  request$url <-
    sprintf("https://%s.blob.core.windows.net%s",
            credentials$name,
            request$path)

  request
}

callStorage <- function(request, content, ...) {
  args <- list(...)

  if (!is.null(args$sasToken) && !is.null(args$accountName))  {
    response <- callStorageSas(request, args$sasToken, args$accountName, ...)
  }
  else {
    credentials <- getStorageCredentials()

    request <- prepareStorageRequest(request, credentials)
    response <- executeAzureRequest(request, ...)
  }

  extractAzureResponse(response, content)
}

listBlobs <- function(containerName, ...) {
  query <- list('restype' = "container", 'comp' = "list")

  request <- AzureRequest$new(
    method = "GET",
    path = paste0("/", containerName),
    query = query
  )

  callStorage(request, ...)
}

listContainers <- function(content = "parsed") {
  query <- list('comp' = "list")

  request <- AzureRequest$new(method = "GET",
                              path = paste0("/"),
                              query = query)

  callStorage(request, content)
}

deleteContainer <- function(containerName, content = "parsed") {
  query <- list('restype' = "container")

  request <- AzureRequest$new(
    method = "DELETE",
    path = paste0("/", containerName),
    query = query
  )

  callStorage(request, ...)
}

createContainer <- function(containerName, content = "parsed", ...) {
  query <- list('restype' = "container")

  request <- AzureRequest$new(
    method = "PUT",
    path = paste0("/", containerName),
    query = query
  )

  callStorage(request, content, ...)
}

deleteBlob <-
  function(containerName, blobName, content = "parsed", ...) {
    request <- AzureRequest$new(method = "DELETE",
                                path = paste0("/", containerName, "/", blobName))

    callStorage(request, content, ...)
  }

uploadBlob <-
  function(containerName,
           fileDirectory,
           sasToken = NULL,
           parallelThreads = 1,
           ...) {
    args <- list(...)

    if (!is.null(args$accountName)) {
      name <- args$accountName
    }
    else{
      storageCredentials <- getStorageCredentials()
      name <- storageCredentials$name
    }

    if (file.exists(fileDirectory)) {
      fileSize <- file.size(fileDirectory)
    }
    else if (file.exists(paste0(getwd(), "/", fileDirectory))) {
      fileDirectory <- paste0(getwd(), "/", fileDirectory)
      fileSize <- file.size(fileDirectory)
    }
    else{
      stop("The given file does not exist.")
    }

    # file size is less than 64 mb
    if (fileSize < (1024 * 1024 * 64)) {
      endFile <- httr::upload_file(fileDirectory)

      headers <- c()
      headers['Content-Length'] <- fileSize
      headers['Content-Type'] <- endFile$type
      headers['x-ms-blob-type'] <- 'BlockBlob'

      blobName <- basename(fileDirectory)
      if (!is.null(args$remoteName)) {
        blobName <- args$remoteName
      }

      request <- AzureRequest$new(
        method = "PUT",
        path = paste0("/", containerName, "/", blobName),
        headers = headers,
        body = endFile
      )

      callStorage(request, ...)
    }
    else{
      uploadChunk(containerName, fileDirectory, parallelThreads = parallelThreads, ...)
    }
  }

uploadChunk <-
  function(containerName,
           fileDirectory,
           sasToken = NULL,
           ...) {
    args <- list(...)
    if (!is.null(args$accountName)) {
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
    if (!is.null(args$remoteName)) {
      blobName <- args$remoteName
    }

    pb <- txtProgressBar(min = 0, max = numOfChunks, style = 3)

    `%fun%` <- foreach::`%do%`
    parallelThreads <- 1
    if (!is.null(args$parallelThreads) && args$parallelThreads > 1) {
      require(doParallel)
      parallelThreads <- args$parallelThreads
      registerDoParallel(parallelThreads)
      `%fun%` <- foreach::`%dopar%`
    }

    # Initialize the current indices for chunks and blockList
    currentChunk <- 0
    blockList <- ""

    while (currentChunk < numOfChunks) {
      count <- 1
      if (currentChunk + parallelThreads >= numOfChunks) {
        count <- numOfChunks - currentChunk
      }
      else{
        count <- parallelThreads
      }

      chunk <- readBin(to.read, raw(), n = defaultSize * count)
      accountName <- name

      results <-
        foreach::foreach(i = 0:(count - 1),
                         .export = c("sasToken", "accountName")) %fun% {
                           if (i == count - 1) {
                             data <- chunk[((i * defaultSize) + 1):length(chunk)]
                           }
                           else{
                             data <-
                               chunk[((i * defaultSize) + 1):((i * defaultSize) + defaultSize)]
                           }

                           blockId <- currentChunk + i
                           currLength <- 8 - nchar(blockId)

                           for (j in 1:currLength)
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
                             query = list('comp' = "block",
                                          'blockid' = blockId)
                           )

                           callStorage(request, ...)

                           return(paste0("<Latest>", blockId, "</Latest>"))
                         }


      if (!is.null(args$parallelThreads) && args$parallelThreads > 1) {
        require(doParallel)
        doParallel::stopImplicitCluster()
        foreach::registerDoSEQ()
      }

      for (j in 1:length(results)) {
        blockList <- paste0(blockList, results[[j]])
      }

      currentChunk <- currentChunk + count
      setTxtProgressBar(pb, currentChunk)
    }

    close(to.read)
    httpBodyRequest <-
      paste0("<BlockList>", blockList, "</BlockList>")
    httpBodyRequest <-
      paste0("<?xml version='1.0' encoding='utf-8'?>", httpBodyRequest)

    callStorage()
    if (is.null(sasToken)) {
      putBlockList(containerName, blobName, httpBodyRequest)
    }
    else{
      putBlockList(
        containerName,
        blobName,
        body = httpBodyRequest,
        sasToken = sasToken,
        accountName = name
      )
    }
  }

putBlockList <-
  function(containerName,
           fileName,
           body,
           sasToken = NULL,
           ...) {
    headers <- c()
    headers['Content-Length'] <- nchar(body)
    headers['Content-Type'] <- 'text/xml'

    request <- AzureRequest$new(
      method = "PUT",
      path = paste0("/", containerName, "/", fileName),
      headers = headers,
      query = list('comp' = "blocklist"),
      body
    )

    callStorage(request, ...)
  }

getBlockList <-
  function(containerName, fileName, ...) {
    request <- AzureRequest$new(
      method = "GET",
      path = paste0("/", containerName, "/", fileName),
      query = list('comp' = "blocklist",
                   'blocklisttype' = "all")
    )

    callStorage(request, ...)
  }

uploadDirectory <- function(containerName, fileDirectory, ...) {
  files = list.files(fileDirectory, full.names = TRUE, recursive = TRUE)
  fileName = list.files(fileDirectory, recursive = TRUE)

  for (i in 1:length(files))
  {
    uploadBlob(containerName, files[i], remoteName = fileName[i], ...)
  }
}

downloadBlob <- function(containerName,
                         blobName,
                         overwrite = FALSE,
                         localDest = NULL,
                         ...) {
  if (!is.null(localDest)) {
    write <- httr::write_disk(localDest, overwrite)
  }
  else {
    write <- httr::write_memory()
  }

  request <- AzureRequest$new(method = "GET",
                              path = paste0("/", containerName, "/", blobName))

  callStorage(request, write, ...)
}
