uploadChunk <-
  function(containerName,
           fileDirectory,
           content = "parsed",
           ...) {
    args <- list(...)

    finfo <- file.info(fileDirectory)
    readBytes <- file(fileDirectory, "rb")

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

    sasToken <- args$sasToken
    accountName <- args$accountName
    config <- getOption("az_config")
    if (is.null(config) &&
        (is.null(sasToken) || is.null(accountName))) {
      stop(
        paste(
          "Missing authentication information: Use",
          "setCredentials or provide sasToken and accountName"
        )
      )
    }

    `%fun%` <- foreach::`%do%`
    parallelThreads <- 1
    if (!is.null(args$parallelThreads) &&
        args$parallelThreads > 1) {
      require(doParallel)
      parallelThreads <- args$parallelThreads
      cluster <- parallel::makeCluster(parallelThreads, outfile = "stdout.txt")
      doParallel::registerDoParallel(cluster)
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

      chunk <- readBin(readBytes, raw(), n = defaultSize * count)

      results <-
        foreach::foreach(
          i = 0:(count - 1),
          .export = c("sasToken", "accountName", "config", "chunk", "content")
        ) %fun% {
          options("az_config" = config)

          blockSize <- i * defaultSize

          if (i == count - 1) {
            data <- chunk[(blockSize + 1):length(chunk)]
          }
          else {
            data <-
              chunk[(blockSize + 1):(blockSize + defaultSize)]
          }

          blockId <- currentChunk + i
          currLength <- 8 - nchar(blockId)

          for (j in 1:currLength)
          {
            blockId <- paste0(blockId, 0)
          }

          blockId <-
            RCurl::base64Encode(enc2utf8(blockId))

          headers <- c()
          headers['Content-Length'] <-
            as.character(length(data))
          headers['x-ms-blob-type'] <- 'BlockBlob'

          request <- AzureRequest$new(
            method = "PUT",
            path = paste0("/", containerName, "/", blobName),
            headers = headers,
            query = list('comp' = "block",
                         'blockid' = blockId)
          )

          print(length(data))
          print(data[1:10])
          callStorage(request,
                      content = NULL,
                      body = data,
                      progress = TRUE,
                      ...)

          return(paste0("<Latest>", blockId, "</Latest>"))
        }


      if (!is.null(args$parallelThreads) &&
          args$parallelThreads > 1) {
        require(doParallel)
        parallel::stopCluster(cluster)
        foreach::registerDoSEQ()
      }

      for (j in 1:length(results)) {
        blockList <- paste0(blockList, results[[j]])
      }

      currentChunk <- currentChunk + count
      setTxtProgressBar(pb, currentChunk)
    }

    close(readBytes)
    httpBodyRequest <-
      paste0("<BlockList>", blockList, "</BlockList>")
    httpBodyRequest <-
      paste0("<?xml version='1.0' encoding='utf-8'?>", httpBodyRequest)

    putBlockList(containerName, blobName, body = httpBodyRequest, ...)
  }

putBlockList <-
  function(containerName,
           fileName,
           body,
           content = "parsed",
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

    callStorage(request, content, ...)
  }

getBlockList <-
  function(containerName, fileName, content = "parsed", ...) {
    request <- AzureRequest$new(
      method = "GET",
      path = paste0("/", containerName, "/", fileName),
      query = list('comp' = "blocklist",
                   'blocklisttype' = "all")
    )

    callStorage(request, content, ...)
  }

uploadDirectory <- function(containerName, fileDirectory, ...) {
  files <-
    list.files(fileDirectory, full.names = TRUE, recursive = TRUE)
  fileName <- list.files(fileDirectory, recursive = TRUE)

  for (i in 1:length(files))
  {
    uploadBlob(containerName,
               files[i],
               remoteName = fileName[i],
               content = "parsed",
               ...)
  }
}
