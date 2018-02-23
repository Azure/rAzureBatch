BlobOperations <- R6::R6Class("BlobOperations",
  public = list(
    path = "/%s",
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
    listBlobs = function(containerName, prefix = "", content = "parsed", ...) {
      query <- list('restype' = "container", 'comp' = "list", 'prefix' = prefix)

      request <- AzureRequestV2$new(
        method = "GET",
        path = paste0(sprintf(self$path, containerName)),
        query = query
      )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    deleteBlob =
      function(containerName, blobName, content = "parsed", ...) {
        request <-
          AzureRequestV2$new(method = "DELETE",
                             path = paste0(sprintf(self$path, containerName),
                                           "/",
                                           blobName))

        response <- self$client$execute(request)
        self$client$extractAzureResponse(response, content)
      },
    uploadBlob =
      function(containerName,
               fileDirectory,
               parallelThreads = 1,
               content = "response",
               ...) {
        args <- list(...)

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
          fileToUpload <- httr::upload_file(fileDirectory)

          headers <- c()
          headers['Content-Length'] <- fileSize
          headers['Content-Type'] <- fileToUpload$type
          headers['x-ms-blob-type'] <- 'BlockBlob'

          blobName <- basename(fileDirectory)
          if (!is.null(args$remoteName)) {
            blobName <- args$remoteName
          }

          request <- AzureRequestV2$new(
            method = "PUT",
            path = paste0(sprintf(self$path,
                                  containerName),
                          "/",
                          blobName),
            headers = headers,
            body = fileToUpload,
            content = content
          )

          response <- self$client$execute(request)
          self$client$extractAzureResponse(response, content)
        }
        else {
          uploadChunk(containerName, fileDirectory, content = content, parallelThreads = parallelThreads, ...)
        }
      },
    downloadBlob = function(containerName,
                             blobName,
                             overwrite = FALSE,
                             downloadPath = NULL,
                             progress = FALSE,
                             ...) {
      write <- httr::write_memory()
      if (!is.null(downloadPath)) {
        write <- httr::write_disk(downloadPath, overwrite)
      }

      content <- NULL
      if (grepl(".txt", blobName)) {
        content <- "text"
      }

      progress <- NULL
      if (progress) {
        progress <- httr::progress()
      }

      request <-
        AzureRequestV2$new(method = "GET",
                           path = paste0(sprintf(self$path,
                                                 containerName),
                                         "/",
                                         blobName),
                           progress = progress,
                           write = write,
                           content = content
                         )

      response <- self$client$execute(request)
      self$client$extractAzureResponse(response, content)
    },
    uploadChunk =
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
          doParallel::registerDoParallel(parallelThreads)
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
              .export = c("sasToken", "accountName", "content")
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

              request <- AzureRequestV2$new(
                method = "PUT",
                path = paste0(sprintf(self$path, containerName),
                              "/",
                              blobName),
                headers = headers,
                query = list('comp' = "block",
                             'blockid' = blockId)
              )

              callStorage(
                request,
                content = NULL,
                body = data,
                progress = TRUE,
                ...
              )

              return(paste0("<Latest>", blockId, "</Latest>"))
            }


          if (!is.null(args$parallelThreads) &&
              args$parallelThreads > 1) {
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

        close(readBytes)
        httpBodyRequest <-
          paste0("<BlockList>", blockList, "</BlockList>")
        httpBodyRequest <-
          paste0("<?xml version='1.0' encoding='utf-8'?>", httpBodyRequest)

        putBlockList(containerName,
                     blobName,
                     content = "response",
                     body = httpBodyRequest,
                     ...)
      },
    putBlockList =
      function(containerName,
               fileName,
               body,
               content = "text",
               ...) {
        headers <- c()
        headers['Content-Length'] <- nchar(body)
        headers['Content-Type'] <- 'text/xml'

        request <- AzureRequestV2$new(
          method = "PUT",
          path = paste0(sprintf(self$path, containerName),
                        "/",
                        fileName),
          headers = headers,
          query = list('comp' = "blocklist")
        )

        callStorage(request, content, body = body, ...)
      },
    getBlockList =
      function(containerName, fileName, content = "parsed", ...) {
        request <- AzureRequestV2$new(
          method = "GET",
          path = paste0(sprintf(self$path, containerName),
                        "/",
                        fileName),
          query = list('comp' = "blocklist",
                       'blocklisttype' = "all")
        )

        callStorage(request, content, ...)
      },
    uploadDirectory = function(containerName, fileDirectory, ...) {
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
  )
)
