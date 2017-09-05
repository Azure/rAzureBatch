listBlobs <- function(containerName, prefix = "", content = "parsed", ...) {
  query <- list('restype' = "container", 'comp' = "list", 'prefix' = prefix)

  request <- AzureRequest$new(
    method = "GET",
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
        headers = headers
      )

      callStorage(request, content, uploadFile = endFile, ...)
    }
    else {
      uploadChunk(containerName, fileDirectory, parallelThreads = parallelThreads, ...)
    }
  }

downloadBlob <- function(containerName,
                         blobName,
                         overwrite = FALSE,
                         downloadPath = NULL,
                         progress = FALSE,
                         ...) {
  if (!is.null(downloadPath)) {
    write <- httr::write_disk(downloadPath, overwrite)
  }
  else {
    write <- httr::write_memory()
  }

  request <- AzureRequest$new(method = "GET",
                              path = paste0("/", containerName, "/", blobName))

  if (grepl(".txt", blobName)) {
    content = "text"
  }
  else {
    content = NULL
  }

  callStorage(request, content = content, write = write, progress = progress, ...)
}
