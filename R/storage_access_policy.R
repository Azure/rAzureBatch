signed_signature <- 'sig'
signed_permission <- 'sp'
signed_start <- 'st'
signed_expiry <- 'se'
signed_resource <- 'sr'
signed_identifier <- 'sip'
signed_protocol <- 'spr'
signed_version <- 'sv'
signed_cache_control <- 'rscc'
signed_content_disposition <- 'rscd'
signed_content_encoding <- 'rsce'
signed_content_language <- 'rscl'
signed_content_type <- 'rsct'
signed_resource_types <- 'srt'
signed_services <- 'ss'
signed_ip <- 'si'
signed_version <- 'sv'

constructSas <- function(expiry, permission, sr, path, account_key){
  myList <- list()
  query = c()

  startTime <- as.POSIXlt(Sys.time() - 60*60*24*1, "UTC", "%Y-%m-%dT%H:%M:%S")
  startTime <- paste0(strftime(startTime, "%Y-%m-%dT%TZ"))

  endTime <- as.POSIXlt(Sys.time() + 60*60*24*2, "UTC", "%Y-%m-%dT%H:%M:%S")
  endTime <- paste0(strftime(endTime, "%Y-%m-%dT%TZ"))

  query[signed_start] <- startTime
  query[signed_expiry] <- endTime
  query[signed_permission] <- permission
  query[signed_version] <- storageVersion
  query[signed_resource] <- sr


  myList[[signed_version]] <- storageVersion
  myList[[signed_resource]] <- sr
  myList[[signed_start]] <- startTime
  myList[[signed_expiry]] <- endTime
  myList[[signed_permission]] <- permission

  storageCredentials <- getStorageCredentials()
  canonicalizedResource <- paste0("/blob/", storageCredentials$name, "/", path, "\n")

  stringToSign <- paste0(getValueFromQuery(query, signed_permission))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_start))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_expiry))
  stringToSign <- paste0(stringToSign, canonicalizedResource)

  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_identifier))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_ip))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_protocol))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_version))

  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_cache_control))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_content_disposition))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_content_encoding))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_content_language))
  stringToSign <- paste0(stringToSign, getValueFromQuery(query, signed_content_type))

  stringToSign <- substr(stringToSign, 1, nchar(stringToSign) - 1)

  config <- getOption("az_config")
  if(!is.null(config) && !is.null(config$settings)){
    verbose <- config$settings$verbose
  }
  else{
    verbose <- getOption("verbose")
  }

  if(verbose){
    print(stringToSign)
  }

  undecodedKey <- RCurl::base64Decode(account_key, mode="raw")
  encString <- RCurl::base64(
    digest::hmac(key=undecodedKey,
                 object=enc2utf8(stringToSign),
                 algo= "sha256", raw=TRUE)
  )

  myList[[signed_signature]] <- encString
  myList
}

getValueFromQuery <- function(query, header){
  value <- "\n"

  if(!is.na(query[header])){
    value <- paste0(query[header], "\n")
  }

  value
}

generateResourceFile <- function(storageAccount, containerName, fileName, sasToken){
  file <- sprintf("https://%s.blob.core.windows.net/%s/%s", storageAccount, containerName, fileName)

  query <- generateSasUrl(sasToken)
  file <- paste0(file, query)

  resourceFile <-list(
    blobSource = file,
    filePath = fileName
  )
}

generateSasUrl <- function(sasToken){
  file <- "?"

  for(query in names(sasToken)){
    file <- paste0(file, query, "=", curlEscape(sasToken[[query]]), "&")
  }

  file <- substr(file, 1, nchar(file) - 1)
}


