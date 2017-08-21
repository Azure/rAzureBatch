apiVersion <- "2017-05-01.5.0"

getBatchCredentials <- function(configPath = "az_config.json", ...){
  config <- getOption("az_config")

  if(!is.null(config) &&!is.null(config$batchAccount)){
    batchAccount <- config$batchAccount
    credentials <- BatchCredentials$new(name=batchAccount$name, key=batchAccount$key, url=batchAccount$url)
  }
  else{
    config <- rjson::fromJSON(file = configPath)
    credentials <- BatchCredentials$new(name=config$batchAccount$name, key=config$batchAccount$key, url=config$batchAccount$url)
  }

  credentials
}

BatchCredentials <- setRefClass("BatchCredentials",
         fields = list(name = "character", key = "character", url="character"),
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

callBatchService <- function(request, credentials, body = NULL, writeFlag = FALSE, verbose = FALSE, ...){
  args <- list(...)
  contentType = args$contentType

  requestdate <- httr::http_date(Sys.time())

  headers <- request$headers
  headers['ocp-date'] <- requestdate

  canonicalizedHeaders <- ""
  for(name in sort(names(headers))){
    if(grepl('ocp-', name)){
      canonicalizedHeaders <- paste0(canonicalizedHeaders, name,":", headers[name], "\n")
    }
  }

  canonicalizedHeaders <- substr(canonicalizedHeaders, 1, nchar(canonicalizedHeaders) - 1)

  canonicalizedResource <- paste0("/", credentials$name, request$path, "\n")
  for (name in sort(names(request$query))) {
    canonicalizedResource <- paste0(canonicalizedResource, name,":", request$query[[name]], "\n")
  }

  canonicalizedResource <- substr(canonicalizedResource, 1, nchar(canonicalizedResource) - 1)

  stringToSign <- createSignature(request$method, request$headers)
  stringToSign <- paste0(stringToSign, canonicalizedHeaders, "\n")
  stringToSign <- paste0(stringToSign, canonicalizedResource)

  authString <- sprintf("SharedKey %s:%s", credentials$name, credentials$signString(stringToSign))

  headers['Authorization'] <- authString

  requestHeaders <- httr::add_headers(.headers = headers, "User-Agent"=paste0("rAzureBatch/", packageVersion("rAzureBatch"), ";", "doAzureParallel/", packageVersion("doAzureParallel")))

  response <- ""

  url <- paste0(credentials$url, request$path)

  config <- getOption("az_config")

  verbose <- ifelse(!is.null(config) && !is.null(config$settings),
                    config$settings$verbose,
                    getOption("verbose"))

  write <- if(writeFlag) { httr::write_memory() } else { NULL }
  verboseMode <- if(getOption("verbose")){ httr::verbose() } else { NULL }

  if (verbose) {
    print(stringToSign)
    print(url)
    print(paste0("Auth String: ", authString))
    print(requestHeaders)
  }

  response <- httr::VERB(request$method,
                   url,
                   config = requestHeaders,
                   verboseMode,
                   write,
                   query = request$query,
                   body = body,
                   encode = "json")

  if(!is.null(contentType) && contentType){
    httr::content(response, as = "text")
  }
  else{
    httr::content(response)
  }
}
