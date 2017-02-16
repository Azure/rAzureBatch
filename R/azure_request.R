createSignature <- function(requestMethod, headerList=character()) {
  headers <- c('Content-Encoding',
               'Content-Language',
               'Content-Length',
               'Content-MD5',
               'Content-Type',
               'Date',
               'If-Modified-Since',
               'If-Match',
               'If-None-Match',
               'If-Unmodified-Since',
               'Range' )

  stringToSign <- paste0(requestMethod, "\n")

  for(name in headers){
    temp <- ifelse(!is.na(headerList[name]), headerList[name], "")

    stringToSign <- paste0(stringToSign, temp, "\n")
  }

  return(stringToSign)
}

AzureRequest <- setRefClass("AzureRequest",
                            fields = list(
                              method = "character",
                              path = "character",
                              headers = "character",
                              query = "list"),

                            methods = list(
                              signString = function(x, key){
                                undecodedKey <- RCurl::base64Decode(key, mode="raw")
                                newString<-RCurl::base64(
                                  digest::hmac(key=undecodedKey,
                                               object=enc2utf8(x),
                                               algo= "sha256", raw=TRUE)
                                )
                              }
                            ))
