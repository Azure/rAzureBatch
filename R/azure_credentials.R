AzureCredentials <- R6::R6Class(
  "AzureCredentials",
  public = list(
    getClassName = function(){
      private$className
    }
  ),
  private = list(
    className = "AzureCredentials"
  )
)

ServicePrincipalCredentials <- R6::R6Class(
  "ServicePrincipalCredentials",
  inherit = AzureCredentials,
  public = list(
    tenantId = NULL,
    clientId = NULL,
    clientSecrets = NULL,
    resource = NULL,
    initialize = function(tenantId = NA, clientId = NA, clientSecrets = NA, resource = NA) {
      self$tenantId <- tenantId
      self$clientId <- clientId
      self$clientSecrets <- clientSecrets
      self$resource <- resource
    },
    getAccessToken = function() {
      # Azure SMR: Get token calls
      URLGT <- paste0("https://login.microsoftonline.com/",
                      self$tenantId,
                      "/oauth2/token?api-version=1.0")
      authKeyEncoded <- URLencode(self$clientSecrets, reserved = TRUE)
      resourceEncoded <- URLencode(self$resource, reserved = TRUE)
      bodyGT <- paste0("grant_type=client_credentials", "&client_secret=", authKeyEncoded,
                       "&client_id=", self$clientId, "&resource=", resourceEncoded)
      r <- httr::POST(URLGT,
                      httr::add_headers(
                        .headers = c(`Cache-Control` = "no-cache",
                                     `Content-type` = "application/x-www-form-urlencoded")),
                      body = bodyGT)

      j1 <- httr::content(r, "parsed", encoding = "UTF-8")
      accessToken <- paste("Bearer", j1$access_token)

      private$expiration <- Sys.time() + 3590
      private$accessToken <- accessToken
      private$refreshToken <- j1$refresh_token
    },
    checkAccessToken = function(){
      if (is.null(private$accessToken) || private$expiration < Sys.time()) {
        self$getAccessToken()
      }

      private$accessToken
    },
    getRefreshToken = function() {
      # Azure SMR: Get token calls
      URLGT <- paste0("https://login.microsoftonline.com/", tenantID, "/oauth2/token?api-version=1.0")
      refreshTokenEncoded <- URLencode(refreshToken, reserved = TRUE)
      # NOTE: Providing the optional client ID fails the request!
      bodyGT <- paste0("grant_type=refresh_token", "&refresh_token=", refreshTokenEncoded)
      r <- httr::POST(URLGT,
                      add_headers(
                        .headers = c(`Cache-Control` = "no-cache",
                                     `Content-type` = "application/x-www-form-urlencoded")),
                      body = bodyGT)

      j1 <- httr::content(r, "parsed", encoding = "UTF-8")

      accessToken <- paste("Bearer", j1$access_token)

      private$expiration <- Sys.time() + 3590
      private$accessToken <- accessToken
      private$refreshToken <- j1$refresh_token
    }
  ),
  private = list(
    className = "ServicePrincipalCredentials",
    expiration = NULL,
    accessToken = NULL,
    refreshToken = NULL
  )
)

SharedKeyCredentials <- R6::R6Class(
  "SharedKeyCredentials",
  inherit = AzureCredentials,
  public = list(
    name = NULL,
    key = NULL,
    initialize = function(name = NA, key = NA) {
      self$name <- name
      self$key <- key
    },
    signRequest = function(request, prefix) {
      headers <- request$headers
      canonicalizedHeaders <- ""

      systemLocale <- Sys.getlocale(category = "LC_COLLATE")
      Sys.setlocale("LC_COLLATE", "C")

      for (name in sort(names(headers))) {
        if (grepl(prefix, name)) {
          canonicalizedHeaders <-
            paste0(canonicalizedHeaders, name, ":", headers[name], "\n")
        }
      }

      canonicalizedResource <- paste0("/", self$name, request$path, "\n")
      if (!is.null(names(request$query))) {
        for (name in sort(names(request$query))) {
          canonicalizedResource <-
            paste0(canonicalizedResource, name, ":", request$query[[name]], "\n")
        }
      }

      canonicalizedResource <-
        substr(canonicalizedResource, 1, nchar(canonicalizedResource) - 1)

      stringToSign <- createSignature(request$method, request$headers)
      stringToSign <- paste0(stringToSign, canonicalizedHeaders)
      stringToSign <- paste0(stringToSign, canonicalizedResource)

      # sign the request
      authorizationString <-
        paste0("SharedKey ",
               self$name,
               ":",
               request$encryptSignature(stringToSign, self$key))

      Sys.setlocale("LC_COLLATE", systemLocale)

      authorizationString
    }
  ),
  private = list(
    className = "SharedKeyCredentials"
  )
)
