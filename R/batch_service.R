apiVersion <- "2017-06-01.5.1"

getBatchCredentials <- function(configPath = "az_config.json", ...) {
  config <- getOption("az_config")

  if (!is.null(config) && !is.null(config$batchAccount)) {
    batchAccount <- config$batchAccount

    BatchCredentials$new(name = batchAccount$name,
                         key = batchAccount$key,
                         url = batchAccount$url)
  }
  else if (!is.null(config) && !is.null(config$ServicePrincipal)) {
    servicePrincipal <- config$ServicePrincipal

    servicePrincipal
  }
  else {
    config <- rjson::fromJSON(file = configPath)

    BatchCredentials$new(
      name = config$batchAccount$name,
      key = config$batchAccount$key,
      url = config$batchAccount$url
    )
  }
}

BatchCredentials <- setRefClass(
  "BatchCredentials",
  fields = list(name = "character", key = "character", url = "character"),
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
    initialize = function(tenantId = NA, clientId = NA, clientSecrets = NA) {
      self$tenantId <- tenantId
      self$clientId <- clientId
      self$clientSecrets <- clientSecrets
    },
    getAccessToken = function(resource) {
      app_name <- 'AzureServiceCredentials'

      URLGT <- paste0("https://login.microsoftonline.com/", self$tenantId, "/oauth2/token?api-version=1.0")
      authKeyEncoded <- URLencode(self$clientSecrets, reserved = TRUE)
      resourceEncoded <- URLencode(resource, reserved = TRUE)
      bodyGT <- paste0("grant_type=client_credentials", "&client_secret=", authKeyEncoded,
                       "&client_id=", self$clientId, "&resource=", resourceEncoded)
      r <- httr::POST(URLGT,
                      httr::add_headers(
                        .headers = c(`Cache-Control` = "no-cache",
                                     `Content-type` = "application/x-www-form-urlencoded")),
                      body = bodyGT,
                      httr::verbose())

      j1 <- httr::content(r, "parsed", encoding = "UTF-8")
      accessToken <- paste("Bearer", j1$access_token)

      private$expiration <- Sys.time() + 3590
      private$accessToken <- accessToken
      private$refreshToken <- j1$refresh_token
    },
    checkAccessToken = function(resource){
      if (is.null(private$accessToken) || private$expiration < Sys.time()) {
        self$getAccessToken(resource)
      }

      private$accessToken
    },
    getRefreshToken = function(resource) {
      app_name <- 'AzureServiceCredentials'

      URLGT <- paste0("https://login.microsoftonline.com/", self$tenantId, "/oauth2/token?api-version=1.0")
      authKeyEncoded <- URLencode(self$clientSecrets, reserved = TRUE)
      resourceEncoded <- URLencode(resource, reserved = TRUE)
      bodyGT <- paste0("grant_type=client_credentials", "&client_secret=", authKeyEncoded,
                       "&client_id=", self$clientId, "&resource=", resourceEncoded)
      r <- httr::POST(URLGT,
                      httr::add_headers(
                        .headers = c(`Cache-Control` = "no-cache",
                                     `Content-type` = "application/x-www-form-urlencoded")),
                      body = bodyGT,
                      httr::verbose())

      j1 <- httr::content(r, "parsed", encoding = "UTF-8")
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

      #if (!is.null(headers)) {
        for (name in sort(names(headers))) {
          if (grepl(prefix, name)) {
            canonicalizedHeaders <-
              paste0(canonicalizedHeaders, name, ":", headers[name], "\n")
          }
        }
      #}

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

BatchServiceClient <- R6::R6Class(
  inherit = AzureServiceClient,
  "BatchServiceClient",
  public = list(
    poolOperations = NULL,
    jobOperations = NULL,
    taskOperations = NULL,
    fileOperations = NULL,
    apiVersion = "2017-06-01.5.1",
    verbose = FALSE,
    initialize = function(url = NA, authentication = NA) {
      self$url <- url
      self$authentication <- authentication
      self$poolOperations <- PoolOperations$new(self, url, authentication, apiVersion)
      self$jobOperations <- JobOperations$new(self, url, authentication, apiVersion)
      self$taskOperations <- TaskOperations$new(self, url, authentication, apiVersion)
      self$fileOperations <- FileOperations$new(self, url, authentication, apiVersion)
    },
    execute = function(request) {
      requestdate <- httr::http_date(Sys.time())
      request$headers['ocp-date'] <- requestdate
      request$headers['User-Agent'] <-
        paste0(
          "rAzureBatch/",
          packageVersion("rAzureBatch"),
          " ",
          "doAzureParallel/",
          packageVersion("doAzureParallel")
        )

      if (self$authentication$getClassName() == "SharedKeyCredentials") {
        authorizationHeader <- self$authentication$signRequest(request,
                                                   "ocp-")
      }
      # Service Principal Path
      else {
        authorizationHeader <- self$authentication$checkAccessToken(
          'https://batch.core.windows.net/')
      }

      request$headers['Authorization'] <- authorizationHeader
      url <- paste0(self$url, request$path)

      executeResponse(url, request)
    }
  )
)

prepareBatchRequest <- function(request, credentials, authType = "SharedKey") {
  requestdate <- httr::http_date(Sys.time())
  request$headers['ocp-date'] <- requestdate

  if (!is.null(credentials[["name"]])) {
    authorizationHeader <-
      signAzureRequest(request, credentials$name, credentials$key, 'ocp-')
  }
  # Service Principal case
  else {
    app_name <- 'BatchTestPackageR' # not important for authorization grant flow
    # 3. Insert the created apps client ID which was issued after app creation:
    client_id <- credentials$clientId
    # In case your app was registered as a web app instead of a native app,
    # you might have to add your secret key string here:
    client_secret <- credentials$secret
    # API resource ID to request access for, e.g. Power BI:
    resource_uri <- 'https://batch.core.windows.net/'
    # azure_endpoint <- httr::oauth_endpoint(authorize = sprintf("https://login.windows.net/%s/oauth2/authorize",
    #                                    credentials$tenantId),
    #                access = sprintf("https://login.windows.net/%s/oauth2/token",
    #                                 credentials$tenantId))
    # #azure_endpoint <- httr::oauth_endpoints('azure')
    # myapp <- httr::oauth_app(appname = app_name,
    #                    key = client_id,
    #                    secret = client_secret)
    #
    # mytoken <- httr::oauth2.0_token(azure_endpoint, myapp,
    #                           user_params = list(resource = resource_uri),
    #                           use_oob = FALSE)
    # if (('error' %in% names(mytoken$credentials)) && (nchar(mytoken$credentials$error) > 0)) {
    #   errorMsg <- paste('Error while acquiring token.',
    #                     paste('Error message:', mytoken$credentials$error),
    #                     paste('Error description:', mytoken$credentials$error_description),
    #                     paste('Error code:', mytoken$credentials$error_codes),
    #                     sep = '\n')
    #   stop(errorMsg)
    # }

    URLGT <- paste0("https://login.microsoftonline.com/", credentials$tenantId, "/oauth2/token?api-version=1.0")
    authKeyEncoded <- URLencode(client_secret, reserved = TRUE)
    resourceEncoded <- URLencode(resource_uri, reserved = TRUE)
    bodyGT <- paste0("grant_type=client_credentials", "&client_secret=", authKeyEncoded,
                     "&client_id=", client_id, "&resource=", resourceEncoded)
    r <- httr::POST(URLGT,
                    httr::add_headers(
                      .headers = c(`Cache-Control` = "no-cache",
                                   `Content-type` = "application/x-www-form-urlencoded")),
                    body = bodyGT,
                    httr::verbose())

    j1 <- httr::content(r, "parsed", encoding = "UTF-8")

    authorizationHeader <- paste("Bearer", j1$access_token)
  }

  request$headers['Authorization'] <- authorizationHeader
  request$headers['User-Agent'] <-
    paste0(
      "rAzureBatch/",
      packageVersion("rAzureBatch"),
      " ",
      "doAzureParallel/",
      packageVersion("doAzureParallel")
    )

  #request$url <- paste0(credentials$url, request$path)
  request$url <- paste0("https://brhoan.southcentralus.batch.azure.com", request$path)

  request
}

callBatchService <- function(request, credentials, content, ...){
  request <- prepareBatchRequest(request, credentials)
  response <- executeAzureRequest(request, ...)

  extractAzureResponse(response, content)
}
