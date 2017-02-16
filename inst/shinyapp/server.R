library(xml2)
library(AzureBatch)
library(shiny)
library(jsonlite)

shinyServer(function(input, output, session) {
  autoInvalidate <- reactiveTimer(10000)

  output$ex1 <- DT::renderDataTable(
    DT::datatable(iris, options = list(pageLength = 25),  style = 'bootstrap')
  )

  output$cluster <- DT::renderDataTable({
    autoInvalidate()

    setPoolOption(input$directory, fullName = TRUE)
    config <- getOption("az_config")
    pool <- config$batchAccount$pool

    nodes <- listPoolNodes(pool$name)

    state <- c()
    id <- c()

    if(length(nodes$value) != 0){
      for(i in 1:length(nodes$value))
      {
        id <- c(id, nodes$value[[i]]$id)
        state <- c(state, nodes$value[[i]]$state)
      }
    }

    return(data.frame(id, state))
  }, options = list(style = 'bootstrap'))

  output$table <- renderDataTable({
    #autoInvalidate()
    a <- "debug"
    setPoolOption(input$directory, fullName = TRUE)

    tasks <- if(input$sessionId == "") c() else listTask(input$sessionId, creds)
    state <- c()
    id <- c()
    commandLine <- c()

    for(i in 1:length(tasks$value))
    {
      state <- c(state, tasks$value[[i]]$state)
      id <- c(id, tasks$value[[i]]$id)
      commandLine <- c(commandLine, tasks$value[[i]]$commandLine)
    }

    data.frame(id, state, commandLine)
  })
})
