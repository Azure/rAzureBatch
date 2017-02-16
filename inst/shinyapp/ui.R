
# This is the user-interface definition of a Shiny web application.
# You can find out more about building applications with Shiny here:
#
# http://shiny.rstudio.com
#

library(shinydashboard)
library(AzureBatch)
library(shiny)

header <- dashboardHeader(
  title = "Azure Batch"
)

body <- dashboardBody(
  fluidRow(
    column(width=9,
           tabBox(side = "left", height = "500px", width = NULL,
               tabPanel('Iris',     DT::dataTableOutput('ex1')),
               tabPanel('Nodes',        DT::dataTableOutput('cluster')),
               tabPanel('Tasks',      DT::dataTableOutput('table')))),

    column(width=3,
           box(width = NULL, solidHeader = TRUE,
               textInput("directory", "Choose Configuration File")))
  )
)

dashboardPage(
  header,
  dashboardSidebar(disable = TRUE),
  body
)
