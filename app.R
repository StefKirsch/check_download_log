library(shiny)
library(dplyr)
library(lubridate)
library(rstudioapi)

source("R/wave_helpers.R")

ui <- fluidPage(

  # App title
  titlePanel("Download Log Validator"),

  # File input and check button
  fileInput("file1", "Choose XLSX File", accept = c(".xlsx")),
  actionButton("check_file", "Check File"),

  # Text output for warnings and messages with CSS for line wrapping
  tags$style(HTML("
    #warnings {
      white-space: pre-wrap;  /* Ensures line breaks are respected */
      word-wrap: break-word;  /* Forces long words to break */
    }
  ")),

  verbatimTextOutput("warnings")
)

server <- function(input, output) {

  # Reactive expression to read the uploaded file when the button is clicked
  observeEvent(input$check_file, {

    # Ensure a file is selected
    req(input$file1)

    # Create a temporary file to store the console output
    tmpfile <- tempfile()

    # Open a connection to the temporary file
    con <- file(tmpfile, open = "wt")

    # Capture all console output (warnings, messages, and errors)
    sink(con, type = "output")
    sink(con, type = "message")

    # Run the file reading and validation inside tryCatch
    tryCatch({
      # Read the file
      path_download_log <- input$file1$datapath

      # Read and validate download log
      download_log <- read_download_log(path_download_log)

      # If everything is correct, add success message
      message("Download log is correct.")

    }, warning = function(w) {
      # Capture warnings
      message(paste("Warning:", w$message))

    }, error = function(e) {
      # Capture errors
      message(paste("Error:", e$message))

    }, finally = {
      # Stop capturing the output and close the connection
      sink(type = "output")
      sink(type = "message")
      close(con)

      # Read the entire captured output
      full_output <- paste(readLines(tmpfile), collapse = "\n")

      # Display the captured output in the UI
      output$warnings <- renderText(full_output)

      # Remove the temporary file
      unlink(tmpfile)
    })

  })
}

shinyApp(ui = ui, server = server)
