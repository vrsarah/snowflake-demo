# Snowflake OAuth Demo App for Posit Connect
# This app demonstrates OAuth authentication with Snowflake using Connect's OAuth Integration

library(shiny)
library(DBI)
library(odbc)
library(bslib)
library(httr)
library(jsonlite)

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT <- "BMB83620"
SNOWFLAKE_DATABASE <- "TEST_DB"
SNOWFLAKE_WAREHOUSE <- "COMPUTE_WH"
SNOWFLAKE_SCHEMA <- "PUBLIC"

# Function to get OAuth access token from Connect
get_oauth_token <- function(session) {
  # Only works when running on Connect
  if (Sys.getenv("RSTUDIO_PRODUCT") != "CONNECT") {
    return(list(
      success = FALSE,
      error = "This app must run on Posit Connect to use OAuth integrations"
    ))
  }

  tryCatch({
    # Get the user session token from Shiny session headers
    user_session_token <- session$request$HTTP_POSIT_CONNECT_USER_SESSION_TOKEN

    if (is.null(user_session_token) || user_session_token == "") {
      return(list(
        success = FALSE,
        error = "No user session token found. This app requires OAuth authentication."
      ))
    }

    # Get Connect server address
    connect_server <- Sys.getenv("CONNECT_SERVER")
    if (connect_server == "") {
      # Fallback: try to construct from request
      connect_server <- paste0(
        session$request$HTTP_X_FORWARDED_PROTO %||% "https",
        "://",
        session$request$HTTP_HOST
      )
    }

    # Call Connect's OAuth credentials exchange endpoint
    # This will return an access token if user has authorized
    # OR return error that triggers Connect's OAuthLoginModal
    response <- POST(
      url = paste0(connect_server, "/__api__/v1/oauth/integrations/credentials"),
      body = list(
        grant_type = "urn:ietf:params:oauth:grant-type:token-exchange",
        subject_token = user_session_token,
        subject_token_type = "urn:posit:connect:user-session-token",
        requested_token_type = "urn:ietf:params:oauth:token-type:access_token"
      ),
      encode = "json",
      add_headers(
        "Content-Type" = "application/json"
      )
    )

    if (status_code(response) != 200) {
      content <- content(response, as = "parsed")
      return(list(
        success = FALSE,
        error = paste0("OAuth error: ", content$message %||% "Unknown error"),
        status_code = status_code(response)
      ))
    }

    token_data <- content(response, as = "parsed")

    return(list(
      success = TRUE,
      access_token = token_data$access_token,
      token_type = token_data$token_type
    ))

  }, error = function(e) {
    return(list(
      success = FALSE,
      error = paste("Token fetch error:", e$message)
    ))
  })
}

# Function to attempt Snowflake connection with OAuth
connect_to_snowflake <- function(session) {
  # Get OAuth token from Connect
  token_result <- get_oauth_token(session)

  if (!token_result$success) {
    return(list(
      success = FALSE,
      error = token_result$error,
      needs_auth = TRUE
    ))
  }

  tryCatch({
    # Attempt connection with OAuth token
    con <- dbConnect(
      odbc::snowflake(),
      account = SNOWFLAKE_ACCOUNT,
      warehouse = SNOWFLAKE_WAREHOUSE,
      database = SNOWFLAKE_DATABASE,
      schema = SNOWFLAKE_SCHEMA,
      authenticator = "oauth",
      token = token_result$access_token
    )

    # Test the connection with a simple query
    result <- dbGetQuery(con, "SELECT CURRENT_USER() as user, CURRENT_ROLE() as role, CURRENT_DATABASE() as database")

    dbDisconnect(con)

    return(list(
      success = TRUE,
      data = result,
      message = "Successfully connected to Snowflake!"
    ))

  }, error = function(e) {
    return(list(
      success = FALSE,
      error = paste("Connection error:", e$message),
      needs_auth = grepl("token|auth", tolower(e$message))
    ))
  })
}

# Function to query Snowflake tables
query_snowflake <- function(session, query) {
  # Get OAuth token from Connect
  token_result <- get_oauth_token(session)

  if (!token_result$success) {
    return(list(
      success = FALSE,
      error = token_result$error
    ))
  }

  tryCatch({
    con <- dbConnect(
      odbc::snowflake(),
      account = SNOWFLAKE_ACCOUNT,
      warehouse = SNOWFLAKE_WAREHOUSE,
      database = SNOWFLAKE_DATABASE,
      schema = SNOWFLAKE_SCHEMA,
      authenticator = "oauth",
      token = token_result$access_token
    )

    result <- dbGetQuery(con, query)
    dbDisconnect(con)

    return(list(
      success = TRUE,
      data = result
    ))

  }, error = function(e) {
    return(list(
      success = FALSE,
      error = paste("Query error:", e$message)
    ))
  })
}

# UI Definition
ui <- page_sidebar(
  title = "Snowflake OAuth Demo",
  theme = bs_theme(version = 5, bootswatch = "cosmo"),

  sidebar = sidebar(
    h4("Connection Info"),
    tags$dl(
      tags$dt("Account ID:"),
      tags$dd(SNOWFLAKE_ACCOUNT),
      tags$dt("Database:"),
      tags$dd(SNOWFLAKE_DATABASE),
      tags$dt("Warehouse:"),
      tags$dd(SNOWFLAKE_WAREHOUSE)
    ),
    hr(),
    actionButton("test_connection", "Test Connection",
                 class = "btn-primary w-100",
                 icon = icon("plug")),
    hr(),
    p("This app uses Posit Connect's OAuth Integration to authenticate with Snowflake.",
      class = "text-muted small"),
    p("If you haven't logged in yet, Connect will show a modal prompting you to authenticate.",
      class = "text-muted small")
  ),

  # Main content
  layout_columns(
    col_widths = c(12, 12, 12),

    # Connection status card
    card(
      card_header("Connection Status"),
      uiOutput("connection_status")
    ),

    # Query interface
    card(
      card_header("Query Snowflake"),
      textAreaInput("sql_query",
                    "SQL Query:",
                    value = "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 10",
                    rows = 4,
                    width = "100%"),
      actionButton("run_query", "Run Query", class = "btn-success", icon = icon("play")),
      hr(),
      uiOutput("query_results")
    ),

    # Example queries
    card(
      card_header("Example Queries"),
      actionButton("query_tables", "List Tables", class = "btn-sm btn-outline-primary"),
      actionButton("query_user", "Current User Info", class = "btn-sm btn-outline-primary"),
      actionButton("query_databases", "List Databases", class = "btn-sm btn-outline-primary")
    )
  )
)

# Server Logic
server <- function(input, output, session) {

  # Reactive value to store connection status
  connection_status <- reactiveVal(NULL)
  query_result <- reactiveVal(NULL)

  # Test connection when button is clicked
  observeEvent(input$test_connection, {
    result <- connect_to_snowflake(session)
    connection_status(result)
  })

  # Run custom query
  observeEvent(input$run_query, {
    req(input$sql_query)
    result <- query_snowflake(session, input$sql_query)
    query_result(result)
  })

  # Example query: List tables
  observeEvent(input$query_tables, {
    query <- "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
              FROM INFORMATION_SCHEMA.TABLES
              LIMIT 20"
    updateTextAreaInput(session, "sql_query", value = query)
  })

  # Example query: Current user info
  observeEvent(input$query_user, {
    query <- "SELECT CURRENT_USER() as user,
                     CURRENT_ROLE() as role,
                     CURRENT_DATABASE() as database,
                     CURRENT_WAREHOUSE() as warehouse"
    updateTextAreaInput(session, "sql_query", value = query)
  })

  # Example query: List databases
  observeEvent(input$query_databases, {
    query <- "SELECT DATABASE_NAME, CREATED
              FROM INFORMATION_SCHEMA.DATABASES"
    updateTextAreaInput(session, "sql_query", value = query)
  })

  # Render connection status
  output$connection_status <- renderUI({
    status <- connection_status()

    if (is.null(status)) {
      return(
        div(
          class = "alert alert-info",
          icon("info-circle"),
          " Click 'Test Connection' to check OAuth authentication status."
        )
      )
    }

    if (status$success) {
      tagList(
        div(
          class = "alert alert-success",
          icon("check-circle"),
          strong(" Success! "), status$message
        ),
        if (!is.null(status$data)) {
          tagList(
            h5("Connection Details:"),
            tags$table(
              class = "table table-sm",
              tags$thead(
                tags$tr(
                  lapply(names(status$data), function(col) tags$th(col))
                )
              ),
              tags$tbody(
                tags$tr(
                  lapply(status$data, function(val) tags$td(as.character(val)))
                )
              )
            )
          )
        }
      )
    } else {
      div(
        class = "alert alert-danger",
        icon("exclamation-triangle"),
        strong(" Error: "), status$error,
        if (isTRUE(status$needs_auth)) {
          tagList(
            hr(),
            p("Go to Settings > Access > Integrations to log in with Snowflake.")
          )
        }
      )
    }
  })

  # Render query results
  output$query_results <- renderUI({
    result <- query_result()

    if (is.null(result)) {
      return(
        p("Enter a SQL query and click 'Run Query'.", class = "text-muted")
      )
    }

    if (result$success) {
      if (nrow(result$data) == 0) {
        return(
          div(class = "alert alert-warning", "Query returned no results.")
        )
      }

      tagList(
        div(
          class = "alert alert-success",
          icon("check"),
          sprintf(" Query successful! Returned %d row(s).", nrow(result$data))
        ),
        div(
          style = "max-height: 400px; overflow-y: auto;",
          renderTable(result$data, striped = TRUE, hover = TRUE)
        )
      )
    } else {
      div(
        class = "alert alert-danger",
        icon("exclamation-triangle"),
        strong(" Query failed: "), result$error
      )
    }
  })

  # Auto-test connection on startup
  observe({
    # Wait a moment for the app to load
    invalidateLater(1000)
    isolate({
      if (is.null(connection_status())) {
        result <- connect_to_snowflake(session)
        connection_status(result)
      }
    })
  })
}

# Run the application
shinyApp(ui = ui, server = server)
