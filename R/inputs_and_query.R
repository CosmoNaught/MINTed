#' Fetch static input parameters for a given parameter/global index
#'
#' @param raw_db_path Path to the .duckdb file. Required if con is not provided.
#' @param con Optional DuckDB connection object. If provided, raw_db_path is ignored.
#' @param parameter_index Integer (optional). One of parameter_index OR global_index must be provided.
#' @param global_index Character (optional). One of parameter_index OR global_index must be provided.
#' @param table_name Table name. Default "simulation_results".
#'
#' @return One-row data.frame with the static inputs.
#' @export
fetch_inputs <- function(con = NULL,
                        raw_db_path = NULL,
                        parameter_index = NULL,
                        global_index = NULL,
                        table_name = "simulation_results") {
  
  # Use spearMINT connection handler
  conn_info <- get_duck_connection(con, raw_db_path, read_only = TRUE)
  con <- conn_info$con
  
  if (conn_info$should_close) {
    on.exit(dbDisconnect(con), add = TRUE)
  }
  
  if (!dbExistsTable(con, table_name)) {
    stop(sprintf("Table '%s' not found in database", table_name))
  }
  
  # Use spearMINT key clause builder
  key_clause <- build_key_clause(con, parameter_index, global_index)
  
  inputs <- c(
    "eir","Q0","phi_bednets","seasonal","routine",
    "dn0_use","dn0_future","itn_use","irs_use",
    "itn_future","irs_future","lsm"
  )
  
  sel <- paste(vapply(inputs, \(n) as.character(dbQuoteIdentifier(con, n)), character(1)),
               collapse = ", ")
  
  sql <- sprintf("
    SELECT DISTINCT %s
    FROM %s
    WHERE %s
    LIMIT 1
  ", sel, as.character(dbQuoteIdentifier(con, table_name)), key_clause)
  
  out <- dbGetQuery(con, sql)
  if (nrow(out) == 0) stop("No data found for the requested index.")
  
  out
}

#' Query static inputs (plus indices) using simple R filters
#'
#' @param raw_db_path Path to the .duckdb file. Required if con is not provided.
#' @param con Optional DuckDB connection object. If provided, raw_db_path is ignored.
#' @param filters Named list of filters where names are column names and values are:
#'   - scalar (numeric/character/logical) -> equality
#'   - numeric vector of length 2 -> inclusive range \code{c(lower, upper)} (e.g., \code{eir = c(30, 50)})
#'   - character vector (length >= 1) -> IN (...)
#'   Multiple filters are AND-ed together.
#'   Examples:
#'     list(parameter_index = 3)
#'     list(global_index = c("fileA.rds","fileB.rds"))
#'     list(prevalence = c(0.1, 0.2), timesteps = c(6*365, 12*365))
#' @param limit Optional integer LIMIT for previewing results.
#' @param table_name Table to query. Default "simulation_results".
#'
#' @return A data.frame with DISTINCT rows of:
#'   parameter_index, global_index, eir, Q0, phi_bednets, seasonal, routine,
#'   dn0_use, dn0_future, itn_use, irs_use, itn_future, irs_future, lsm
#' @export
query_database <- function(con = NULL,
                           raw_db_path = NULL,
                           filters = list(),
                           limit = NULL,
                           table_name = "simulation_results") {

  if (is.null(con)) {
    if (is.null(raw_db_path)) stop("Either con or raw_db_path must be provided")
    stopifnot(file.exists(raw_db_path))
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = raw_db_path, read_only = TRUE)
    close_con <- TRUE
  } else {
    close_con <- FALSE
  }
  if (close_con) on.exit(DBI::dbDisconnect(con), add = TRUE)

  if (!DBI::dbExistsTable(con, table_name)) {
    stop(sprintf("Table '%s' not found in database", table_name))
  }

  inputs <- c("eir","Q0","phi_bednets","seasonal","routine",
              "dn0_use","dn0_future","itn_use","irs_use",
              "itn_future","irs_future","lsm")
  return_cols <- c("parameter_index", "global_index", inputs)

  preds <- unlist(mapply(
    build_pred,
    names(filters),
    filters,
    MoreArgs = list(con = con),
    SIMPLIFY = TRUE,
    USE.NAMES = FALSE
  ))
  where_clause <- if (length(preds)) paste("WHERE", paste(preds, collapse = " AND ")) else ""

  sel <- paste(as.character(DBI::dbQuoteIdentifier(con, return_cols)), collapse = ", ")
  limit_sql <- if (!is.null(limit)) sprintf(" LIMIT %d", as.integer(limit)) else ""

  sql <- sprintf("SELECT DISTINCT %s FROM %s %s%s",
                 sel,
                 as.character(DBI::dbQuoteIdentifier(con, table_name)),
                 where_clause,
                 limit_sql)

  DBI::dbGetQuery(con, sql)
}
