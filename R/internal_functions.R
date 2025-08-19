#' Make a string safe for filenames
#' 
#' @description Sanitise a string for use as a filename by replacing non-alphanumerics (except `_` and `-`) with `_` and dropping a trailing `.rds`.
#' @param x Input filename or label.
#' @return A sanitized filename-safe string without a trailing `.rds`.
#' @noRd
clean_filename <- function(x) gsub("[^A-Za-z0-9_-]", "_", gsub("\\.rds$", "", x))

#' Parse vector-like input into a numeric vector
#' @param x A numeric vector, list, or character string representing numbers.
#' @return Numeric vector
#' @noRd
parse_ts_vec <- function(x) {
  if (is.null(x) || length(x) == 0) return(numeric(0))
  if (is.list(x)) x <- unlist(x, recursive = TRUE, use.names = FALSE)
  if (is.numeric(x)) return(as.numeric(x))
  if (is.character(x)) {
    s <- x[1]
    s <- gsub("^\\s*c?\\(|\\)\\s*$", "", s)   # strip c(...)
    s <- gsub("^\\s*\\[|\\]\\s*$", "", s)     # strip [...]
    parts <- strsplit(s, "[,\\s]+")[[1]]
    parts <- parts[nzchar(parts)]
    vals <- suppressWarnings(as.numeric(parts))
    return(vals[!is.na(vals)])
  }
  numeric(0)
}

#' Convert timesteps to years within an optional window
#' @param v Numeric vector of timesteps.
#' @param days_per_year Days per year (default option MINTed.days_per_year or 365).
#' @param start_timestep,end_timestep Optional inclusive window (options or -Inf/Inf).
#' @return Numeric vector of years
#' @noRd
to_years_in_window <- function(
  v,
  days_per_year = getOption("MINTed.days_per_year", 365),
  start_timestep = getOption("MINTed.start_timestep", -Inf),
  end_timestep   = getOption("MINTed.end_timestep",   Inf)
) {
  if (length(v) == 0) return(numeric(0))
  v <- v[v >= start_timestep & v <= end_timestep]
  v / days_per_year
}

#' Y-position for a label row
#' @param rank Integer or numeric rank (1 = highest row).
#' @param ylim Optional c(ymin, ymax); defaults to current plot via par("usr").
#' @return Numeric y coordinate
#' @noRd
y_row <- function(rank, ylim = NULL) {
  if (is.null(ylim)) {
    usr <- par("usr")    # c(xmin, xmax, ymin, ymax)
    ylim <- usr[3:4]
  }
  y_min <- ylim[1]; y_max <- ylim[2]
  y_max - rank * 0.06 * (y_max - y_min)
}

#' Draw vertical markers and labels on a plot
#' @param xs Numeric vector of x-positions.
#' @param col Color for lines/labels.
#' @param label Character label.
#' @param rank Passed to y_row() to stagger labels.
#' @param xlim,ylim Optional axis limits; default to par("usr").
#' @return Invisibly, the x positions drawn.
#' @noRd
draw_vmarkers <- function(xs, col, label, rank, xlim = NULL, ylim = NULL) {
  if (!length(xs)) return(invisible(numeric(0)))
  if (is.null(xlim) || is.null(ylim)) {
    usr <- par("usr")               # c(xmin, xmax, ymin, ymax)
    if (is.null(xlim)) xlim <- usr[1:2]
    if (is.null(ylim)) ylim <- usr[3:4]
  }
  xs <- xs[xs >= xlim[1] & xs <= xlim[2]]
  if (!length(xs)) return(invisible(numeric(0)))

  y_lab <- y_row(rank, ylim = ylim)
  for (xv in xs) {
    abline(v = xv, lty = 2, lwd = 3, col = col)
    text(x = xv, y = y_lab, labels = label, cex = 0.75, col = col, srt = 0, xpd = NA, adj = c(0.5, 0.5))
  }
  invisible(xs)
}

#' Quote SQL identifiers with DBI
#' @param x Character vector of identifier names to quote.
#' @param con A DBI connection.
#' @return Character vector of quoted identifiers.
#' @noRd
qi <- function(x, con) {
  if (!inherits(con, "DBIConnection")) stop("`con` must be a DBIConnection.", call. = FALSE)
  vapply(x, function(n) as.character(dbQuoteIdentifier(con, n)), character(1))
}

#' Build a SQL predicate from a filter value
#' @param col Column name (unquoted).
#' @param val Filter value (logical; numeric scalar/range/vector; character scalar/vector).
#' @param con A DBI connection.
#' @return SQL predicate string or NULL if `val` is NULL.
#' @noRd
build_pred <- function(col, val, con) {
  if (is.null(val)) return(NULL)
  if (!inherits(con, "DBIConnection")) stop("`con` must be a DBIConnection.", call. = FALSE)

  col_sql <- qi(col, con)

  if (is.logical(val) && length(val) == 1L) {
    return(sprintf("%s = %s", col_sql, if (val) "TRUE" else "FALSE"))
  }

  if (is.numeric(val)) {
    if (length(val) == 1L) {
      return(sprintf("%s = %s", col_sql, format(val, scientific = FALSE)))
    } else if (length(val) == 2L) {
      lo <- min(val); hi <- max(val)
      return(sprintf("%s BETWEEN %s AND %s", col_sql,
                     format(lo, scientific = FALSE),
                     format(hi, scientific = FALSE)))
    } else {
      vals <- paste(format(val, scientific = FALSE), collapse = ", ")
      return(sprintf("%s IN (%s)", col_sql, vals))
    }
  }

  if (is.character(val)) {
    if (length(val) == 1L) {
      return(sprintf("%s = %s", col_sql, as.character(dbQuoteString(con, val))))
    } else {
      qv <- paste(vapply(val, function(s) as.character(dbQuoteString(con, s)), character(1)),
                  collapse = ", ")
      return(sprintf("%s IN (%s)", col_sql, qv))
    }
  }

  warning(sprintf("Filter for column '%s' is a complex type; attempting string equality.", col))
  sprintf("CAST(%s AS TEXT) = %s", col_sql, as.character(dbQuoteString(con, as.character(val))))
}

#' Build a concise or expanded panel title for prevalence plots
#'
#' Constructs a one- or two-line string beginning with "Prevalence over time".
#' In short mode (default), it appends the first elements of `parameter_index`
#' and `global_index`. In full mode, it prints name–value pairs for selected
#' fields from `meta`, formatting non-index fields with [fmt3()].
#'
#' @param meta A `data.frame`/list-like object containing at least
#'   `parameter_index` and `global_index`. If present, the following fields are
#'   also included in the expanded output (in this order):
#'   `eir`, `Q0`, `phi_bednets`, `seasonal`, `routine`, `dn0_use`, `dn0_future`,
#'   `itn_use`, `irs_use`, `itn_future`, `irs_future`, `lsm`.
#'   Only the first element of each field is used.
#' @param full_name Logical. If `FALSE` (default), return a short single-line
#'   string with indices only. If `TRUE`, include all available fields and wrap
#'   onto a second line after `wrap_after` pairs.
#' @param wrap_after Integer or `NULL`. Number of name–value pairs to include on
#'   the first line when `full_name = TRUE`. If `NULL` (default), wraps after
#'   `ceiling(n/2)` to force two lines where `n` is the number of included pairs.
#'
#' @return A length-1 character string suitable for use as a plot title/subtitle.
#'
#' @details
#' Non-index fields are formatted via [fmt3()] (3 significant figures for
#' numerics; `"TRUE"`/`"FALSE"`/`"NA"` for logicals; `as.character()` otherwise).
#'
#' @noRd
make_main <- function(meta, full_name = FALSE, wrap_after = NULL) {
  short <- sprintf(
    "Prevalence over time\nParameter Index: %s | Global Index: %s",
    as.character(meta$parameter_index[1]),
    as.character(meta$global_index[1])
  )
  if (!full_name) return(short)

  fields <- c(
    "parameter_index","global_index",
    "eir","Q0","phi_bednets","seasonal","routine",
    "dn0_use","dn0_future","itn_use","irs_use",
    "itn_future","irs_future","lsm"
  )
  fields <- intersect(fields, names(meta))

  pairs <- vapply(fields, function(f) {
    val_str <- if (f %in% c("parameter_index","global_index")) {
      as.character(meta[[f]][1])
    } else {
      fmt3(meta[[f]])
    }
    paste0(f, ": ", val_str)
  }, FUN.VALUE = character(1))

  n <- length(pairs)
  if (is.null(wrap_after)) wrap_after <- ceiling(n / 2)  # force 2 lines
  line1 <- paste(pairs[seq_len(min(wrap_after, n))], collapse = " | ")
  line2 <- if (n > wrap_after) paste(pairs[(wrap_after + 1):n], collapse = " | ") else ""

  paste0("Prevalence over time\n", line1, if (nzchar(line2)) paste0("\n", line2) else "")
}

#' Format a value with 3 significant figures (utility)
#'
#' Returns a character string for the first element of `v`:
#' - If empty, `NULL`, or `NA`, returns `"NA"`.
#' - If numeric, formats with 3 significant figures using `formatC(..., format = "g")`.
#' - If logical, returns `"TRUE"`, `"FALSE"`, or `"NA"`.
#' - Otherwise, coerces with `as.character()`.
#'
#' @param v A vector (numeric, logical, character, etc.). Only `v[1]` is used.
#'
#' @return A length-1 character string.
#'
#' @noRd
fmt3 <- function(v) {
  if (length(v) < 1 || is.null(v) || is.na(v[1])) return("NA")
    x <- v[1]
    if (is.numeric(x)) {
      return(formatC(x, digits = 3, format = "g"))  # 3 significant figs
    } else if (is.logical(x)) {
      return(ifelse(is.na(x), "NA", ifelse(x, "TRUE", "FALSE")))
    } else {
      return(as.character(x))
    }
}
