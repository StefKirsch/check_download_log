#' Read Download Log from XLSX File
#'
#' This function reads the download log from an xlsx file based on hard-coded wave names.
#' The wave names are used to access the individual sheets of the xlsx file.
#' cleans the column names, and returns a named list of data frames, one per sheet/wave name.
#'
#' @param path_to_download_log Character. The file path to the download log in xlsx format.
#'
#' @return A named list of data frames, where each data frame corresponds to the wave name in the download log.
#'
#' @importFrom readxl read_xlsx
#' @importFrom janitor clean_names
#'
#' @export
read_download_log <- function(path_to_download_log) {

  wave_names_in_download_log <- list(
    "VLF" = "LF",
    "VLP" = "VITA+ LF",
    "VMM" = "MM",
    "VMP" = "VITA+ MM"
  )

  download_log <- lapply(
    names(wave_names_in_download_log),
    function(sheet_name) {
      readxl::read_xlsx(
        path = path_to_download_log,
        sheet = wave_names_in_download_log[[sheet_name]],
        na = c( # all values that are considered NA and will be ignore for type guessing
          "x",
          "datum open gezet",
          "nog even naar kijken",
          "Gedownload op nieuwe manier",
          "nog niet geautoriseerd",
          "gestopt met fitbit",
          "Gestopt",
          "open gezet",
          "staat niet in MinIO",
          "overleden",
          "below add any other data values that should not be considered..."
        ),
        .name_repair = "unique_quiet"
      ) |>
        janitor::clean_names() |> # removes spaces in column names
        rename(study.number = studienummer) |> # translate studienummer
        rename_with( # translate eind to end
          .fn = ~ gsub(
            pattern = "eind",
            replacement = "end",
            x = .x
          ),
          .cols = ends_with("_eind")
        ) |>
        rename_with( # dot notation for column names
          .fn = ~gsub(
            pattern = "_",
            replacement = ".",
            x = .x
          )
        ) |>
        mutate(across( # convert everything to date.
          -study.number,
          ~ as.Date(.x, optional = TRUE) # return na if no date can be found
        )) |>
        janitor::remove_empty(which = c("rows", "cols")) |>  # remove all NA rows and columns
        rename_with(tolower)
    }
  ) |>
    setNames(names(wave_names_in_download_log)) |> # set column names
    bind_rows() |>
    arrange(study.number) |>
    add_spans() |>  # add wave duration spans
    assert_download_log() # run assertive test suite

  return(download_log)
}


#' Add Wave Label Column to fitbit dataframe
#'
#' This function takes a data frame containing fitbit observations (`df_data`) and another data frame
#' containing wave information (`df_waves`), and adds a new column to `df_data` that indicates the
#' wave ID for each observation based on its datetime. The wave ID is determined by the
#' `assign_wave_id` function.
#'
#' @param df_data A data frame containing the observations with a 'Datetime' column and a 'study.number'
#'                column.
#' @param df_waves A data frame containing wave start and end times, with a 'study.number' column.
#'
#' @return A data frame identical to `df_data` but with an additional 'wave.id' column indicating
#'         the wave ID for each observation.
#'
#' @details The function currently uses a row-wise operation which might be less efficient on very
#'          large datasets. Future implementations might improve efficiency by replacing the start
#'          and end columns with logical values to avoid the need for row-wise operations.
#'
#' @examples
#' df_data_with_wave_id <- add_wave_label_col(df_data, df_waves)
#'
#' @export
#' @importFrom dplyr left_join rowwise mutate ungroup
#' @seealso \code{\link{assign_wave_id}}
add_wave_label_col <- function(df_data, df_waves, rm_outside_wave = TRUE) {

  # short-circuit the function if df_waves is NULL
  # This tells the function that there is no wave information to be processed

  if (!is.null(df_waves)) {

    message(paste0(Sys.time(),": Labeling waves..."))

    start_end_pattern = "w\\d+\\.(start|end)"

    df_data <- df_data |>
      left_join(df_waves, by = "study.number") |> # Joining tables, keeping all observations in df_data
      mutate(across( # slow, because it's probably not vectorized
        .cols = matches("span."),
        .fn = ~ Datetime %within% .x
      )) |>
      spans_to_in_week() |> # overwrite spans columns with wave ID or NA
      mutate( # squeeze wave.id from the 5 columns, only keep the ones that are not missing
        wave.id = coacross(matches("span.")), # custom function that combines coalesce with across as a workaround, see function description
        .after = study.number
      ) |>
      select(-contains("span")) |> # remove temporary helper columns
      replace_na(list(wave.id = 0)) |>
      mutate(
        wave_start_name_temp = paste0("w", wave.id, ".start"), # lookup column start
        wave_end_name_temp = paste0("w", wave.id, ".end"), # lookup column end
      )

    # fast base R solution to lookup value of wX.start and wX.end from another column
    # c.f. https://stackoverflow.com/questions/67678405/r-lookup-values-of-a-column-defined-by-another-columns-values-in-mutate
    df_data_wo_lookup_vars <- df_data |>
      as.data.frame() |> # not tibble!
      select(-c(wave_start_name_temp, wave_end_name_temp))

    df_data_wo_lookup_vars$wave.start <- df_data_wo_lookup_vars[cbind(
      seq_len(nrow(df_data_wo_lookup_vars)), # row index
      match(df_data$wave_start_name_temp, names(df_data_wo_lookup_vars)) # column index
    )]

    df_data_wo_lookup_vars$wave.end <- df_data_wo_lookup_vars[cbind(
      seq_len(nrow(df_data_wo_lookup_vars)), # row index
      match(df_data$wave_end_name_temp, names(df_data_wo_lookup_vars)) # column index
    )]

    # a bit of final housekeeping...
    df_data_wo_lookup_vars <- df_data_wo_lookup_vars |>
      select(-matches(start_end_pattern)) |>  # remove temporary helper columns
      select(study.number, wave.id, wave.start, wave.end, everything()) |>  # reorder columns
      mutate( # add wave duration
        wave.duration.mins = difftime(wave.end, wave.start, units = "mins")
      )

    if (rm_outside_wave) {
      df_data_wo_lookup_vars <- df_data_wo_lookup_vars |> filter(wave.id > 0)
    }

    message(paste0(Sys.time(),": Done"))

    return(df_data_wo_lookup_vars)
  } else {
    # short-circuit
    return(df_data)
  }

}


# Add span column form start and end columns
add_spans <- function(downLoad_log) {

  for (i in seq(1, 5)) {
    span_col <- paste0("span.w",i)
    start_col <- paste0("w",i,".start")
    end_col <- paste0("w",i,".end")

    # add intervals
    # skip intervals where we don't have both a start and end date
    if (start_col %in% colnames(downLoad_log) & end_col %in% colnames(downLoad_log)) {
      downLoad_log <- downLoad_log |>
        mutate(
          # sym seems to be needed
          !!span_col := interval(!!sym(start_col), !!sym(end_col)),
          .keep = "all"
        )
    }
  }

  return(downLoad_log)
}


# Overwrites the spans columns added by add_spans()
# If the observation falls in the span, it's replaced with the wave id
# if not, the value is set to NA
spans_to_in_week <- function(downLoad_log) {

  for (i in seq(1, 5)) {
    span_col <- paste0("span.w",i)

    # skip intervals that don' exist
    if (span_col %in% colnames(downLoad_log)) {
      downLoad_log <- downLoad_log |>
        mutate(
          !!span_col := if_else(!!sym(span_col), i, NA),
          .keep = "all"
        )
    }
  }

  return(downLoad_log)
}


# Workaround for problem with coalesce and across
# c.f. https://github.com/tidyverse/funs/issues/54
coacross <- function(...) {
  coalesce(!!!across(...))
}


# Get actual download log by identifying periods where the participants
# wore the fitbit the most within the date ranges given in the original
# download log.
# The desired period to look for the densest data is `wave_duration_days`
# Runs get_optimal_wave_dates_per_group() per study_number of a dataframe
# that is sampled per day.
# Then cleans up the result to get a new_download log in the same format
# as the original one that can then be used
get_actual_download_log <- function(df_daily) {

  wave_duration_days <- 14

  df_daily |>
    group_by(study.number, wave.id) %>%
    # get optimal wave dates BY GROUP
    # this requires that the groups are passed into get_optimal_wave_dates()
    # in sequence
    # Just calling the function would not do the operation by group
    # ind of weird that this function is called "do()"
    # It is somewhat deprecated, but the alternatives seems excessively clunky
    do(get_optimal_wave_dates_per_group(., wave_duration_days)) |>
    ungroup() |>
    # Do some manipulation to restore the original download_log format
    # Note: All wave ranges that were missing in daily_steps_hr
    # turn up as NA now.
    # arrange by wave.id so that the date columns end up in the right order
    arrange(
      wave.id
    ) |>
    # concatenate wave ids to naming to reflect what we got from original download log
    mutate(
      wave.id = paste0("w", wave.id)  # Prefix wave.id with 'w' for better column naming
    ) |>
    pivot_wider(
      names_from = wave.id,
      values_from = c(wave.start.actual, wave.end.actual),
      names_sep = " " # Separate the wave id prefix and date type with a space for clarity
    ) %>%
    # rename start dates
    rename_with(
      ~ gsub(
        pattern = "wave.start.actual w([1-9])",
        replacement = "w\\1.start",
        x = .
      ),
      starts_with("wave.start.actual")
    ) %>%
    # rename end dates
    rename_with(
      ~ gsub(
        pattern = "wave.end.actual w([1-9])",
        replacement = "w\\1.end",
        x = .
      ),
      starts_with("wave.end.actual")
    ) |>
    arrange(study.number) |>
    add_spans() # add wave duration spans

}


# detect the date windows where the most data was found
# intended to be called per group with `do()`
get_optimal_wave_dates_per_group <- function(df, wave_duration_days) {
  # function to get all 14-day periods with maximum `n.value.`

  start_date <- df$wave.start[1]
  end_date <- df$wave.end[1]

  # account for the fact that some waves are shorter than wave_duration_days
  latest_start_date <- if (end_date - start_date >= days(wave_duration_days - 1)) {
    start_date + days(wave_duration_days - 1)
  } else {
    end_date
  }

  all_possible_starts <- seq(
    from = start_date,
    to = latest_start_date,
    by = "day"
  )

  # Calculate sum of `n.value.` for each 14-day period
  # sapply loops over every element in all_possible_starts and
  # per start date returns the sum of recorded observations for the
  # following 2 week period
  period_sums <- sapply(all_possible_starts, function(start_date) {
    df |>
      # filter for candidate period
      filter(Date >= start_date & Date <= start_date + days(wave_duration_days) - 1) |>
      # select only number of observations
      select(starts_with("n.value.")) |>
      # Calculate sum of observations
      sum()
  })

  # Identify the period with the maximum `n.value.`
  max_periods <- which.max(period_sums)
  start_date_max <- all_possible_starts[max_periods]

  # Return the start and end dates of the optimal period
  tibble(
    wave.start.actual = start_date_max,
    wave.end.actual = start_date_max + days(wave_duration_days - 1)
  )
}


assert_download_log <- function(downLoad_log) {
  max_wave_length_weeks <- 4
  max_wave_length_seconds <- 4 * 7 * 24 * 60 * 60

  # Do some checks
  assertr_errors <- downLoad_log |>
    # Check that every study number is only mentioned once
    assertr::assert(
      assertr::is_uniq,
      study.number,
      error_fun = assertr::error_append,
      description = "One or more study numbers are mentioned more than once."
    ) |>
    # Check for reverse intervals, i.e. where the start date happens after the end date
    assertr::assert(
      predicate = assertr::within_bounds(0,Inf),
      starts_with("span.w"),
      error_fun = assertr::error_append,
      description = "The wave(s) start date is after the wave end date."
    ) |>
    # Check for waves that last longer than three weeks
    assertr::assert(
      predicate = assertr::within_bounds(0, max_wave_length_seconds),
      starts_with("span.w"),
      error_fun = assertr::error_append,
      description = paste(
        "One or more waves seem to be longer than",
        max_wave_length_weeks,
        "weeks."
      )
    ) |>
    attr("assertr_errors")

  message_text <- character()

  for (assertr_error in assertr_errors) {
    index <- assertr_error$error_df$index
    study_number <- paste(unique(downLoad_log$study.number[index]), collapse = ", " )
    column <- assertr_error$error_df$column
    description <- assertr_error$description

    message_text <- glue::glue(paste(
      "Warning:",
      "Assertion failed for study number",
      "{study_number}",
      "in column",
      "\"{column}\"",
      "- ",
      "{description}",
      "\n\n"
    )) |> unique() # message is repeated per conflicting row

    message(message_text)
  }

  if (length(message_text)) {
    stop("The download log excel sheet failed at least one test. Please correct them first.")
  }

  return(downLoad_log)
}