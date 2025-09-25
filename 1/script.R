req_pkgs <- c(
  "readr",
  "dplyr",
  "tidyr",
  "stringr",
  "ggplot2",
  "forcats",
  "purrr",
  "tibble"
)
to_install <- setdiff(req_pkgs, rownames(installed.packages()))
if (length(to_install) > 0) {
  install.packages(to_install, repos = "https://cloud.r-project.org")
}
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(ggplot2)
  library(forcats)
  library(purrr)
  library(tibble)
})

parse_usd <- function(x) {
  if (is.numeric(x)) {
    return(x)
  }
  as.numeric(gsub("[^0-9.]", "", x))
}

usd_to_rub <- function(usd, rate = 80) {
  as.numeric(usd) * rate
}

fmt_rub <- function(x, decimal.mark = ".", big.mark = " ") {
  out <- format(
    round(as.numeric(x), 2),
    nsmall = 2,
    big.mark = big.mark,
    decimal.mark = decimal.mark,
    trim = TRUE
  )
  paste0(out, " RUB")
}

read_clean_cars <- function(path_csv, encoding = "UTF-8") {
  df <- suppressMessages(readr::read_csv(
    path_csv,
    locale = readr::locale(encoding = encoding),
    show_col_types = FALSE
  ))
  if (!all(c("Make", "Type", "Origin", "Invoice") %in% names(df))) {
    stop("CARS.csv must contain at least columns: Make, Type, Origin, Invoice")
  }
  df <- df %>%
    mutate(
      InvoiceUSD = parse_usd(Invoice),
      MSRPUSD = if ("MSRP" %in% names(.)) parse_usd(MSRP) else NA_real_
    )
  as.data.frame(df)
}

make_blocks <- function(df, all_types, rate = 80, sep = ", ") {
  df <- as.data.frame(df)
  makers <- df %>% distinct(Make, Origin) %>% arrange(Make, Origin)

  blocks <- purrr::pmap_chr(
    list(makers$Make, makers$Origin),
    function(mk, org) {
      subdf <- df %>% filter(Make == mk, Origin == org)

      cnt <- subdf %>%
        count(Type, name = "n") %>%
        right_join(tibble(Type = all_types), by = "Type") %>%
        mutate(n = ifelse(is.na(n), 0L, n)) %>%
        arrange(match(Type, all_types))

      mx <- subdf %>%
        group_by(Type) %>%
        summarise(
          max_inv_usd = suppressWarnings(max(InvoiceUSD, na.rm = TRUE)),
          .groups = "drop"
        ) %>%
        mutate(
          max_inv_usd = ifelse(is.infinite(max_inv_usd), NA_real_, max_inv_usd)
        ) %>%
        right_join(tibble(Type = all_types), by = "Type") %>%
        mutate(
          max_inv_rub = ifelse(
            is.na(max_inv_usd),
            0,
            usd_to_rub(max_inv_usd, rate)
          )
        ) %>%
        arrange(match(Type, all_types))

      header <- sprintf("%s[%s]", mk, org)
      line2 <- paste(sprintf("%s=%d", mx$Type, cnt$n), collapse = sep)
      line3 <- paste(
        sprintf("%s=%s", mx$Type, fmt_rub(mx$max_inv_rub)),
        collapse = sep
      )
      paste(header, line2, line3, sep = "\n")
    }
  )
  blocks
}

write_step1 <- function(obj, path_txt = "cars_step1.txt", sep = ", ") {
  if (!inherits(obj, "cars_report")) {
    stop("write_step1 expects a 'cars_report' object")
  }
  all_types <- attr(obj, "all_types")
  rate <- attr(obj, "rate")
  df <- as.data.frame(obj)
  blocks <- make_blocks(df, all_types = all_types, rate = rate, sep = sep)
  txt <- paste(blocks, collapse = "\n\n")
  writeLines(txt, path_txt, useBytes = TRUE)
  attr(obj, "step1_path") <- path_txt
  obj
}

parse_kv_line <- function(line, value_is_money = FALSE) {
  parts <- strsplit(line, ",")[[1]]
  parts <- trimws(parts)
  kv <- strsplit(parts, "=", fixed = TRUE)
  types <- vapply(kv, `[[`, character(1), 1)
  vals <- vapply(kv, function(x) paste(x[-1], collapse = "="), character(1))
  if (value_is_money) {
    vals_num <- as.numeric(gsub("[^0-9.]", "", vals))
  } else {
    vals_num <- suppressWarnings(as.numeric(vals))
  }
  names(vals_num) <- types
  vals_num
}

read_step1_to_long <- function(path_txt, all_types) {
  lines <- readLines(path_txt, warn = FALSE, encoding = "UTF-8")

  is_blank <- trimws(lines) == ""
  grp <- cumsum(c(0, head(is_blank, -1) & !tail(is_blank, -1)))
  blocks_raw <- split(lines, grp)

  norm_blocks <- lapply(blocks_raw, function(v) trimws(v[nzchar(trimws(v))]))
  norm_blocks <- Filter(function(v) length(v) >= 3, norm_blocks)

  parsed <- purrr::map_dfr(norm_blocks, function(v) {
    header <- v[1]
    line2 <- v[2]
    line3 <- v[3]
    m <- regmatches(header, regexec("^(.*)\\[(.*)\\]$", header))[[1]]
    if (length(m) != 3) {
      return(tibble())
    }
    make <- m[2]
    origin <- m[3]

    cnt <- parse_kv_line(line2, value_is_money = FALSE)
    mxr <- parse_kv_line(line3, value_is_money = TRUE)

    cnt <- cnt[all_types]
    cnt[is.na(cnt)] <- 0
    mxr <- mxr[all_types]
    mxr[is.na(mxr)] <- 0

    tibble(
      Make = make,
      Origin = origin,
      Type = all_types,
      Count = as.integer(cnt),
      MaxInvoiceRUB = as.numeric(mxr)
    )
  })

  as.data.frame(parsed)
}

build_step2_pivot <- function(long_tbl) {
  long_tbl <- as.data.frame(long_tbl)
  step2 <- long_tbl %>%
    group_by(Type, Origin) %>%
    summarise(
      MaxInvoiceRUB = if (all(is.na(MaxInvoiceRUB))) {
        NA_real_
      } else {
        max(MaxInvoiceRUB, na.rm = TRUE)
      },
      .groups = "drop"
    ) %>%
    tidyr::pivot_wider(names_from = Origin, values_from = MaxInvoiceRUB) %>%
    arrange(Type)
  attr(step2, "label_fun") <- fmt_rub
  as.data.frame(step2)
}

print_step2 <- function(step2_tbl) {
  rubfmt <- attr(step2_tbl, "label_fun", exact = TRUE)
  if (is.null(rubfmt)) {
    rubfmt <- fmt_rub
  }
  df <- step2_tbl
  num_cols <- names(df)[sapply(df, is.numeric)]
  df_fmt <- df
  df_fmt[num_cols] <- lapply(df_fmt[num_cols], rubfmt)
  print(df_fmt, row.names = FALSE)
}

plot_step3 <- function(
  step2_tbl,
  title = "Максимальный размер счета по типам кузова и регионам"
) {
  long <- step2_tbl %>%
    pivot_longer(-Type, names_to = "Origin", values_to = "MaxInvoiceRUB")
  ggplot(long, aes(x = Type, y = MaxInvoiceRUB, fill = Origin)) +
    geom_col() +
    scale_y_continuous(labels = fmt_rub) +
    labs(
      x = "Тип кузова",
      y = "Максимальный счет, RUB",
      title = title,
      fill = "Регион"
    ) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 25, hjust = 1))
}

cars_report <- function(path_csv = "CARS.csv", rate = 80, encoding = "UTF-8") {
  df <- read_clean_cars(path_csv, encoding = encoding)
  all_types <- df %>% distinct(Type) %>% arrange(Type) %>% pull(Type)
  obj <- as.data.frame(df)
  class(obj) <- c("cars_report", class(obj))
  attr(obj, "rate") <- rate
  attr(obj, "all_types") <- all_types
  attr(obj, "step1_path") <- NULL
  obj
}

print.cars_report <- function(x, ..., sep = ", ") {
  df <- as.data.frame(x)
  rate <- attr(x, "rate")
  all_types <- attr(x, "all_types")
  blocks <- make_blocks(df, all_types = all_types, rate = rate, sep = sep)
  cat(paste(blocks, collapse = "\n\n"))
  invisible(x)
}

write.csv <- function(x, ...) UseMethod("write.csv")
write.csv.default <- function(x, file = "", ...) {
  utils::write.csv(x, file = file, ...)
}

write.csv.cars_report <- function(
  x,
  file = "cars_step2_summary.csv",
  ...,
  sep = ", "
) {
  step1_path <- attr(x, "step1_path", exact = TRUE)
  all_types <- attr(x, "all_types", exact = TRUE)
  rate <- attr(x, "rate", exact = TRUE)

  if (!is.null(step1_path) && file.exists(step1_path)) {
    long_tbl <- read_step1_to_long(step1_path, all_types = all_types)
  } else {
    df <- as.data.frame(x)
    blocks <- make_blocks(df, all_types = all_types, rate = rate, sep = sep)
    tmpf <- tempfile(fileext = ".txt")
    writeLines(paste(blocks, collapse = "\n\n"), tmpf, useBytes = TRUE)
    on.exit(unlink(tmpf), add = TRUE)
    long_tbl <- read_step1_to_long(tmpf, all_types = all_types)
  }

  step2 <- build_step2_pivot(long_tbl)
  utils::write.csv(
    step2,
    file = file,
    row.names = FALSE,
    fileEncoding = "UTF-8"
  )
  invisible(step2)
}

plot.cars_report <- function(x, ..., title = NULL, sep = ", ") {
  step1_path <- attr(x, "step1_path", exact = TRUE)
  all_types <- attr(x, "all_types", exact = TRUE)
  rate <- attr(x, "rate", exact = TRUE)

  if (!is.null(step1_path) && file.exists(step1_path)) {
    long_tbl <- read_step1_to_long(step1_path, all_types = all_types)
  } else {
    df <- as.data.frame(x)
    blocks <- make_blocks(df, all_types = all_types, rate = rate, sep = sep)
    tmpf <- tempfile(fileext = ".txt")
    writeLines(paste(blocks, collapse = "\n\n"), tmpf, useBytes = TRUE)
    on.exit(unlink(tmpf), add = TRUE)
    long_tbl <- read_step1_to_long(tmpf, all_types = all_types)
  }

  step2 <- build_step2_pivot(long_tbl)
  if (is.null(title)) {
    title <- "Максимальный размер счета (RUB) по типам кузова с накоплением по регионам"
  }
  plt <- plot_step3(step2, title = title)
  print(plt)
  invisible(plt)
}

report <- cars_report("CARS.csv", rate = 80)
# print(report)
report <- write_step1(report, "cars_step1.txt")
write.csv(report, "cars_step2_summary.csv")
png("cars_step3_plot.png", width = 1200, height = 800, res = 150)
plot(report)
# dev.off()
