library(tidyverse)
library(haven)
library(data.table)
library(arrow)

directory <- "/Users/hhs/Dropbox/My Mac (Hanss-MacBook-Air.local)/Desktop/Part 1"
output_directory <- "/path/to/intermediate_chunks"
final_output <- "timss_4_2019.parquet"

# Drop empty columns
not_all_na <- function(x) any(!is.na(x))

# List all SAS files
file_list <- list.files(directory, pattern = "\\.sas7bdat$", full.names = TRUE)

# Create a directory for intermediate files
dir.create(output_directory, showWarnings = FALSE)

# Process each file and save as a separate Parquet file
for (i in seq_along(file_list)) {
  file <- file_list[i]
  data <- haven::read_sas(file) %>%
    select(where(not_all_na)) %>%
    data.table::setDT()
  
  # Save each file as a Parquet chunk
  chunk_file <- file.path(output_directory, paste0("chunk_", i, ".parquet"))
  arrow::write_parquet(data, chunk_file)
}

# Combine all intermediate Parquet files into one
chunk_files <- list.files(output_directory, pattern = "\\.parquet$", full.names = TRUE)
combined_data <- lapply(chunk_files, arrow::read_parquet) %>%
  data.table::rbindlist()
arrow::write_parquet(combined_data, final_output)

# Clean up intermediate files
unlink(output_directory, recursive = TRUE)
