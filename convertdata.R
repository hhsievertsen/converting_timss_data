library("parquetize")
setwd("/Users/hhs/Dropbox/My Mac (Hanss-MacBook-Air.local)/Downloads/")
library(arrow)
# import lib
library(haven)
library(microbenchmark)
library(dplyr)
library(readr)
library(readstata13)
library(tictoc)
library(data.table)
library(jsonlite)
library("feather")
directory <-"/Users/hhs/Dropbox/My Mac (Hanss-MacBook-Air.local)/Downloads/T19_G4_SAS Data"

# List all sas7bdat files in the directory
file_list <- list.files(directory, pattern = "\\.sas7bdat$", full.names = TRUE)

# Initialize an empty list to store data frames
data_list <- list()

# Loop through each file and read the data
for (file in file_list) {
  data <- read_sas(file)  # Read the SAS file
  data_list[[file]] <- data  # Store the data frame in the list
}

# Combine all data frames into one
combined_data <- bind_rows(data_list)


write_parquet(combined_data, 'Temp/timss_4_2019.parquet')



# Save rdata
save(combined_data,file="Temp/timss_8_2019.Rdata")
# Save csv file
write.csv(combined_data, "Temp/timss_8_2019.csv", row.names = FALSE)
# Save Stata file
write_dta(combined_data,"Temp/timss_8_2019.dta")
# Save Parquet file 
write_parquet(combined_data, 'Temp/timss_8_2019.parquet')
# Save json file
# Convert the data frame to JSON
json_output <- toJSON(combined_data, pretty = TRUE)
# Save the JSON output to a file
write(json_output, file = "data.json")
# feather
write_feather(combined_data, 'Temp/timss_8_2019.feather')
# RDS 
saveRDS(combined_data, file = "Temp/timss_8_2019.rds")



# Load rdata
tic()
load("Temp/timss_8_2019.Rdata")
time_Rdata=toc()
rm(combined_data)
# read files
# Load csv data with readr
tic()
  df_read_csv<-read_csv("Temp/timss_8_2019.csv")
time_read_csv=toc()
rm(df_read_csv)
# Load csv data with base R
tic()
df_read.csv<-read.csv("Temp/timss_8_2019.csv")
time_read.csv=toc()
rm(df_read.csv)
# Load csv data with data.table
tic()
df_fread.csv<-fread("Temp/timss_8_2019.csv")
time_fread.csv=toc()
rm(df_fread.csv)
# Load feather data
tic()
df_feather<-read_feather("Temp/timss_8_2019.feathjer")
time_feather=toc()
rm(df_feather)
# Load parquet data
tic()
df_parquet<-read_parquet("Temp/timss_8_2019.parquet")
time_parquet=toc()
rm(df_parquet)




# Benchmark
mbm = microbenchmark(
  df_read_csv<-read_csv("Temp/timss_8_2019.csv"),
  df_read.csv<-read.csv("Temp/timss_8_2019.csv"),
  df_fread.csv<-fread("Temp/timss_8_2019.csv"),
  times=5
)