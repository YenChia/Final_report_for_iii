file_dir1 <- "/share/clean/status_30min_2.csv"
file_dir2 <- "/share/clean/status_30min_flo_2.csv"
file_dir3 <- "/share/clean/weather.csv"


status <- read.csv(file_dir2, sep = ",", header = TRUE)

#wea <- weather$Weather

unique(status$Weather)

status$Weather <- as.character(status$Weather)

status[grepl("Thunder", status$Weather),"Weather"] <- "Heavy rain"
status[grepl("thunder", status$Weather),"Weather"] <- "Heavy rain"
status[grepl("Heavy rain", status$Weather),"Weather"] <- "Heavy rain"
status[grepl("Lots of rain", status$Weather),"Weather"] <- "Heavy rain"

status[grepl("Light rain", status$Weather),"Weather"] <- "Light rain"
status[grepl("Drizzle", status$Weather),"Weather"] <- "Light rain"
status[grepl("Scattered", status$Weather),"Weather"] <- "Light rain"
status[grepl("Sprinkles", status$Weather),"Weather"] <- "Light rain"

status[grepl("Rain", status$Weather),"Weather"] <- "Rain"

status[grepl("Sunny", status$Weather),"Weather"] <- "Sunny"
status[grepl("Warm", status$Weather),"Weather"] <- "Sunny"
status[grepl("Clear", status$Weather),"Weather"] <- "Sunny"
status[grepl("Pleasantly warm", status$Weather),"Weather"] <- "Sunny"
status[grepl("Cool", status$Weather),"Weather"] <- "Sunny"
status[grepl("Refreshingly cool", status$Weather),"Weather"] <- "Sunny"
status[grepl("Mild", status$Weather),"Weather"] <- "Sunny"
status[grepl("Partly sunny", status$Weather),"Weather"] <- "Sunny"

status[grepl("Fog", status$Weather),"Weather"] <- "Fog"
status[grepl("Haze", status$Weather),"Weather"] <- "Fog"
status[grepl("Light fog", status$Weather),"Weather"] <- "Fog"

status[grepl("Snow", status$Weather),"Weather"] <- "Snow"
status[grepl("Sleet", status$Weather),"Weather"] <- "Snow"

status[grepl("Cloudy", status$Weather),"Weather"] <- "Cloudy"
status[grepl("clouds", status$Weather),"Weather"] <- "Cloudy"
status[grepl("cloudy", status$Weather),"Weather"] <- "Cloudy"
status[grepl("Overcast", status$Weather),"Weather"] <- "Cloudy"

unique(status$Wind.Direction)

status$Wind.Direction <- as.character(status$Wind.Direction)

status$Wind.Direction <- str_split(status$Wind.Direction, "°") %>% 
  sapply("[[", 1) %>% 
  str_sub(19,-1) %>% 
  as.numeric()

write.table(status, file = file_dir2, sep = ",", row.names = FALSE)

#####  for 1 min csv

file_dir <- "/share/clean/"

start_time <- Sys.time()
for (i in 2:15) {
  locdfile <- paste0(file_dir, "status2_", i, ".csv")
  status <- read.csv(locdfile, sep = ",", header = FALSE)
  
  names(status) <- c("city","station_id","bikes_available","docks_available","time",
                     "weekdays","name","lat","long","dock_count","situation","year",
                     "month","day","hour","Temperature","Weather","Wind","Wind.Direction",
                     "Humidity","Barometer","Visibility")

  status$Weather <- as.character(status$Weather)
  
  status[grepl("Thunder", status$Weather),"Weather"] <- "Heavy rain"
  status[grepl("thunder", status$Weather),"Weather"] <- "Heavy rain"
  status[grepl("Heavy rain", status$Weather),"Weather"] <- "Heavy rain"
  status[grepl("Lots of rain", status$Weather),"Weather"] <- "Heavy rain"
  
  status[grepl("Light rain", status$Weather),"Weather"] <- "Light rain"
  status[grepl("Drizzle", status$Weather),"Weather"] <- "Light rain"
  status[grepl("Scattered", status$Weather),"Weather"] <- "Light rain"
  status[grepl("Sprinkles", status$Weather),"Weather"] <- "Light rain"
  
  status[grepl("Rain", status$Weather),"Weather"] <- "Rain"
  
  status[grepl("Sunny", status$Weather),"Weather"] <- "Sunny"
  status[grepl("Warm", status$Weather),"Weather"] <- "Sunny"
  status[grepl("Clear", status$Weather),"Weather"] <- "Sunny"
  status[grepl("Pleasantly warm", status$Weather),"Weather"] <- "Sunny"
  status[grepl("Cool", status$Weather),"Weather"] <- "Sunny"
  status[grepl("Refreshingly cool", status$Weather),"Weather"] <- "Sunny"
  status[grepl("Mild", status$Weather),"Weather"] <- "Sunny"
  status[grepl("Partly sunny", status$Weather),"Weather"] <- "Sunny"
  
  status[grepl("Fog", status$Weather),"Weather"] <- "Fog"
  status[grepl("Haze", status$Weather),"Weather"] <- "Fog"
  status[grepl("Light fog", status$Weather),"Weather"] <- "Fog"
  
  status[grepl("Snow", status$Weather),"Weather"] <- "Snow"
  status[grepl("Sleet", status$Weather),"Weather"] <- "Snow"
  
  status[grepl("Cloudy", status$Weather),"Weather"] <- "Cloudy"
  status[grepl("clouds", status$Weather),"Weather"] <- "Cloudy"
  status[grepl("cloudy", status$Weather),"Weather"] <- "Cloudy"
  status[grepl("Overcast", status$Weather),"Weather"] <- "Cloudy"
  
  status$Wind.Direction <- as.character(status$Wind.Direction)
  
  status$Wind.Direction <- str_split(status$Wind.Direction, "°") %>% 
    sapply("[[", 1) %>% 
    str_sub(19,-1) %>% 
    as.numeric()
  
  savefile <- paste0(file_dir, "s", i, ".csv")
  write.table(status, file = savefile, sep = ",", row.names = FALSE, col.names = FALSE)
}
end_time <- Sys.time()
end_time - start_time
