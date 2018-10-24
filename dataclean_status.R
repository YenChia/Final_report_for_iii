library(dplyr)
library(rvest)
library(stringr)

#file_dir1 <- "/status.csv"
file_dir2 <- "/station.csv"
#file_dir3 <- "/trip.csv"
#file_dir4 <- "/weather.csv"

file_dir <- "/status_1.csv"
wea_dir <- "/san-jose-weather-data.csv"
wea_dir2 <- "/san-francisco-weather-data.csv"

system.time({  status <- read.csv(file_dir, sep = ",", header = TRUE)  })
system.time({  station <- read.csv(file_dir2, sep = ",")  })  
system.time({  weather <- read.csv(wea_dir3, sep = ",")  })  
 
names(status) <- c("station_id","bikes_available","docks_available","time")

##### check NA

check_NA <- function(x) {
  is_na_num <- c()
  for (i in 1:length(x)) {
    n <- is.na(x[i]) %>% 
      sum()
    is_na_num <- c(is_na_num,  n)
  }
  names(is_na_num) <- colnames(x)
  return(is_na_num)
}

check_NA(weather) %>% View()

##### time to POSIXct append weekdays

start_time <- Sys.time()
  status$time <- as.POSIXct(status$time)
  status <- cbind(status, weekdays = weekdays(status$time))
  status[,5] <- as.character(status[,5])


##### merge status and station by "station_id"

status <- merge(status, station, by = "station_id", all.x = TRUE)

##### station situation

situation <- c(NA, length = nrow(status))
situation_num <- status$bikes_available / status$dock_count
situation[situation_num <= 0.1] <- -1
situation[situation_num >= 0.9] <- 1
situation[situation_num > 0.1 & situation_num < 0.9] <- 0
status <- cbind(status, situation = situation)

write.table(status, file = "/status_1_two.csv", sep = ",", row.names = FALSE)

end_time <- Sys.time()
end_time - start_time





##### for loop

start_time <- Sys.time()
for (i in c(2:3,5:15)) {
  dir <- "/status_"
  loadfile <- paste0(dir, i, ".csv")
  status <- read.csv(loadfile, sep = ",", header = FALSE)
  names(status) <- c("station_id","bikes_available","docks_available","time")
  
  status$time <- as.POSIXlt(status$time)
  status <- cbind(status, weekdays = weekdays(status$time))
  status[,5] <- as.character(status[,5])
  
  status <- merge(status, station, by = "station_id", all.x = TRUE)
  
  situation <- c(NA, length = nrow(status))
  situation_num <- status$bikes_available / status$dock_count
  situation[situation_num <= 0.1] <- -1
  situation[situation_num >= 0.9] <- 1
  situation[situation_num > 0.1 & situation_num < 0.9] <- 0
  status <- cbind(status, situation = situation)
  
  savefile <- paste0(dir, i, "_two.csv")
  write.table(status, file = savefile, sep = ",", row.names = FALSE, col.names = FALSE)
}
end_time <- Sys.time()
end_time - start_time

##### weather clean

weather$Date <- as.character(weather$Date)
yaer <- substr(weather$Date, 1, 4)
month <- substr(weather$Date, 5, 6)
day <- substr(weather$Date, 7, 8)
weather$Date <- paste0(yaer, "-", month, "-", day, " ", weather$Time, ":00") %>% as.POSIXct()

### "" to NA

for (i in 3:9) {imputeData[imputeData[,i] == "", i] <- NA}

check_NA(imputeData) %>% View()

##### KNN fill NA

library(DMwR)
imputeData <- knnImputation(weather)

##### 

library(stringr)
a <- as.character(imputeData$Date)
b <- paste0(substr(a, 1, 4), substr(a, 6, 7), substr(a, 9, 10))
c <- as.numeric(substr(a, 12, 13))
d <- as.numeric(substr(a, 15, 16))
c[d > 30] <- c[d > 30] + 1
c[c == 24] <- 0
imputeData$Time <- paste0(b, paste0("0", c[str_length(as.character(c)) == 1]))

imputeData$Temperature <- str_trim(sub("°C", "", imputeData$Temperature))

imputeData$Wind <- as.character(imputeData$Wind)
imputeData$Wind[grepl("No wind", imputeData$Wind)] <- "0 km/h"
imputeData$Wind[is.na(imputeData$Wind)] <- "0 km/h"
imputeData$Wind <- str_trim(sub("km/h", "", imputeData$Wind))

imputeData$Humidity[is.na(imputeData$Humidity)] <- 0
imputeData$Humidity <- as.numeric(sub("%", "", imputeData$Humidity))/100

imputeData$Barometer <- as.character(imputeData$Barometer)
imputeData$Barometer <- str_trim(sub("mbar", "", imputeData$Barometer))

imputeData$Visibility <- as.character(imputeData$Visibility)
imputeData$Visibility[is.na(imputeData$Visibility)] <- "0 km"
imputeData$Visibility <- str_trim(sub("km", "", imputeData$Visibility))

write.table(imputeData, file = "/san-francisco-weather-data_new.csv", sep = ",", row.names = FALSE)


##### weather ver.2


wea_dir1 <- "/san-francisco-weather-data_new2.csv"
wea_dir2 <- "/san-jose-weather-data_new2.csv"
wea_dir3 <- "/Palo-Alto-weather-data-ver-2_new2.csv"
wea_dir4 <- "/Mountain-View-weather-data-ver-2_new2.csv"
wea_dir5 <- "/Redwood-City-weather-data-ver-2.csv"

system.time({ weather <- read.csv(wea_dir4, sep = ",") })
weather <- weather[!duplicated(weather$new.date),]

a <- as.character(weather2$Date)
b <- paste0(substr(a, 1, 4), substr(a, 6, 7), substr(a, 9, 10))
c <- as.numeric(substr(a, 12, 13))
d <- as.numeric(substr(a, 15, 16))
weather2 <- weather2[ d>30 & d!= 47 | d==0,]  ## repeat above
c[d!=0] <- c[d!=0] + 1
c[c == 24] <- 0
c[str_length(as.character(c)) == 1] <-  paste0("0", c[str_length(as.character(c)) == 1])
weather["id"] <- paste0(b, c)

weather2 <- data.frame(Date = weather$new.date, id = weather$id, Temperature = weather$temperature,
                       Weather = weather$even, Wind = weather$wind, Wind.Direction = weather$wind.direction,
                       Humidity = weather$humidity, Barometer = weather$barometer, Visibility = weather$visibility)
weather2$Date <- as.POSIXct(as.character(weather2$Date))
weather2$Humidity <- as.numeric(as.character(weather2$Humidity)) / 100

weather2$city <- "Mountain View"




write.table(weather2, file = "/Mountain-View-weather-data-ver-2_new3.csv", sep = ",", row.names = FALSE)

##### merge status.csv & weather_city

start_time <- Sys.time()
for (i in 2:15) {
  file_dir <- "/"
  wea_dir <- "/weather.csv"
  locdfile <- paste0(file_dir, "status_", i, "_two.csv")

  status <- read.csv(locdfile, sep = ",", header = FALSE)
  weather <- read.csv(wea_dir, sep = ",")
  
  names(status) <- c("station_id","bikes_available","docks_available","time",
                     "weekdays","name","lat","long","dock_count","city","installation_date","situation")
  
  a <- as.character(status$time)
  b <- paste0(substr(a, 1, 4), substr(a, 6, 7), substr(a, 9, 10))
  c <- as.numeric(substr(a, 12, 13))
  c[str_length(as.character(c)) == 1] <-  paste0("0", c[str_length(as.character(c)) == 1])
  status["year"] <- substr(a, 1, 4)
  status["month"] <- substr(a, 6, 7)
  status["day"] <- substr(a, 9, 10)
  status["hour"] <- c
  status["id"] <- paste0(b, c)
  status["k"] <- 1:nrow(status)
  
  #length(unique(weather$id))
  e <- c("id","city")
  status2 <- merge(status, weather, by = e, all.x = TRUE)
  status2 <- status2[order(status2$k),]
  status2[c("k","Date","installation_date","id")] <- NULL
  
  savefile <- paste0(file_dir, "s", i, ".csv")
  write.table(status2, file = savefile, sep = ",", row.names = FALSE, col.names = FALSE)
}
end_time <- Sys.time()
end_time - start_time





##### weather delect repeat

wea_dir1 <- "/Mountain-View-weather-data-ver-2_new4.csv"
wea_dir2 <- "/Palo-Alto-weather-data-ver-2_new4.csv"
wea_dir3 <- "/Redwood-City-weather-data-ver-2_new2.csv"
wea_dir4 <- "/san-francisco-weather-data_new3.csv"
wea_dir5 <- "/san-jose-weather-data_new4.csv"

weather <- read.csv(wea_dir5, sep = ",")
weather <- weather[!duplicated(weather$id),]

write.table(weather, file = "/san-jose-weather-data_new5.csv", sep = ",", row.names = FALSE)






