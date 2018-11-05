library(dplyr)
library(rvest)
library(stringr)

file_dir <- "/status.csv"

system.time({  status <- read.csv(file_dir, sep = ",", header = TRUE)  })

start_time <- Sys.time()
  a <- as.character(status$time)
  b <- paste0(substr(a, 1, 4), substr(a, 6, 7), substr(a, 9, 10), substr(a, 12, 13))
  d <- substr(status$time, 15, 16)
  status[d>=30,"id"] <- paste0(b[d>=30],"30")
  status[d<30,"id"] <- paste0(b[d<30],"00")
end_time <- Sys.time()
end_time - start_time

start_time <- Sys.time()
  e <- c(NA, length = nrow(status))
  situation_num <- status$bikes_available / (status$bikes_available + status$docks_available)
  e[situation_num <= 0.1] <- -1
  e[situation_num >= 0.9] <- 1
  e[situation_num > 0.1 & situation_num < 0.9] <- 0
  status <- cbind(status, situation = e)
end_time <- Sys.time()
end_time - start_time

status["time"] <- NULL
write.table(status, file = "/share/clean/status_new.csv", sep = ",", row.names = FALSE)

# %>% filter(min < 30) %>% getmode("situation")

file_dir <- "/status_new.csv"

system.time({  status <- read.csv(file_dir, sep = " ", header = TRUE)  })

getmode <- function(x,v) {
  z <- names(table(x[v]))[which.max(table(x[v]))]
  head(x[x[v]==z,],1)
}

system.time({  status2 <- status %>% group_by(id,station_id) %>% do(getmode(., "situation"))  })
system.time({   write.table(status2, file = "/share/clean/status_30min.csv", sep = ",", row.names = FALSE)  })

head(status,1000) %>% View()

length(unique(status$station_id))

z <- c(2:14,16,21:39,41,42,45:51,54:77,80,82:84)
length(z)


start_time <- Sys.time()
for (i in z) {
  savevar <- paste0("/status2_", i,".csv")
  write.table(status[status$station_id==i,], file = savevar, sep = ",", row.names = FALSE)
}
end_time <- Sys.time()
end_time - start_time

y <- c(55,73)

start_time <- Sys.time()
for (i in y) {
  loadvar <- paste0("/status2_", i,".csv")
  status2 <- read.csv(loadvar, sep = ",", header = TRUE)
  status3 <- status2 %>% group_by(id) %>% do(getmode(., "situation"))
  savefile <- paste0("/status22_", i,".csv")
  write.table(status3, file = savefile, sep = ",", row.names = FALSE, col.names = FALSE)
}
end_time <- Sys.time()
end_time - start_time


#### status2_55.csv  look    73

status2 <- read.csv("/status2_73.csv", sep = ",", header = TRUE)

unique(status2$station_id)
unique(status2$bikes_available)
unique(status2$docks_available)
unique(status2$id)
unique(status2$situation)

##### merge station & weather

system.time({  status <- read.csv("/status22_new.csv", sep = ",", header = TRUE)  })  
system.time({  station <- read.csv("/station.csv", sep = ",")  })  
system.time({  weather <- read.csv("/weather.csv", sep = ",")  })  

start_time <- Sys.time()
  status$id <- as.character(status$id)
  a <- status$id
  status$time <- paste0(substr(a,1,4),"/",substr(a,5,6),"/",substr(a,7,8)," ",substr(a,9,10),":",substr(a,11,12),":00")
  status$time <- as.POSIXct(status$time)
  status$id <- substr(a,1,10)
  status <- cbind(status, weekdays = weekdays(status$time))
  status["year"] <- substr(a,1,4)
  status["month"] <- substr(a,5,6)
  status["hour"] <- substr(a,9,10)
  status["k"] <- 1:nrow(status)

  #head(status) %>% View()

  status <- merge(status, station, by = "station_id", all.x = TRUE)
  e <- c("id","city")
  status <- merge(status, weather, by = e, all.x = TRUE)
  status <- status[order(status$k),]
  status[c("k","Date","installation_date","id")] <- NULL

  write.table(status, file="/status_30min.csv", sep = ",", row.names = FALSE)
end_time <- Sys.time()
end_time - start_time

##### change column "hour" append 30min

file_dir <- "/status_30min_2.csv"
system.time({  status <- read.csv(file_dir, sep = ",", header = TRUE)  })

head(status, 5)
a <- as.character(status$time)
b <- paste0(substr(a, 12, 13), substr(a, 15, 16))
status$hour <- b
unique(status$hour)

system.time({  write.table(status, file="/status_30min_2.csv", sep = ",", row.names = FALSE)  })


