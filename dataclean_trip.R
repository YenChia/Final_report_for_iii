library(dplyr)
library(stringr)
library(rvest)
#library(saprklyr)

df <- read.csv("/home/kilio/R/status10000.csv", sep = ",", header = TRUE)

a <- as.character(df$time)
b <- paste0(substr(a,1,4),substr(a,6,7),substr(a,9,10),substr(a,12,13))
d <- as.integer(substr(a,15,16))
df[d>=30,"id"] <- paste0(b[d>=30],"30")
df[d<30,"id"] <- paste0(b[d<30],"00")

write.table(df, file = "/home/kilio/R/statusNew.csv", sep = ",", row.names = FALSE)

df["situation"] <- NULL
d = c(NA, length = nrow(df))
b <- df$bikes_available / (df$bikes_available + df$docks_available)
d[b<=0.1] <- -1
d[b>=0.9] <- 1
d[b>0.1&b<0.9] <- 0
df <- cbind(df, situation = d)

df2 <- df %>% group_by(station_id, id) %>% do(getMode(.,"situation"))
df2 <- df %>% group_by(station_id) %>% do(group_by(.["id"])) #%>% do(getMode(.,"situation"))

getMode <- function(x,v){
  a <- names(table(x[v]))[which.max(table(x[v]))]
  head(x[x[v]==a,],1)
}

##### trip.csv

file_dir <- "/share/sf_bike_data/trip.csv"
trip <- read.csv(file_dir, sep = ",", header = TRUE)

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

check_NA(trip) %>% View()

##### 
trip$start_date <- as.character(trip$start_date)
trip$end_date <- as.character(trip$end_date)
trip[c("id","zip_code")] <- NULL


start_time <- Sys.time()
  trip["start_id"] <- NA
  year <- str_extract(trip$start_date, "[0-9]{4}")
  month <- str_extract(trip$start_date, "^[0-9]{1,2}")
  month[str_length(month)==1] <- paste0("0",month[str_length(month)==1])
  day <- str_extract(trip$start_date, "/[0-9]{1,2}/") %>% str_replace_all("/","")
  day[str_length(day)==1] <- paste0("0",day[str_length(day)==1])
  hour <- str_trim(str_extract(trip$start_date,"\ [0-9]{1,2}:")) %>% str_replace_all(":","")
  hour[str_length(hour)==1] <- paste0("0",hour[str_length(hour)==1])
  min <- as.integer(str_extract(trip$start_date, "[0-9]{1,2}$"))
  z <- paste0(year,month,day,hour)
  trip[min>=30,"start_id"] <- paste0(z[min>=30], "30")
  trip[min<30,"start_id"] <- paste0(z[min<30], "00")
  trip$start_date <- paste0(year,"/",month,"/",day," ",hour,":",min,":00") %>% as.POSIXct()
end_time <- Sys.time()
end_time - start_time

start_time <- Sys.time()
  trip["end_id"] <- NA
  year <- str_extract(trip$end_date, "[0-9]{4}")
  month <- str_extract(trip$end_date, "^[0-9]{1,2}")
  month[str_length(month)==1] <- paste0("0",month[str_length(month)==1])
  day <- str_extract(trip$end_date, "/[0-9]{1,2}/") %>% str_replace_all("/","")
  day[str_length(day)==1] <- paste0("0",day[str_length(day)==1])
  hour <- str_trim(str_extract(trip$end_date,"\ [0-9]{1,2}:")) %>% str_replace_all(":","")
  hour[str_length(hour)==1] <- paste0("0",hour[str_length(hour)==1])
  min <- as.integer(str_extract(trip$end_date, "[0-9]{1,2}$"))
  z <- paste0(year,month,day,hour)
  trip[min>=30,"end_id"] <- paste0(z[min>=30], "30")
  trip[min<30,"end_id"] <- paste0(z[min<30], "00")
  trip$end_date <- paste0(year,"/",month,"/",day," ",hour,":",min,":00") %>% as.POSIXct()
end_time <- Sys.time()
end_time - start_time

trip["start_id"] <- NULL
trip["end_id"] <- NULL
system.time({  write.table(retNum, file = "/share/clean/trip_fix.csv", sep = ",", row.names = FALSE)  })

##### calculate every 30min number of borrow and return

start_time <- Sys.time()
  borrow <- trip %>% group_by(start_station_id,start_id) %>% do(data.frame(nrow(.)))
  return <- trip %>% group_by(end_station_id,end_id) %>% do(data.frame(nrow(.)))
  borNum <- data.frame(borrow[1], borrow[2], borrow[3])
  retNum <- data.frame(return[1], return[2], return[3])
  names(borNum) <- c("start_station_id","start_id","borNum")
  names(retNum) <- c("end_station_id","end_id","retNum")
end_time <- Sys.time()
end_time - start_time

paste(trip$start_station_id, trip$start_id) %>% unique() %>% length()
paste(trip$end_station_id, trip$end_id) %>% unique() %>% length()

write.table(borNum, file = "/share/clean/borNum.csv", sep = ",", row.names = FALSE)
write.table(retNum, file = "/share/clean/retNum.csv", sep = ",", row.names = FALSE)

##### merge status & trip

file_dir <- "/share/sf_bike_clean/status_30min.csv"
system.time({  status <- read.csv(file_dir, sep = ",", header = TRUE)  })

status$id <- paste0(substr(status$time,1,4),substr(status$time,6,7),substr(status$time,9,10),substr(status$time,12,13),substr(status$time,15,16))

#status[c("borNum","retNum")] <- NULL
system.time({  status2 <- merge(status, borNum, by.x = c("station_id","id"), by.y = c("start_station_id","start_id"), all.x = TRUE)  })
system.time({  status2 <- merge(status2, retNum, by.x = c("station_id","id"), by.y = c("end_station_id","end_id"), all.x = TRUE)  })

status2[is.na(status2["borNum"]),"borNum"] <- 0
status2[is.na(status2["retNum"]),"retNum"] <- 0

start_time <- Sys.time()
  status2["id"] <- NULL
  write.table(status2, file = "/share/clean/status_30min.csv", sep = ",", row.names = FALSE)
end_time <- Sys.time()
end_time - start_time

head(status2) %>% View()

status["workday"] <- NA
status[status$weekdays == "Saturday" | status$weekdays == "Sunday", "workday"] <- 0
status[is.na(status["workday"]),"workday"] <- 1

unique(status2$weekdays)

##### end_station_id one hot encoding

library(ade4)
library(data.table)

trip$end_station_id <- as.character(trip$end_station_id)
trip_dummy <- acm.disjonctif(trip["end_station_id"])
trip["end_station_id"] <- NULL
trip <- cbind(trip, trip_dummy)

ohe = c("end_station_id.2","end_station_id.3","end_station_id.4","end_station_id.5",
        "end_station_id.6","end_station_id.7","end_station_id.8","end_station_id.9",
        "end_station_id.10","end_station_id.11","end_station_id.12","end_station_id.13",
        "end_station_id.14","end_station_id.16","end_station_id.21","end_station_id.22",
        "end_station_id.23","end_station_id.24","end_station_id.25","end_station_id.26",
        "end_station_id.27","end_station_id.28","end_station_id.29","end_station_id.30",
        "end_station_id.31","end_station_id.32","end_station_id.33","end_station_id.34",
        "end_station_id.35","end_station_id.36","end_station_id.37","end_station_id.38",
        "end_station_id.39","end_station_id.41","end_station_id.42","end_station_id.45",
        "end_station_id.46","end_station_id.47","end_station_id.48","end_station_id.49",
        "end_station_id.50","end_station_id.51","end_station_id.54","end_station_id.55",
        "end_station_id.56","end_station_id.57","end_station_id.58","end_station_id.59",
        "end_station_id.60","end_station_id.61","end_station_id.62","end_station_id.63",
        "end_station_id.64","end_station_id.65","end_station_id.66","end_station_id.67",
        "end_station_id.68","end_station_id.69","end_station_id.70","end_station_id.71",
        "end_station_id.72","end_station_id.73","end_station_id.74","end_station_id.75",
        "end_station_id.76","end_station_id.77","end_station_id.80","end_station_id.82",
        "end_station_id.83","end_station_id.84")

b <- trip %>% group_by(start_station_id, start_id) %>%
  do(apply(.[,ohe], 2, sum, na.rm = TRUE) %>% t() %>% rbind() %>% data.frame())

write.table(b, file = "/share/clean/trip_ohe_id.csv", sep = ",", row.names = FALSE)

##### merge status & ohe_id

file_dir <- "/share/sf_bike_clean/status_30min.csv"
system.time({  status <- read.csv(file_dir, sep = ",", header = TRUE)  })

status$id <- paste0(substr(status$time,1,4),substr(status$time,6,7),substr(status$time,9,10),substr(status$time,12,13),substr(status$time,15,16))
status["workday"] <- NA
status[status$weekdays == "Saturday" | status$weekdays == "Sunday", "workday"] <- 0
status[is.na(status["workday"]),"workday"] <- 1
status["k"] <- 1:nrow(status)
system.time({  status2 <- merge(status, b, by.x = c("station_id","id"), by.y = c("start_station_id","start_id"), all.x = TRUE)  })

for (i in 1:length(ohe)){
  status2[is.na(status2[ohe[i]]),ohe[i]] <- 0
}
status2 <- status2[order(status2$k),]

start_time <- Sys.time()
  status2[c("id","k")] <- NULL
  write.table(status2, file = "/share/clean/status_30min_flo.csv", sep = ",", row.names = FALSE)
end_time <- Sys.time()
end_time - start_time