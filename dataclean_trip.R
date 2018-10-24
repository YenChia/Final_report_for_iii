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
end_time <- Sys.time()
end_time - start_time

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

status2["workday"] <- NA
status2[status2$weekdays == "Saturday" | status2$weekdays == "Sunday", "workday"] <- 0
status2[is.na(status2["workday"]),"workday"] <- 1

unique(status2$weekdays)





