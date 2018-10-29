library(dplyr)
library(rvest)
library(ggplot2)
library(scales)
library(stringr)
library(tidyr)

file_dir <- "/trip_two2.csv"
file_dir1 <- "/trip_fix.csv"
file_dir2 <- "/station.csv"
file_dir3 <- "/borNum.csv"
file_dir4 <- "/retNum.csv"

##### start

trip <- read.csv(file_dir, sep = ",", header = TRUE)
#trip[c("k","installation_date","Date","dock_count","name","id")] <- NULL
#write.table(trip, file = file_dir, sep = ",", row.names = FALSE)
station <- read.csv(file_dir2, sep = ",", header = TRUE)

##### weekday total usage number

trip["weekdays"] <- NULL
trip <- cbind(trip, weekdays = weekdays(as.POSIXct(substr(trip$start_date,1,10))))
trip$weekdays <- as.factor(trip$weekdays) 
wd <- c("Monday", "Tuesday", "Wednesday","Thursday", "Friday", "Saturday", "Sunday")
trip$weekdays <- factor(trip$weekdays, levels = wd)

png(filename = "/perWeekday.png", width = 1200, height = 800)
ggplot(trip, aes(weekdays, group = subscription_type, fill = subscription_type))+
  geom_bar(alpha=.5, position = "dodge")+
  theme(axis.text = element_text(size = 18, color = "black", face = "bold", hjust = 0.5, vjust = 0.5, angle = 0))+
  theme(axis.title = element_text(size = 22, color = "black", face = "italic", hjust = 0.5, vjust = 1, angle =0))+
  theme(legend.title = element_text(size = 16, color = "azure4", face = "plain"))+
  theme(legend.text = element_text(size=14, color = "azure4", face = "bold.italic"))+
  theme(legend.position = "top")+
  theme(title = element_text(size = 22, color = "azure4", face = "italic"))+
  xlab("Weekday")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for Weekday")
dev.off()

##### month total usage number (+avg. temperature??)

trip$month <- as.character(substr(trip$start_date,6,7))

ggplot(trip, aes(trip$month, group = subscription_type, fill = subscription_type))+
  geom_bar(alpha=.4, position = "stack")+
  xlab("Month")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for Month")

##### station total usage number

trip$start_station_id <- as.character(trip$start_station_id)
trip[str_length(trip$start_station_id)==1,"start_station_id"] <- paste0("0",trip[str_length(trip$start_station_id)==1,"start_station_id"])

ggplot(trip, aes(trip$start_station_id, group = city, fill = city))+
  geom_bar(alpha=.8)+
  theme(axis.text.x = element_text(size = 10, color = "blue", face = "bold", vjust = 0.5, hjust = 0.5, angle = 90))+
  xlab("Station")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for Station")

##### station total return number

trip$end_station_id <- as.character(trip$end_station_id)
trip[str_length(trip$end_station_id)==1,"end_station_id"] <- paste0("0",trip[str_length(trip$end_station_id)==1,"end_station_id"])

ggplot(trip, aes(trip$end_station_id, group = city, fill = city))+
  geom_bar(alpha=.2)+
  theme(axis.text = element_text(size = 18, color = "black", face = "bold", hjust = 0.5, vjust = 0.5, angle = 0))+
  theme(axis.title = element_text(size = 22, color = "black", face = "italic", hjust = 0.5, vjust = 1, angle =0))+
  theme(legend.title = element_text(size = 16, color = "azure4", face = "plain"))+
  theme(legend.text = element_text(size=14, color = "azure4", face = "bold.italic"))+
  theme(legend.position = "top")+
  theme(title = element_text(size = 22, color = "azure4", face = "italic"))+
  xlab("Station")+
  ylab("Total Usage Number")+
  ggtitle("Return Number for Station")

##### city total usage number

ggplot(trip, aes(trip$city, group = subscription_type, fill = subscription_type))+
  geom_bar(alpha=.4, position = "dodge")+
  theme(axis.text = element_text(size = 18, color = "black", face = "bold", hjust = 0.5, vjust = 0.5, angle = 0))+
  theme(axis.title = element_text(size = 22, color = "black", face = "italic", hjust = 0.5, vjust = 1, angle =0))+
  theme(legend.title = element_text(size = 16, color = "azure4", face = "plain"))+
  theme(legend.text = element_text(size=14, color = "azure4", face = "bold.italic"))+
  theme(legend.position = "top")+
  theme(title = element_text(size = 22, color = "azure4", face = "italic"))+
  xlab("City")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for City")

##### per Hour Usage by workday

trip$hour <- as.character(substr(trip$start_date,12,13))
trip$workday <- NA
trip[trip$weekdays == "Sunday" | trip$weekdays == "Saturday", "workday"] <- "non-Work"
trip[is.na(trip$workday), "workday"] <- "Work"

png(filename = "/perHour.png", width = 1200, height = 800)
ggplot(trip, aes(trip$hour, group = workday, fill = workday))+
  geom_bar(alpha=.4, position = "dodge")+
  theme(axis.text = element_text(size = 18, color = "black", face = "bold", hjust = 0.5, vjust = 0.5, angle = 0))+
  theme(axis.title = element_text(size = 22, color = "black", face = "italic", hjust = 0.5, vjust = 1, angle =0))+
  theme(legend.title = element_text(size = 16, color = "azure4", face = "plain"))+
  theme(legend.text = element_text(size=14, color = "azure4", face = "bold.italic"))+
  theme(legend.position = "top")+
  theme(title = element_text(size = 22, color = "azure4", face = "italic"))+
  xlab("Hour")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for Hour")
dev.off()

##### duration -> minute

trip$minute <- round(trip$duration / 60)
data_minute <- summary(trip$minute)
trip[trip$minute>100,"minute"] <- 100
trip2 <- trip[trip$minute<=60,]

png(filename = "/perMinute.png", width = 1200, height = 800)
ggplot(trip2, aes(minute, group=subscription_type, fill=subscription_type))+
  geom_bar(alpha=.4, position = "stack")+
  scale_x_continuous(breaks = seq(0,60,5))+
  theme(axis.text = element_text(size = 18, color = "black", face = "bold", hjust = 0.5, vjust = 0.5, angle = 0))+
  theme(axis.title = element_text(size = 22, color = "black", face = "italic", hjust = 0.5, vjust = 1, angle =0))+
  theme(legend.title = element_text(size = 16, color = "azure4", face = "plain"))+
  theme(legend.text = element_text(size=14, color = "azure4", face = "bold.italic"))+
  theme(legend.position = "top")+
  theme(title = element_text(size = 22, color = "azure4", face = "italic"))+
  xlab("Minute")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for Minute")
dev.off()

##### minute on workday

trip["weekdays"] <- NULL
trip <- cbind(trip, weekdays = weekdays(as.POSIXct(substr(trip$start_date,1,10))))
trip$weekdays <- as.factor(trip$weekdays) 
wd <- c("Monday", "Tuesday", "Wednesday","Thursday", "Friday", "Saturday", "Sunday")
trip$weekdays <- factor(trip$weekdays, levels = wd)
trip["workday"] <- NA
trip[trip$weekdays == "Sunday" | trip$weekdays == "Saturday", "workday"] <- "non-Work"
trip[is.na(start$workday), "workday"] <- "Work"
trip$minute <- round(trip$duration / 60)
a <- trip %>% group_by(workday) %>% do(data.frame(summary(.["minute"])))
data_minute <- a[c("workday","Freq")]
trip[trip$minute>100,"minute"] <- 100
  
ggplot(trip, aes(minute, group=workday, fill=workday))+
  geom_bar(alpha=.4, position = "dodge")+
  scale_x_continuous(breaks = seq(0,100,5))+
  theme(axis.text = element_text(size = 18, color = "black", face = "bold", hjust = 0.5, vjust = 0.5, angle = 0))+
  theme(axis.title = element_text(size = 22, color = "black", face = "italic", hjust = 0.5, vjust = 1, angle =0))+
  theme(legend.title = element_text(size = 16, color = "azure4", face = "plain"))+
  theme(legend.text = element_text(size=14, color = "azure4", face = "bold.italic"))+
  theme(legend.position = "top")+
  theme(title = element_text(size = 22, color = "azure4", face = "italic"))+
  xlab("Minute")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for Minute")

##### weather

ggplot(trip, aes(Temperature, group=subscription_type, fill=subscription_type))+
  geom_bar(alpha=.4, position = "stack")+
  theme(axis.text = element_text(size = 18, color = "black", face = "bold", hjust = 0.5, vjust = 0.5, angle = 0))+
  theme(axis.title = element_text(size = 22, color = "black", face = "italic", hjust = 0.5, vjust = 1, angle =0))+
  theme(legend.title = element_text(size = 16, color = "azure4", face = "plain"))+
  theme(legend.text = element_text(size=14, color = "azure4", face = "bold.italic"))+
  theme(legend.position = "top")+
  theme(title = element_text(size = 22, color = "azure4", face = "italic"))+
  xlab("Temperature")+
  ylab("Total Usage Number")+
  ggtitle("Usage Number for Temperature")

##### PLOTLY ######
library(plotly)

#station[c("name","dock_count","city","installation_date")] <- NULL
#trip$k <- 1:nrow(trip)
#trip <- merge(trip, station, by.x = "start_station_id", by.y = "id", all.x = TRUE)
#trip <- trip[order(trip$k),]
#trip$k <- NULL
#write.table(trip, file = "/home/kilio/R/trip_two2.csv", sep = ",", row.names = FALSE)

Sys.setenv("plotly_username" = "kilio")
Sys.setenv("plotly_api_key" = "sDkWMyHwE0DJeG3JX92T")

trip[c("city","duration","start_date","start_station_name","end_date","end_station_name",
       "bike_id","subscription_type","Temperature","Weather","Wind","Wind.Direction",
       "Humidity","Barometer","Visibility")] <- NULL
trip$end_station_id <- as.character(trip$end_station_id)
trip[str_length(trip$end_station_id)==1,"end_station_id"] <- paste0("0",trip[str_length(trip$end_station_id)==1,"end_station_id"])
trip$start_station_id <- as.character(trip$start_station_id)
trip[str_length(trip$start_station_id)==1,"start_station_id"] <- paste0("0",trip[str_length(trip$start_station_id)==1,"start_station_id"])
trip$to <- paste0(trip$start_station_id," to ",trip$end_station_id)
trip2 <- trip %>% group_by(to) %>% do(data.frame(.[1,]))
write.table(trip2, file = "/home/kilio/R/trip2.csv", sep = ",", row.names = FALSE)
trip2 <- read.csv("/home/kilio/R/trip2.csv", header = TRUE)

geo <- list(
  scope = "San Francisco",
  projection = list(type = "Satellite Map"),
  showland = TRUE,
#  type = "Satellite Map",
  landcolor = toRGB("gray95"),
  countrycolor = toRGB("gray80")
)

p <- plot_geo(locationmode = "SF", color = I("red")) %>% 
  add_markers(
    data = trip2, x = ~long, y = ~lat, text = ~start_station_id,
    hoverinfo = "text", alpha = 0.5
  ) %>% 
  add_markers(
    data = group_by(trip2, start_station_id),
    x = ~start_long, xend = ~end_long,
    y = ~start_lat, yend = ~end_lat,
    alpha = 0.3, size = I(1), hoverinfo = "none"
  ) %>% 
  layout(
    title = "sf_bike",
    geo = geo, showlegend = FALSE
  )

chart_link = api_create(p, filename = "sf_bike", fileopt = "overwrite")
chart_link
