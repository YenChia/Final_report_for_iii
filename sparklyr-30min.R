library(sparklyr)
library(dplyr)
library(rvest)
library(DBI)
library(stringr)
library(rsparkling)
library(ggplot2)

library(tidyr)

Sys.setenv(HADOOP_CONF_DIR = 'hadoop-3.1.1/etc/hadoop')
Sys.setenv(YARN_CONF_DIR = 'hadoop-3.1.1/etc/hadoop')
Sys.setenv(SPARK_HOME = 'spark-2.3.2-bin-hadoop2.7')

config <- spark_config()
config$spark.executor.instances <- 17

system.time({  sc <- spark_connect(master = 'yarn-client', # 'yarn-client',"spark://spkma.example.org:7077,rma.example.org:7077"
                    version = '2.3.2',
                    method = c("shell", "livy", "databricks", "test"),
                    app_name = "kilio",
                    config = config)  })  

system.time({  status <- spark_read_csv(sc, name = "status_30min_user",
                         path = "/dataset/sf_bike_clean/status_30min_user.csv",
                         header = TRUE,
                         delimiter = ",")  })  

#spark_disconnect(sc)

##### Function for Recall Precision & f1

rp <- function(x){
  a1 <- round(x[1]/(x[1]+x[4]+x[7]),2)
  a2 <- round(x[5]/(x[2]+x[5]+x[8]),2)
  a3 <- round(x[9]/(x[3]+x[6]+x[9]),2)
  b1 <- round(x[1]/(x[1]+x[2]+x[3]),2)
  b2 <- round(x[5]/(x[4]+x[5]+x[6]),2)
  b3 <- round(x[9]/(x[7]+x[8]+x[9]),2)
  recall <- c("-1"=a1, "0"=a2, "1"=a3)
  precision <- c("-1"=b1, "0"=b2, "1"=b3)
  f1 <- round(2 * (recall * precision) / (recall + precision), 2)
  res <- data.frame(precision, recall, f1)
  return(res)
}

######

src_tbls(sc)  # sc連線有多少個tibble
class(status)  # tibble的型態
str(status)  # tibble的結構
object.size(status)/1024  # tibble佔的記憶體大小(bytes)
dim(status)  # status的維度
print(status, n = 5, width = Inf)  # 印出前五筆，所有欄位
glimpse(status)  # DataFrame的內容與欄位類別
sc %>% spark_context %>% invoke("getRDDStorageInfo")  ## check sparklyr memory

features_col <- c("station_id", "city", "year", "month", "hour", "weekdays", "Temperature", "Weather",
                  "Wind", "Wind_Direction", "Humidity", "Barometer", "Visibility", "workday")
features_col2 <- c("station_id", "city2", "year", "month", "hour", "weekdays2", "Temperature", "Weather2",
                  "Wind", "Wind_Direction", "Humidity", "Barometer", "Visibility", "workday")
features <- "situation ~ station_id + city + year + month + hour + weekdays + Temperature + Weather +Wind + Wind_Direction + Humidity + Barometer + Visibility + workday"
features2 <- "situation ~ station_id + city2 + year + month + hour + weekdays2 + Temperature + Weather2 + Wind + Wind_Direction + Humidity + Barometer + Visibility + workday"

#####
system.time({  })
##### 按照"situation"欄位分割訓練、測試資料
                  
train_test_status <- status %>% 
  ft_string_indexer(input_col = "city", output_col = "city2") %>% 
  ft_string_indexer(input_col = "weekdays", output_col = "weekdays2") %>% 
  ft_string_indexer(input_col = "Weather", output_col = "Weather2") %>% 
  select(features_col2, situation) %>% 
  group_by(situation) %>% 
  sdf_partition(training = 0.8, testing = 0.2, seed = 61345)

train_test_status <- status %>% 
  select(features_col, situation) %>% 
  group_by(situation) %>% 
  sdf_partition(training = 0.8, testing = 0.2, seed = 61345)

##### 驗證資料正確分割
start_time <- Sys.time()
  a <- train_test_status$testing %>% filter(situation==1) %>% collect() %>% dim()
  b <- train_test_status$testing %>% filter(situation==0) %>% collect() %>% dim()
  c <- train_test_status$testing %>% filter(situation==-1) %>% collect() %>% dim()
  (a[1]+c[1]) / (a[1]+b[1]+c[1])
  a <- train_test_status$training %>% filter(situation==1) %>% collect() %>% dim()
  b <- train_test_status$training %>% filter(situation==0) %>% collect() %>% dim()
  c <- train_test_status$training %>% filter(situation==-1) %>% collect() %>% dim()
  (a[1]+c[1]) / (a[1]+b[1]+c[1])
end_time <- Sys.time()
end_time - start_time

##### data correlation matrix

fea <- c("station_id", "year", "month", "hour", "Temperature",
         "Wind", "Wind_Direction", "Humidity", "Barometer", "Visibility", "workday")
ml_corr(status, columns = fea, method = "pearson") %>% 
  ggcorrplot()

##### ml_decision_tree_classifier  可丟入類別型字串，目標欄位要轉換成類別型
features_col <- c("station_id", "city", "year", "month", "hour", "weekdays", "Temperature", "Weather",
                  "Wind", "Wind_Direction", "Humidity", "Barometer", "Visibility", "workday")
features <- "situation ~ station_id + city + year + month + hour + weekdays + Temperature + Weather +Wind + Wind_Direction + Humidity + Barometer + Visibility + workday"
train_test_status <- status %>% 
  select(features_col, situation) %>% 
  group_by(situation) %>% 
  sdf_partition(training = 0.8, testing = 0.2, seed = 6138)
train <- train_test_status$training
test <- train_test_status$testing

start_time <- Sys.time()
tree_model <- train %>% 
  mutate(situation = as.character(situation)) %>% 
  #mutate(situation = factor(situation, labels = c("no_bike", "normal", "no_dock"))) %>% 
  ml_decision_tree_classifier(features,
                              max_depth = 30L,
                              max_bins = 300L,
                              min_instances_per_node = 1L,
                              #min_info_gain = 0,
                              impurity = "entropy",  # 'gini' 'entropy'
                              seed = 6138)
                              #thresholds = NULL,
                              #cache_node_ids = FALSE,
                              #checkpoint_interval = 10L,
                              #max_memory_in_mb = 256L,
                              #uid = random_string("decision_tree_classifier_"), ...)
#ml_feature_importances(tree_model)
  responses_DT <- test %>% 
    select(features_col, situation) %>% 
    collect() %>% 
    mutate(pre_situ = predict(tree_model, test))
  results_DT <- table(Prediction = responses_DT$pre_situ, Actual = responses_DT$situation)
  rp(results_DT)
end_time <- Sys.time()
end_time - start_time

#####  ml_random_forest 將資料內容全部轉成數字，目標欄位依舊是轉類別型

features_col <- c("station_id", "city", "year", "month", "hour", "weekdays", "Temperature", "Weather",
                  "Wind", "Wind_Direction", "Humidity", "Barometer", "Visibility", "workday")
features <- "situation ~ station_id + city + year + month + hour + weekdays + Temperature + Weather +Wind + Wind_Direction + Humidity + Barometer + Visibility + workday"
train_test_status <- status %>% 
  select(features_col, situation) %>% 
  group_by(situation) %>% 
  sdf_partition(training = 0.8, testing = 0.2, seed = 6138)
train <- train_test_status$training
test <- train_test_status$testing

start_time <- Sys.time()
forest_model <- train %>%
  mutate(situation = as.character(situation)) %>% 
  ml_random_forest(features,
                   type = "classification", # c("auto", "regression", "classification"),
                   #feature_subset_strategy = "sqrt",  # "auto" "all" "onethird" "sqrt" "log2" "n"
                   #impurity = "gini",  # "auto" "gini" "entropy"
                   #checkpoint_interval = 10,
                   #max_bins = 100,
                   max_depth = 24,
                   num_trees = 20)
                   #min_info_gain = 0,
                   #min_instances_per_node = 5,
                   #subsampling_rate = 1)
                   #seed = 6138,
                   #thresholds = NULL,
                   #cache_node_ids = FALSE,
                   #max_memory_in_mb = 1024)
responses_RF <- test %>% 
  select(features_col, situation) %>% 
  collect() %>% 
  mutate(pre_situ = predict(forest_model, test))
results_RF <- table(Prediction = responses_RF$pre_situ, Actual = responses_RF$situation)
rp(results_RF)
end_time <- Sys.time()
end_time - start_time

ml_feature_importances(forest_model)

###### ml_one_vs_rest TO ml_linear_svc

features_col2 <- c("station_id", "city2", "year", "month", "hour", "weekdays2", "Temperature", "Weather2",
                   "Wind", "Wind_Direction", "Humidity", "Barometer", "Visibility", "workday")
features2 <- "situation ~ station_id + city2 + year + month + hour + weekdays2 + Temperature + Weather2 + Wind + Wind_Direction + Humidity + Barometer + Visibility + workday"

train_test_status <- status %>% 
  ft_string_indexer(input_col = "city", output_col = "city2") %>% 
  ft_string_indexer(input_col = "weekdays", output_col = "weekdays2") %>% 
  ft_string_indexer(input_col = "Weather", output_col = "Weather2") %>% 
  select(features_col2, situation) %>% 
  group_by(situation) %>% 
  sdf_partition(training = 0.8, testing = 0.2, seed = 6138)
train <- train_test_status$training
test <- train_test_status$testing

start_time <- Sys.time()
  dfTrain <- train %>% 
    ft_vector_assembler(
      input_cols = features_col2,
      output_col = "features") %>%
    ft_string_indexer("situation", "label") %>% 
    select(features, label)
  OVR_SVC <- ml_one_vs_rest(sc,
                            #formula = features2,
                            classifier = ml_linear_svc(sc)) # 放入其他建模
                            #features_col = features_col2,
                            #label_col = "situation")
                            #prediction_col = "prediction",
                            #uid = random_string("one_vs_rest_"),...)
  #OVR_SVC
  SVC_Model <- ml_fit(OVR_SVC, dfTrain)
  ml_transform(SVC_Model, dfTrain)
  dfTest <- test %>% 
    ft_vector_assembler(
      input_cols = features_col2,
      output_col = "features") %>%
    select(features, situation)
  pred <- SVC_Model$.jobj%>%invoke("transform",spark_dataframe(dfTest))%>%collect()
  results <- table(Prediction = pred$prediction, Actual = pred$situation )
  rp(results)
end_time <- Sys.time()
end_time - start_time
