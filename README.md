# 資策會期末報告
* 資料清洗：使用Ｒ語言整理及彙整
* dataclean_status.R 主要資料清理，包含彙整五份天氣資料至weather.csv
* dataclean_30min.R 將時間壓縮成30分鐘一筆的整理
* dataclean_trip.R 主要從trip.csv中提取自行車站半個小時內借還車次數，匯入status.csv中
* 更新dataclean_trip.csv 擴展還車狀況欄位
* weatherType.R 將天氣類別減少至7種，並將風向欄位只保留角度數據
* trip_EDA.R 觀察trip檔，並作出數種數據特徵視覺化圖表
