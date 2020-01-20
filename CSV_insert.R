rm(list=ls())
library(dplyr)

##### Connection String 
conString <- "Driver=SQL Server;Server=server_address;Database=DB_Name;Uid=User_ID;Pwd=Password!!"

##### File Path where the csv file was saved
filesPath <- "D:/csv_data_folder/"

allFiles <- dir(path = filesPath)
csvFiles <- allFiles[grep("[.]csv", allFiles)]
print(csvFiles)

##### CSV Files
csvPath <- paste(filesPath, csvFiles, sep="")
print(csvPath)
##### Rbind
# Originally the number of data column should be 50, but sometimes value1-3 was missed in the file.
new_csv <- data.frame()
for(i in 1:length(csvPath)){
  tempdf0 <- read.csv(file = csvPath[i], stringsAsFactors = FALSE)
  if(ncol(tempdf0)==50) {
    tempdf1<-tempdf0
  } else {tempdf0[,c("value1",	"value2",	"value3")]<-NA
  tempdf1<-tempdf0
  }
  new_csv <- rbind(new_csv, tempdf1)
}

names(new_csv) <- gsub(".", "_", names(new_csv), fixed=TRUE)

##### Sorting Data
new_csv <- new_csv[order(new_csv$SalesID, new_csv$PrdNo), ]

##### Type transformation
new_csv$Notes <- as.character(new_csv$Notes)
new_csv$Tilt  <- as.character(new_csv$Tilt)

print(names(new_csv))
print(sapply(new_csv, class))

##### Column Order
ColOrder <- c(
  "PrdNo","Date","Time","....","value1","value2","value3"
)
new_csv <- new_csv[, c(ColOrder)]

# Data Check
print(
  new_csv %>%
    group_by(SalesID)%>%
    count()
)


##### Insert Temp (tempcsv)
TempTable_Con <- RxSqlServerData(connectionString = conString, table = "tempcsv")
rxDataStep(inData = new_csv, outFile = TempTable_Con, overwrite = TRUE)

##### Insert new cSV Data into old_csv_files
outOdbcDS <- RxOdbcData(table = "old_csv_files",           
                        connectionString = conString,
                        useFastRead=TRUE)
rxOpen(outOdbcDS, "w")                       
rxExecuteSQLDDL(outOdbcDS, sSQLString = "insert into old_csv_files select * from tempcsv")
