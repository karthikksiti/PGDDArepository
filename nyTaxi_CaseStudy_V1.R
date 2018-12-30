#load SparkR
spark_path <- '/usr/local/spark'

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "yarn", sparkConfig = list(spark.driver.memory = "1g"))

# Create a Spark DataFrame and examine structure
nytaxi_2015 <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", source = "csv",
                    header = TRUE, inferSchema = TRUE)
nytaxi_2016 <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", source = "csv",
                       header = TRUE, inferSchema = TRUE)
nytaxi_2017 <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", source = "csv",
                       header = TRUE, inferSchema = TRUE)
# Check the Spark Dataframe
head(nytaxi_2015)
head(nytaxi_2016)
head(nytaxi_2017)

#Examine the data

# 1) Find the total number of tickets for each year.

nrow(nytaxi_2015)
# Total no of tickets in 2015 are: 11809233
nrow(nytaxi_2016)
# Total no of tickets in 2016 are: 10626899
nrow(nytaxi_2017)
# Total no of tickets in 2017 are: 10803028

# 2)Find out the number of unique states from where the cars that got parking tickets came from.
#(Hint: Use the column 'Registration State')
#There is a numeric entry in the column which should be corrected.
#Replace it with the state having maximum entries. Give the number of unique states for each year again.


# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")

# For using SQL, you need to create a temporary view
createOrReplaceTempView(nytaxi_2015, "data_2015")

# Lets first find the state with highest tickets
regStates2015 <- SparkR::sql("SELECT `Registration State` , COUNT(*) as cnt FROM data_2015 GROUP BY `Registration State` Order by cnt DESC limit 5")
head(regStates2015)
# NY 9193289 : NY is the state with highest tickets in 2015
# Lets replace any numerical values in the Registration State column with 'NY'
# To check what numerical values are used as Registration state:
head(SparkR::sql("select `Registration State` from data_2015 where `Registration State` not rlike '[^0-9]'"))
# all the numerical values used are same - 99
## Lets replace and count the unique states in 2015

head(SparkR::sql("SELECT count(DISTINCT replace(`Registration State`, '99', 'NY')) as uniqueStates2015 from data_2015"))

## Number of unique states that were issued tickets in 2015 is 68

## Repeating above  for 2016:
createOrReplaceTempView(nytaxi_2016, "data_2016")
## Finding the state with maximum tickets
regStates2016 <- SparkR::sql("SELECT `Registration State` , COUNT(*) as cnt FROM data_2016 GROUP BY `Registration State` Order by cnt DESC limit 5")
head(regStates2016)
# NY 8260189 : NY is the state with highest tickets in 2016

# Lets replace any numerical values in the Registration State column with 'NY'
# To check what numerical values are used as Registration state:
head(SparkR::sql("select `Registration State` from data_2016 where `Registration State` not rlike '[^0-9]'"))
# all the numerical values used are same - 99
## Lets replace and count the unique states in 2016

head(SparkR::sql("SELECT count(DISTINCT replace(`Registration State`, '99', 'NY')) as uniqueStates2016 from data_2016"))

## Number of unique states that were issued tickets in 2016 is 67

## For 2017

createOrReplaceTempView(nytaxi_2017, "data_2017")
## Finding the state with maximum tickets
regStates2017 <- SparkR::sql("SELECT `Registration State` , COUNT(*) as cnt FROM data_2017 GROUP BY `Registration State` Order by cnt DESC limit 5")
head(regStates2017)
# NY 8481061 : NY is the state with highest tickets in 2017

# Lets replace any numerical values in the Registration State column with 'NY'
# To check what numerical values are used as Registration state:
head(SparkR::sql("select `Registration State` from data_2017 where `Registration State` not rlike '[^0-9]'"))
# all the numerical values used are same - 99
## Lets replace and count the unique states in 2017

head(SparkR::sql("SELECT count(DISTINCT replace(`Registration State`, '99', 'NY')) as uniqueStates2017 from data_2017"))

## Number of unique states that were issued tickets in 2017 is 66

## Observation: While there is no particular trend in the total now of tickets, no os states with no tickets shows a health sign.

#3) Some parking tickets don’t have the address for violation location on them, which is a cause for concern. 
#Write a query to check the number of such tickets.

##2015
## lets examine the  top 5 violation locaitons
head(SparkR::sql("Select `Violation Location`, COUNT(*) as cnt  from data_2015 GROUP BY `Violation Location` Order by cnt DESC limit 5"))
## Violation Location     cnt                                                    
#1                 NA 1799170
#2                 19  598351
#3                 18  427510
#4                 14  409064
#5                  1  329009
# From the above it is evident that In 2015, there are 1799170 tickets without a location attributed.

#2016
#Violation Location     cnt                                                    
#1                 NA 1868656
#2                 19  554465
#3                 18  331704
#4                 14  324467
#5                  1  303850
# From the above it is evident that In 2016, there are 1868656 tickets without a location attributed.

#2017
#  Violation Location     cnt                                                    
#1                 NA 2072400
#2                 19  535671
#3                 14  352450
#4                  1  331810
#5                 18  306920
# From the above it is evident that In 2017, there are 2072400 tickets without a location attributed.
# Comparison: The no of tickets without a location is increasing year over year.
##Aggregation tasks

#How often does each violation code occur? Display the frequency of the top five violation codes.
#2015
head(SparkR::sql("Select `Violation Code`, COUNT(*) as cnt  from data_2015 GROUP BY `Violation Code` Order by cnt DESC limit 5"))

#Violation Code     cnt                                                        
#1             21 1630912
#2             38 1418627
#3             14  988469
#4             36  839197
#5             37  795918
#2016
head(SparkR::sql("Select `Violation Code`, COUNT(*) as cnt  from data_2016 GROUP BY `Violation Code` Order by cnt DESC limit 5"))

#Violation Code     cnt                                                        
#1             21 1531587
#2             36 1253512
#3             38 1143696
#4             14  875614
#5             37  686610

#2017
head(SparkR::sql("Select `Violation Code`, COUNT(*) as cnt  from data_2017 GROUP BY `Violation Code` Order by cnt DESC limit 5"))

#Violation Code     cnt                                                        
#1             21 1528588
#2             36 1400614
#3             38 1062304
#4             14  893498
#5             20  618593

# Wrong Parking (21) and Failing to show a parking ticket (38) and Speeding over the limit(21) are consistently high in all three years.


#How often does each 'vehicle body type' get a parking ticket? 
#How about the 'vehicle make'? (Hint: find the top 5 for both)

#2015
head(SparkR::sql("Select `Vehicle Body Type`, COUNT(*) as cnt  from data_2015 GROUP BY `Vehicle Body Type` Order by cnt DESC limit 10"))
#Vehicle Body Type     cnt                                                     
#1              SUBN 3729346
#2              4DSD 3340014
#3               VAN 1709091
#4              DELV  892781
#5               SDN  524596
#6              2DSD  319046
#Make
head(SparkR::sql("Select `Vehicle Make`, COUNT(*) as cnt  from data_2015 GROUP BY `Vehicle Make` Order by cnt DESC limit 10"))
#Vehicle Make     cnt                                                          
#1         FORD 1521874
#2        TOYOT 1217087
#3        HONDA 1102614
#4        NISSA  908783
#5        CHEVR  897845
#6        FRUEH  432073

#2016
head(SparkR::sql("Select `Vehicle Body Type`, COUNT(*) as cnt  from data_2016 GROUP BY `Vehicle Body Type` Order by cnt DESC limit 10"))
#Vehicle Body Type     cnt                                                     
#1              SUBN 3466037
#2              4DSD 2992107
#3               VAN 1518303
#4              DELV  755282
#5               SDN  424043
#6              2DSD  276455

#Make
head(SparkR::sql("Select `Vehicle Make`, COUNT(*) as cnt  from data_2016 GROUP BY `Vehicle Make` Order by cnt DESC limit 10"))
#Vehicle Make     cnt                                                          
#1         FORD 1324774
#2        TOYOT 1154790
#3        HONDA 1014074
#4        NISSA  834833
#5        CHEVR  759663
#6        FRUEH  423590
#2017
head(SparkR::sql("Select `Vehicle Body Type`, COUNT(*) as cnt  from data_2017 GROUP BY `Vehicle Body Type` Order by cnt DESC limit 10"))
#Vehicle Body Type     cnt                                                     
#1              SUBN 3719802
#2              4DSD 3082020
#3               VAN 1411970
#4              DELV  687330
#5               SDN  438191
#6              2DSD  274380

#Make
head(SparkR::sql("Select `Vehicle Make`, COUNT(*) as cnt  from data_2017 GROUP BY `Vehicle Make` Order by cnt DESC limit 10"))
#Vehicle Make     cnt                                                          
#1         FORD 1280958
#2        TOYOT 1211451
#3        HONDA 1079238
#4        NISSA  918590
#5        CHEVR  714655
#6        FRUEH  429158

# Observation: Sub-Urban and 4 Door Sedans consistently got the highest tickets in all three years
# From a make point of view, Ford , Toyota and Honda makes attracted the highest tickets. 
# This could possibly becuase of the distribution of total no of cars in US 

