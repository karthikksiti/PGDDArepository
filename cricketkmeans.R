#Set working directory
cricket <- read.csv("Cricket.csv", stringsAsFactors = F)
cricket$SR <- scale(cricket$SR)
cricket$Ave <- scale(cricket$Ave)
crickkmeans <- cricket[,c(8,10)]
## Finding the optimal value of K

r_sq<- rnorm(20)

for (number in 1:20){clus <- kmeans(crickkmeans, centers = number, nstart = 50)
r_sq[number]<- clus$betweenss/clus$totss
}
plot(r_sq)

## Running the K-Means algorithm for K =4

clus4 <- kmeans(crickkmeans, centers = 4, iter.max = 50, nstart = 50)
## Appending the ClusterIDs to cricket data
cricket_km <- cbind(cricket,clus4$cluster)
colnames(cricket_km)[14] <- "ClusterID"
filter(cricket_km, ClusterID=2)

ggplot(cricket, aes(x = SR, y = Ave, colour = as.factor(cricket_km$ClusterID), label = Player)) + geom_point() + geom_text(size = 3)

