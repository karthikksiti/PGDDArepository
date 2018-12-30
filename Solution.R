
#MNIST Letter recognition using SVM
# 1. Business Understanding
# 2. Data Understanding
# 3. Data Preparation
# 4. Model Building 
#  4.1 Linear kernel
#  4.2 RBF Kernel
# 5 Hyperparameter tuning and cross validation
#
#1. Business Understanding
# The is a handwritten digit recognition problem. The handwritten image is scanned. 
####
# 2. Data understanding
#The MNIST database of handwritten digits has a training set of 60,000 examples, and a test set of 10,000 examples. 
#http://yann.lecun.com/exdb/mnist/ 
##3. Data Preparation: 
#Loading Neccessary libraries
# install.packages("caret")
# install.packages("kernlab")
# install.packages("dplyr")
# install.packages("readr")
# install.packages("ggplot2")
# install.packages("gridExtra")

library("caret")
library("kernlab")
library("dplyr")
library("readr")
library("ggplot2")
library("gridExtra")

#Loading data
mnist_test <- read.csv("mnist_test.csv", header = F, stringsAsFactors = F)
mnist_train <- read.csv("mnist_train.csv", header = F, stringsAsFactors = F)

#view the data
#View(mnist_train)

#Understanding Dimensions
dim(mnist_train) # 60000 obs and 785 variables
dim(mnist_test) # 10000 obs and 785 variables

#Structure of the dataset
str(mnist_train)

#printing first few rows
head(mnist_train)

#Exploring the data
summary(mnist_train)

#checking missing value

sapply(mnist_train, function(x) sum(is.na(x)))
# no missing values

# Check for duplicated rows
sum(duplicated(mnist_test)) 
sum(duplicated(mnist_train))

#no duplicate rows

#Making our target class to factor
mnist_train$V1 <- factor(mnist_train$V1)
mnist_test$V1 <- factor(mnist_test$V1)

# Utilize 16% of the training data for training as the data volume is very high, and will cost a lot computationally

set.seed(1)

train.indices = sample(1:nrow(mnist_train), 0.16*nrow(mnist_train))
test.indices = sample(1:nrow(mnist_test),0.16*nrow(mnist_test))
train = mnist_train[train.indices, ]
test = mnist_test[test.indices, ]

#Constructing Model

#Using Linear Kernel
Model_linear <- ksvm(V1~ ., data = train, scale = FALSE, kernel = "vanilladot")
Eval_linear<- predict(Model_linear, test)

#confusion matrix - Linear Kernel
confusionMatrix(Eval_linear,test$V1)
#Accuracy = 91.3%

#Using RBF Kernel
Model_RBF <- ksvm(V1~ ., data = train, scale = FALSE, kernel = "rbfdot")
Eval_RBF<- predict(Model_RBF, test)

#confusion matrix - RBF Kernel
confusionMatrix(Eval_RBF,test$V1)
# Accuracy : 0.9594 
#The accuracy of the RBF model is better than the Linear model

###  Hyperparameter tuning and Cross Validation 

# We will use the train function from caret package to perform Cross Validation. 

#traincontrol function Controls the computational nuances of the train function.
# i.e. method =  CV means  Cross Validation.
#      Number = 2 implies Number of folds in CV.

trainControl <- trainControl(method="cv", number=5)


# Metric <- "Accuracy" implies our Evaluation metric is Accuracy.

metric <- "Accuracy"

#Expand.grid functions takes set of hyperparameters, that we shall pass to our model.

set.seed(7)
grid <- expand.grid(.sigma=c(0.025, 0.05), .C=c(0.1,0.5,1,2,3,4) )


#train function takes Target ~ Prediction, Data, Method = Algorithm
#Metric = Type of metric, tuneGrid = Grid of Parameters,
# trcontrol = Our traincontrol method.

fit.svm <- train(V1~., data=train, method="svmRadial", metric=metric, 
                 tuneGrid=grid, trControl=trainControl)

print(fit.svm)

plot(fit.svm)
