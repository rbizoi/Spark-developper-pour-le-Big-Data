df <- read.df("/user/spark/donnees/postesSynop.csv", "csv",
               sep = ";", inferSchema = TRUE, header = TRUE)
str(df)
summary(df)
head(df)
q('yes')
