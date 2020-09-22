df <- read.df("file:/home/spark/postesSynop.csv", "csv",
               sep = ";", inferSchema = TRUE, header = TRUE)
str(df)

head(df)
q('yes')
