from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.sql import SparkSession

# Set up Spark session
spark = SparkSession.builder.appName("TFIDF").getOrCreate()

# Now I'll load my Top 10 ham CSV file
data = spark.read.csv("s3://ca675spark/data/top_10_spam_accounts.csv", header=True, inferSchema=True)

# Tokenize the text column
tokenizer = Tokenizer(inputCol="message", outputCol="word_count")
words_data = tokenizer.transform(data)

# Calculating TF for the input column
hashingTF = HashingTF(inputCol="word_count", outputCol="raw_columns", numFeatures=500)
featurized_data = hashingTF.transform(words_data)

# Calculating IDF for the input column
idf = IDF(inputCol="raw_columns", outputCol="features")
idf_model = idf.fit(featurized_data)
tfidf_data = idf_model.transform(featurized_data)

# Now I'll print the results obtained !!!
tfidf_data.select("message", "features").show(truncate=False)

# Stop Spark session
spark.stop()
