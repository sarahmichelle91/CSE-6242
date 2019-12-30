// Databricks notebook source
// Q2 [25 pts]: Analyzing a Large Graph with Spark/Scala on Databricks

// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Definfing the data schema
val customSchema = StructType(Array(StructField("answerer", IntegerType, true), StructField("questioner", IntegerType, true),
    StructField("timestamp", LongType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
// MAKE SURE THAT YOU REPLACE THE examplegraph.csv WITH THE mathoverflow.csv FILE BEFORE SUBMISSION.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "false") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/mathoverflow.csv")
   .withColumn("date", from_unixtime($"timestamp"))
   .drop($"timestamp")

// COMMAND ----------

//display(df)
df.show()

// COMMAND ----------

// PART 1: Remove the pairs where the questioner and the answerer are the same person.
// ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

// ENTER THE CODE BELOW
var df1 = df.filter($"answerer" =!= $"questioner")
df1.show()

// COMMAND ----------

// PART 2: The top-3 individuals who answered the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest out-degrees.

// ENTER THE CODE BELOW
var df2 = df1.groupBy($"answerer").agg(count($"answerer") as "questions_answered").orderBy($"questions_answered".desc, $"answerer".asc).limit(3)
df2.show()

// COMMAND ----------

// PART 3: The top-3 individuals who asked the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest in-degree.

// ENTER THE CODE BELOW
var df3 = df1.groupBy($"questioner").agg(count($"questioner") as "questions_asked").orderBy($"questions_asked".desc, $"questioner".asc).limit(3)
df3.show() 

// COMMAND ----------

// PART 4: The top-5 most common asker-answerer pairs - sorted in descending order - if tie, the one with lower value node-id in the first column (u->v edge, u value) gets listed first.

// ENTER THE CODE BELOW
var df4 = df1.groupBy($"answerer", $"questioner").count().orderBy($"count".desc, $"answerer".asc, $"questioner".asc).limit(5)
df4.show() 

// COMMAND ----------

// PART 5: Number of interactions (questions asked/answered) over the months of September-2010 to December-2010 (i.e. from September 1, 2010 to December 31, 2010). List the entries by month from September to December.

// Reference: https://www.obstkel.com/blog/spark-sql-date-functions
// Read in the data and extract the month and year from the date column.
// Hint: Check how we extracted the date from the timestamp.

// ENTER THE CODE BELOW
var df5 = df1.withColumn("month", month($"date")).withColumn("year", year($"date")).filter($"month" >= 9 and $"year" === 2010).groupBy($"month").agg(count($"month") as "total_interactions").orderBy($"month".asc)
df5.show()

// COMMAND ----------

// PART 6: List the top-3 individuals with the maximum overall activity, i.e. total questions asked and questions answered.

// ENTER THE CODE BELOW
var df6_1 = df1.groupBy($"answerer").agg(count($"answerer") as "questions_answered").withColumnRenamed("answerer", "userID")
var df6_2 = df1.groupBy($"questioner").agg(count($"questioner") as "questions_asked").withColumnRenamed("questioner", "userID")
var df6_3 = df6_1.join(df6_2, Seq("userID"), "full").na.fill(0)
var df6 = df6_3.groupBy("userID").agg(sum($"questions_answered" + $"questions_asked") as "total_activity").orderBy($"total_activity".desc, $"userID".asc).limit(3)
df6.show()
