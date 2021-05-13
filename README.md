# Spark-LOF
In anomaly detection, the local outlier factor(LOF) algorithm is based on a concept of a local density, where locality is given by k nearest neighbors, whose distance is used to estimate the density. By comparing the local density of an object to the local densities of its neighbors, one can identify regions of similar density, and points that have a substantially lower density than their neighbors. Due to the local approach, LOF is able to identify outliers in a data set that would not be outliers in another area of the data set. Spark-LOF is a parallel implementation of local outlier factor based on Spark.

# Examples
## Scala API
```scala
val spark = SparkSession
  .builder()
  .appName("LOFExample")
  .master("local[4]")
  .getOrCreate()

val schema = new StructType(Array(
  new StructField("col1", DataTypes.DoubleType),
  new StructField("col2", DataTypes.DoubleType)))
val df = spark.read.schema(schema).csv("data/outlier.csv")

val assembler = new VectorAssembler()
  .setInputCols(df.columns)
  .setOutputCol("features")
val data = assembler.transform(df).repartition(4)

val startTime = System.currentTimeMillis()
val result = new LOF()
  .setMinPts(5)
  .transform(data)
val endTime = System.currentTimeMillis()
result.count()
    
// Outliers have much higher LOF value than normal data
result.sort(desc(LOF.lof)).head(10).foreach { row =>
  println(row.get(0) + " | " + row.get(1) + " | " + row.get(2))
}
println("Total time = " + (endTime - startTime) / 1000.0 + "s")
```

# Requirements
Spark-LOF is built against Spark 3.1.1.

# Build From Source
```scala
sbt assembly
```

# Licenses
Spark-LOF is available under Apache Licenses 2.0.

# Contact & Feedback
If you encounter bugs, feel free to submit an issue or pull request. Also you can mail to:
+ hibayesian (hibayesian@gmail.com).
