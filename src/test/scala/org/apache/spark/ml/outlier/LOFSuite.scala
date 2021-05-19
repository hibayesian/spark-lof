/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.outlier

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._

object LOFSuite {
  def main(args: Array[String]): Unit = {
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
    result.sort(desc("lof")).show()
    println("Total time = " + (endTime - startTime) / 1000.0 + "s")
    spark.close()
  }
}
