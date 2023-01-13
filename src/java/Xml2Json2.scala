package com.cloudera

import org.apache.spark.sql._
import org.json._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode
import java.text.ParseException

object Xml2Json2 {

  def main(args: Array[String]) {
    val parse = udf((value: String) => XML.toJSONObject(value).toString())

    val schema_json = """{"type":"struct","fields":[{"name":"Event","type":{"type":"struct","fields":[{"name":"EventData","type":{"type":"struct","fields":[{"name":"Data","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"Name","type":"string","nullable":true,"metadata":{}},{"name":"content","type":"string","nullable":true,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"System","type":{"type":"struct","fields":[{"name":"Channel","type":"string","nullable":true,"metadata":{}},{"name":"Computer","type":"string","nullable":true,"metadata":{}},{"name":"Correlation","type":"string","nullable":true,"metadata":{}},{"name":"EventID","type":"long","nullable":true,"metadata":{}},{"name":"EventRecordID","type":"long","nullable":true,"metadata":{}},{"name":"Execution","type":{"type":"struct","fields":[{"name":"ProcessID","type":"long","nullable":true,"metadata":{}},{"name":"ThreadID","type":"long","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"Keywords","type":"string","nullable":true,"metadata":{}},{"name":"Level","type":"string","nullable":true,"metadata":{}},{"name":"Opcode","type":"string","nullable":true,"metadata":{}},{"name":"Provider","type":{"type":"struct","fields":[{"name":"Guid","type":"string","nullable":true,"metadata":{}},{"name":"Name","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"Security","type":"string","nullable":true,"metadata":{}},{"name":"Task","type":"string","nullable":true,"metadata":{}},{"name":"TimeCreated","type":{"type":"struct","fields":[{"name":"SystemTime","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"Version","type":"long","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}""" // Define Schema of your xml data in json.

    val schema = DataType.fromJson(schema_json).asInstanceOf[StructType] // Convert Json schema data to schema.

    val spark = SparkSession.builder().appName("XmlToJson").master("local[2]").getOrCreate()

    //Read Json Schema and Create Schema_Json
    val schema_json_new =spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("/Users/mbawa/Downloads/test.json").schema
    print(schema_json_new)

    //add the schema
    //val newSchema=DataType.fromJson(schema_json).asInstanceOf[StructType]

    //val df = spark.read.textFile(path = "file:///Users/mbawa/xml/test.xml");
    val df = spark.readStream.text(path = "file:///Users/mbawa/Downloads/xml/");

    val jsonDF = df.select(from_json(parse(col("value")), schema).as("Event")).select("Event.Event.*")
      .select("EventData.*", "System.*").withColumn("Data", map_from_entries(col("Data")))

    jsonDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      {
        val keyDF = batchDF.select(explode(map_keys(col("Data")))).distinct()
        val keyCols = keyDF.collect().map(f => f.get(0)).map(f => col("Data").getItem(f).as(f.toString))
        // var keyCols
        val finalDF = batchDF.select(col("*") +: keyCols: _*).drop("Data").drop("ProcessId")
        finalDF.select(to_json(struct(finalDF.columns.map(column): _*)).alias("value")).write.option("truncate", false).format("console").save()
      }
    }.start().awaitTermination()

    //newDf.write.json("/Users/mbawa/Downloads/json/test")

    // spark.stop()
  }
}