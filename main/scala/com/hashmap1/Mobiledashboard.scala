package com.hashmap1

import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object MobileDashboard {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val customschemaDistrict = StructType(
      List(
        StructField("State Code", IntegerType)
        , StructField("District Code", IntegerType)
        , StructField("Name of the State/Union territory and Districts", DataTypes.StringType)
      ))

    val customschemaWaterQuality = StructType(
      List(
        StructField("State Name", DataTypes.StringType)
        , StructField("District Name", DataTypes.StringType)
        , StructField("Block Name", DataTypes.StringType)
        , StructField("Panchayat Name", DataTypes.StringType)
        , StructField("Village Name", DataTypes.StringType)
        , StructField("Habitation Name", DataTypes.StringType)
        , StructField("Quality Parameter", DataTypes.StringType)
        , StructField("Year", DataTypes.StringType)
         ))

    val Districtdf: DataFrame = spark.sqlContext.read
      .format("csv")
      .option("useHeader", "true")
      .option("inferSchema", "false")
      .schema(customschemaDistrict)
      .load("C:\\Users\\Sesha\\Downloads\\DistrictsCodes2001.csv")


    val  skipable_first_row = Districtdf.first()
    val Districtdf1    = Districtdf.filter(row => row != skipable_first_row)

    val updatedDf = Districtdf1.withColumn("District Code", when($"District Code" === "0", "-999")otherwise($"District Code"))

    //val dfwithoutasterisk=updatedDf.withColumn("Name of the State/Union territory and Districts",when($"Name of the State/Union territory and Districts".contains(lit("*")),null)otherwise($"Name of the State/Union territory and Districts"))

    val finaldf=updatedDf.filter(!col("Name of the State/Union territory and Districts").contains(lit("*")))

    val finaldf1=finaldf.withColumn("Name of the State/Union territory and Districts", lower(col("Name of the State/Union territory and Districts")))

    val WaterQualitydf: DataFrame = spark.sqlContext.read
      .format("csv")
      .option("useHeader", "true")
      .option("inferSchema", "false")
      .schema(customschemaWaterQuality)
      .load("C:\\Users\\Sesha\\Downloads\\IndiaAffectedWaterQualityAreas.csv")

    val first_row = WaterQualitydf.first()
    val WaterQualitydf1    = WaterQualitydf.filter(row => row != first_row)

    val WaterQualitydf3=WaterQualitydf1.withColumn("District Name", lower(col("District Name")))

    val df4=WaterQualitydf3.withColumn("District Name",regexp_replace(WaterQualitydf3.col("District Name"), "[(0-9)]", ""))

    val finalDF =finaldf1.join(df4,df4("District Name")===finaldf1("Name of the State/Union territory and Districts"),"inner")

    val finalDF1=finalDF.drop("Name of the State/Union territory and Districts")

    val df1=finalDF1.groupBy(col("Village Name"),col("Year"),col("Quality Parameter")).count.as("Frequency")

    df1.show(150)

    //df1.write.format("orc").mode("append").saveAsTable("default.table1")
}
}
