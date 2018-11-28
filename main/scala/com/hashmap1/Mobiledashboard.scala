package com.hashmap1
import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}

object MobileDashboard {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._
   /*val customschemaDistrict = StructType(
      List(
        StructField("State Code", IntegerType)
        , StructField("District Code", IntegerType)
        //, StructField("AccNumber", IntegerType)
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
        , StructField("Year", DateType)
         ))*/
 /*
      val extractPhoneNumber = udf((columnname: String) => {
        if (columnname != null && columnname.contains("*")) {
          if (columnname.length() == 11) {
            val extract = columnname.split("-")
            extract(1)
          }
          else {
            val extract1 = columnname.split("-")
            val extract = extract1(1).split("@")
            extract(0)
          }
        }
        else
          null
      })

  */
    val Districtdf: DataFrame = spark.sqlContext.read
      .format("csv")
      .option("useHeader", "true")
      //.option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      //.option("skipFirstRows",2)
      //.option("startColumn", 0)
      //.option("endColumn", 12)
      //.schema(customschemaDistrict)
      .load("C:\\Users\\Sesha\\Downloads\\DistrictsCodes2001.csv")


   val  skipable_first_row = Districtdf.first()
    val Districtdf1    = Districtdf.filter(row => row != skipable_first_row)
    Districtdf1.show()


    val WaterQualitydf: DataFrame = spark.sqlContext.read
      .format("csv")
      .option("useHeader", "true")
      //.option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      //.option("skipFirstRows", 2)      //.option("startColumn", 0)
      //.option("endColumn", 12)
      //.schema(customschemaWaterQuality)
      .load("C:\\Users\\Sesha\\Downloads\\IndiaAffectedWaterQualityAreas.csv")

    val first_row = WaterQualitydf.first()
    val WaterQualitydf1    = WaterQualitydf.filter(row => row != first_row)
    WaterQualitydf1.show()


    Districtdf1.printSchema()
    WaterQualitydf.printSchema()

    val Districtdf2:DataFrame = Districtdf1.select((Districtdf1.col("_c0")).as("State_Code").cast("int"),(Districtdf1.col("_c1")as("District_Code")).cast("int"),(Districtdf1.col("_c2")as("Name_of_the_State/Union_territory_and_Districts")).cast("String"))

    //val  districtdf1=Districtdf.toDF()ame of the State/Union territory and Districts

    val WaterQualitydf2:DataFrame = WaterQualitydf1.select((WaterQualitydf1.col("_c0")).as("State_Name"),(WaterQualitydf1.col("_c1")as("District_Name")),(WaterQualitydf1.col("_c2")as("Block_Name")),(WaterQualitydf1.col("_c3")as("Panchayat_Name")),(WaterQualitydf1.col("_c4")).as("Village_Name"),(WaterQualitydf1.col("_c5")).as("Habitation_Name"),(WaterQualitydf1.col("_c6")).as("Quality_Parameter"),(WaterQualitydf1.col("_c7")).as("Year").cast("Date"))
    WaterQualitydf2.printSchema()
    //WaterQualitydf.printSchema()

    //val districtdf2=districtdf1.filter(m=>m!=null)

    //val Districtdf3=Districtdf2.filter(Districtdf2.col("Name_of_the_State/Union_territory_and_Districts").contains('*'))

    //val districtdf4=Districtdf2.union(Districtdf3).except(Districtdf3.intersect(Districtdf2))
  val finalDF =Districtdf2.join(WaterQualitydf2)//val finalDf = spark.sqlContext.sql("SELECT * FROM Districtdf2 CROSS JOIN WaterQualitydf2 on Districtdf2.Name_of_the_State/Union_territory_and_Districts == WaterQualitydf2.State_Name")

    //Districtdf2.createOrReplaceTempView("df")
    //WaterQualitydf2.createOrReplaceTempView("waterdf")
    finalDF.show()

    //finalDf.write.format("orc").mode("append").saveAsTable("default.table1")
  }

}
