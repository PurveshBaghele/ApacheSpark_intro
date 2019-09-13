package group10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp

object Group10 {
	case class GData ( 
    GKGRECORDID: String, 
    Gdate: String, 
    SourceCollectionIdentifier: Integer,
    SourceCommonName: String,
    DocumentIdentifier: String,
	Counts: String,
	V2Counts: String,
	Themes: String,
    V2Themes: String,
	Locations: String,
	V2Locations: String,
	Persons: String,
    V2Persons: String,
	Organizations: String,
	V2Organizations: String,
	V2Tone: String,
    Dates: String,
	GCAM: String,
	SharingImage: String,
	RelatedImages: String,
    SocialImageEmbeds: String,
	SocialVideoEmbeds: String,
	Quotations: String,
	AllNames: String,
    Amounts: String,
	TranslationInfo: String,
	Extras: String 
  )
  /*case class SensorData ( 
    sensorName: String, 
    timestamp: Timestamp, 
    numA: Double,
    numB: Double,
    numC: Long,
    numD: Double,
    numE: Long,
    numF: Double
  )*/
  def main(args: Array[String]) {
	
	val schema = StructType( 
		Array( StructField("GKGRECORDID", StringType), 
			   StructField("Gdate", StringType), 
			   StructField("SourceCollectionIdentifier", IntegerType), 
			   StructField("SourceCommonName", StringType), 
			   StructField("DocumentIdentifier", StringType), 
			   StructField("Counts", StringType), 
			   StructField("V2Counts", StringType), 
			   StructField("Themes", StringType), 
			   StructField("V2Themes", StringType), 
			   StructField("Locations",StringType), 
			   StructField("V2Locations",StringType), 
			   StructField("Persons",StringType), 
			   StructField("V2Persons",StringType), 
			   StructField("Organizations",StringType), 
			   StructField("V2Organizations",StringType), 
			   StructField("V2Tone", StringType), 
			   StructField("Dates",StringType), 
			   StructField("GCAM", StringType), 
			   StructField("SharingImage", StringType), 
			   StructField("RelatedImages",StringType), 
			   StructField("SocialImageEmbeds",StringType), 
			   StructField("SocialVideoEmbeds",StringType), 
			   StructField("Quotations", StringType),
			   StructField("AllNames", StringType), 
			   StructField("Amounts",StringType), 
			   StructField("TranslationInfo",StringType), 
			   StructField("Extras", StringType)
        )
      )
	 /* val schema =
      StructType(
        Array(
          StructField("sensorname", StringType, nullable=false),
          StructField("timestamp", TimestampType, nullable=false),
          StructField("numA", DoubleType, nullable=false),
          StructField("numB", DoubleType, nullable=false),
          StructField("numC", LongType, nullable=false),
          StructField("numD", DoubleType, nullable=false),
          StructField("numE", LongType, nullable=false),
          StructField("numF", DoubleType, nullable=false)
        )
      )*/
	
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	
	val spark = SparkSession
      .builder
      .appName("Group10")
      .config("spark.master", "local")
      .getOrCreate()
	  
	val sc = spark.sparkContext // If you need SparkContext object

    import spark.implicits._

    val ds = spark.read 
                  .schema(schema) 
                  .csv("./data/segment/chutyap.csv")
                  .as[GData]
				  
	/*val ds = spark.read 
                  .schema(schema) 
                  .option("timestampFormat", "MM/dd/yy:hh:mm")
                  .csv("./data/segment/sensordata.csv")
                  .as[SensorData]	*/
	ds.take(1).foreach(println)			  

    //val dsFilter = ds.filter("Gdate = TIMESTAMP(\"20150219230000\")")

    //dsFilter.collect.foreach(println)  
    // ...

    spark.stop()
  }
}
