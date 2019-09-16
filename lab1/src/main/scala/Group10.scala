package group10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.split 
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp

object Group10 {
	
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
			   StructField("Gdate", TimestampType), 
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
                  .option("timestampFormat", "yyyyMMddhhmmss") 
                  .option("sep","\t")
                  .csv("./data/segment/20150218230000.gkg.csv")
                  .as[GData]
	
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
	/*val ds = spark.read 
                  .schema(schema) 
                  .option("timestampFormat", "MM/dd/yy:hh:mm")
                  .csv("./data/segment/sensordata.csv")
                  .as[SensorData]	*/	
    import spark.implicits._	  
    var filter1=ds.select("GDate","AllNames").groupBy("GDate").agg(collect_list("AllNames"))  
     var filter2=ds.select("AllNames")
   
// val urlCleaner = (s:String) => {
//    if (s == null) null else s.replaceAll(";",",")
// }
val urlCleaner = (s:String) => {
     if (s == null) null else s.replaceAll(";"," ").replaceAll("\\d+(?:[.,]\\d+)*\\s*", "")
}
val url_cleaner_udf = udf(urlCleaner)

val splitCleaner = (s:String) => {
     if (s == null) null else s.split(",").map(word=> (word, 1))
}
val split_cleaner_udf = udf(splitCleaner)

val mapCleaner = (s: Array[String]) => {
     if (s == null) null else finalArray.map(word=> (word, 1))
}
val map_cleaner_udf = udf(mapCleaner)

val df = filter2.withColumn("AllNames", url_cleaner_udf(filter2("AllNames")) )
// var q1 =df.select("AllNames").map(r => r.getString(0)).collect.toList
// var s1=q1.mkString(" ")
// val finalArray = s1.split(",")
// val counts = finalArray.map(word=> (word, 1))
// val rdd = sc.parallelize(counts)
// val reducedRDD = rdd.reduceByKey(_ + _)
// val finalThing = reducedRDD.collect
finalThing.sortBy{ case (x,y) => (-y) }
   
    spark.stop()
  }
}

case class GData ( 
    GKGRECORDID: String, 
    Gdate: Timestamp, 
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