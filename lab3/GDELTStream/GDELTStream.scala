package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit


import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import scala.collection.JavaConversions._
import org.apache.kafka.streams.kstream.{Transformer,TransformerSupplier}

import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.processor.Cancellable

object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally,
  // write the result to a new topic called gdelt-histogram.
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

val topic:KStream[String, String] = records.map((date,str)=> (date,str.split("\t"))) 
                                                .filter((date,col)=>col.size>23 && col(23)!="") //get allNames column
                                                .map((date,str)=>(date, str(23).split(";")
																			   .map(s=>s.split(",")(0)).distinct //get the topic
                                                                               .mkString(","))) 
                                                .flatMapValues(topic => topic.toLowerCase.split(",")) 

 //Print filtered topics to console
 val sysout = Printed.toSysOut[String, String].withLabel("allNames")
//topic.print(sysout)
  

  // Publish to topic allNames
  //topic.to("allNames")


  //state store code is referenced from Kafka Documentation
  //https://kafka.apache.org/20/javadoc/org/apache/kafka/streams/kstream/KStream.html#transform-org.apache.kafka.streams.kstream.TransformerSupplier-java.lang.String...-
  //Build a persistent  state store
  val keyValueStoreBuilder: StoreBuilder[KeyValueStore[String,Long]] = 
  								Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("topicstate"),
															Serdes.String,
															Serdes.Long);
  // register store
  builder.addStateStore(keyValueStoreBuilder);

  //Transform each record of the input stream
  val outputStream: KStream[String, Long] = topic.transform(new HistogramTransformer(), "topicstate");
  
  //publish Kafka topic
  outputStream.to("gdelt-histogram")

//print outputStream to console for debugging
val sysprint = Printed.toSysOut[String, Long].withLabel("gdelt-hist")
outputStream.print(sysprint)

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

}

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends TransformerSupplier[String, String, KeyValue[String, Long]] {
  

// returns a transformer object
def get(): Transformer[String, String, KeyValue[String, Long]] = {  
	
	
	return new Transformer[String, String, KeyValue[String, Long]](){ 
		var context: ProcessorContext = _
		var topicStore: KeyValueStore[String, Long] = _
		val t2 = System.nanoTime 
		var total_len=0 

		  // Initialize Transformer object
		  def init(context: ProcessorContext) {
		    this.context = context
			//fetch the state store instance
		    this.topicStore = context.getStateStore("topicstate").asInstanceOf[KeyValueStore[String, Long]];
		  }

		  // Should return the current count of the name during the _last_ hour
		  def transform(key: String, name: String): KeyValue[String, Long] = {
		    
		    val count = TopicCount_increment(name) // Increment the topic count
		    
			//calculate average bytes per second consumed by transformer 
			// val duration2 = (System.nanoTime - t2) / 1e9d
			// total_len= total_len+name.length()+8    //size of string + size of long
			// println(" Average bytes per second- "+(total_len)/duration2)
			
			//schedule a cron_job to decrement topic count after 1 hour
			var cron_job: Cancellable = null
		    cron_job = this.context.schedule(1000 * 60 * 60, PunctuationType.WALL_CLOCK_TIME, (timestamp) => {
		      			TopicCount_decrement(name) // Decrement the topic count after 1 hour
		      			cron_job.cancel() // decremenmt once
		    			});
		    
			return new KeyValue(name,count)
		  }

		  
		//decrement topic count after 1 hour
		  def TopicCount_decrement(topic: String) = {
			var count = this.topicStore.get(topic)
		   if (count!=null) {
		      count = count - 1
			  if (count > 0) {
		      	this.topicStore.put(topic, count)
		    	}
		      else{
				  this.topicStore.delete(topic)
			  }
		   }else{
			   count= 0
			   this.topicStore.delete(topic)
		   }
			  this.context.forward(topic, count)
		  }

		//increment topic count
		  def TopicCount_increment(topic: String): Long = {
			
			var topic_cnt = this.topicStore.get(topic)
		    if (topic_cnt>0) {
		      topic_cnt = topic_cnt + 1
		    } else {
		      topic_cnt = 1L
		    }
		    this.topicStore.put(topic, topic_cnt)
		    return topic_cnt
		  }

		  // Close any resources if any
		  def close() {
		  }
		}//end of transformer
	}//end of get method
}//end of HistogramTransform