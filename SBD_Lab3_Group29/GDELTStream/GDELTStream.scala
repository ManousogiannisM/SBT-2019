package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import scala.collection.JavaConversions._
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.Cancellable

object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally, 
  // write the result to a new topic called gdelt-histogram. 
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")
   												.mapValues(v => v.split("\t",-1)) //split into columns with tab seperator
												.filter((_,v) => v.length>23 )   // keep only rows with more than 23 rows to avoid Index Out of Range Exception
												.mapValues(a => a(23))           // keep only 'topics' column
												.flatMapValues(topics => (topics.split(';').map(_.split(',')(0))))  //split with ; and keep only names
												.filter((_,v) => v!="" && v!="Type ParentCategory")   // exclude false positives
 

  //Initializing the state store
  val myStateStore: StoreBuilder[KeyValueStore[String, Long]]=  Stores.keyValueStoreBuilder(
																		Stores.persistentKeyValueStore("myStateStore"),
																		Serdes.String,
																		Serdes.Long);	
  // register store to streamBuilder
  builder.addStateStore(myStateStore);

//store the transformed output stream in the format (String,Long) to topic gdelt-histogram.
  val outputstream: KStream[String, Long] = records.transform(new HistogramTransformer(), "myStateStore")
  outputstream.to("gdelt-histogram")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var mystate: KeyValueStore[String, Long] = _
  
  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    this.mystate = context.getStateStore("myStateStore").asInstanceOf[KeyValueStore[String, Long]]; //get our created state store. (needed to cast it as KeyValueStore object)
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    //increase the counter for every record arriving
	var counter = this.mystate.get(name)
      counter = counter + 1
    
    this.mystate.put(name, counter)
	
	//schedule a delete of the record count after one hour (3600000 millisec-> 1 hour). Punctuation type based on clock time as requested.
    var scheduled: Cancellable = null
    scheduled = this.context.schedule(3600000, PunctuationType.WALL_CLOCK_TIME, (timestamp) => {
    
	var counter2 = this.mystate.get(name)
    if (counter2 == 1) {
      counter2 = 0
      this.mystate.delete(name)
    } else {
      counter2 = counter2 - 1
    }
    if (counter2 >= 0) {
      this.mystate.put(name, counter2)
    }
	
    this.context.forward(name, counter2) 
      scheduled.cancel() // after the record is deleted the scheduled task does not need to be repeated
    })
    return (name, counter)
  }



  // Close any resources if any
  def close() {
  }
}