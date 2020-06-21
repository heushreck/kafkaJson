import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Min
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import java.util.concurrent.ConcurrentHashMap


fun main() {
    val properties = Properties()
    properties["bootstrap.servers"] = "localhost:29092"
    properties["group.id"] = "test"
    properties["key.serializer"] = StringSerializer::class.java
    properties["value.serializer"] = ByteArraySerializer::class.java
    GlobalScope.launch {
        delay(2000L)
        val producer = Producer(properties)
        val time_before = System.currentTimeMillis()
        Run(producer, "events-big.json")
        val time_after = System.currentTimeMillis()
        println("time before: " + time_before + " time after: " + time_after + " diff: " + (time_after - time_before))
        producer.finalize()
    }
    properties["key.deserializer"] = StringDeserializer::class.java
    properties["value.deserializer"] = ByteArrayDeserializer::class.java
    properties["session.timeout.ms"] = "10000"
    properties["enable.auto.commit"] = "true"
    properties["auto.offset.reset"] = "earliest"
    val consumer1 = Consumer(properties)
    val map = ConcurrentHashMap<String, Long>()
    GlobalScope.launch {
        var records = consumer1.consume("event_test")
        consumer1.finalize()
        records.iterator().forEach { 
            map.put(it.key()!!, it.timestamp())
        }
    }
    val consumer2 = Consumer(properties)
    val meetups = consumer2.consume("MEETUP_EVENT_STREAM_DE")
    consumer2.finalize()
    
    Thread.sleep(30000L)
    val results = hashMapOf<String, Long>()
    println(map.size)
    meetups.iterator().forEach { 
        val key = it.key()!!
        val timestamp = map[key]
        if (timestamp != null){
            results.put(key, timestamp - it.timestamp())
        }        
    }
    println(results)
   
}