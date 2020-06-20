import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
        
        Run(producer, "events.json")
        producer.finalize()
    }
    properties["key.deserializer"] = StringDeserializer::class.java
    properties["value.deserializer"] = ByteArrayDeserializer::class.java
    properties["session.timeout.ms"] = "10000"
    
    val consumer1 = Consumer<ByteArray>(properties)
    val consumer2 = Consumer<ByteArray>(properties)
    val map = ConcurrentHashMap<String, Long>()
    GlobalScope.launch {
        val records = consumer1.consume("event_test")
        records.iterator().forEach { 
            map.put(it.key()!!, it.timestamp())
        }
        consumer1.finalize()
    }
    val meetups = consumer2.consume("MEETUP_EVENT_STREAM_DE")
    consumer2.finalize()
    
    Thread.sleep(5000L)

    val results = HashMap<String, Long>()

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