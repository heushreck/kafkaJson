import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

fun main() {
    val properties = Properties()
    properties["bootstrap.servers"] = "localhost:29092"
    properties["group.id"] = "test3"
    properties["key.serializer"] = StringSerializer::class.java
    properties["value.serializer"] = ByteArraySerializer::class.java
    GlobalScope.launch {
        delay(2000L)
        val producer = Producer(properties)
        //sampleRun(producer)
        Run(producer, "events.json")
        producer.finalize()
    }
    properties["key.deserializer"] = StringDeserializer::class.java
    properties["value.deserializer"] = ByteArrayDeserializer::class.java
    properties["session.timeout.ms"] = "10000"
    //properties["auto.offset.reset"] = "earliest"
    val consumer1 = Consumer<ByteArray>(properties)
    val consumer2 = Consumer<ByteArray>(properties)
    GlobalScope.launch {
        consumer1.consume("event_test")
    }
    consumer2.consume("MEETUP_EVENT_STREAM_DE")
    consumer1.finalize()
    consumer2.finalize()
    Thread.sleep(5000L)
}