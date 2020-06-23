import kotlinx.coroutines.*
import kotlinx.serialization.*
import kotlinx.serialization.json.*
import kotlinx.serialization.internal.*
import kotlinx.serialization.builtins.*
import java.util.*
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.long
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.time.Duration

val config = HashMap<String, String>()
val json = Json(JsonConfiguration(ignoreUnknownKeys = true))

fun main(args: Array<String>) = Kafka().subcommands(Produce(), Consume()).main(args)

class Kafka : CliktCommand(
    help = """Kafka is a command line tool to either produce or consume
    kafka topics.""") {
    
    val serverAddress: String? by option(help = "Server address of Kafka Broker.")

    override fun run() {
        var address: String
        if (this.serverAddress != null) {
            address = this.serverAddress.toString()
        } else {
            address = "localhost:29092"
        }
        config["server-address"] = address        
    }
}

class Produce : CliktCommand(help = "Starts the Kafka Producer") {
    
    val groupID: String by option(help = "Group ID of Producer.").default("test")
    val fileName: String by option(help = "JSON file to read events from.").default("events.json")
    val topic: String by option(help = "Topic to produce events to.").default("event_test")
    val props = Properties()

    override fun run() {
        // Populate producer properties
        props["bootstrap.servers"] = config["server-address"]
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = ByteArraySerializer::class.java
        props["group.id"] = groupID
               
        println(String.format("Kafka Broker: %s, Group ID: %s, Topic: %s, Event File: %s", config["server-address"], groupID, topic, fileName))
        val producer = Producer(props, json)

        val time_before = System.currentTimeMillis()
        Run(topic, producer, fileName)
        val time_after = System.currentTimeMillis()
        val diff = time_after - time_before
        val file_size = File(fileName).length()
        println(String.format("Start: %d, End: %d, Difference: %d ms, File Size: %d bytes", time_before, time_after, diff, file_size))
        
        producer.finalize()        
    }
}

class Consume : CliktCommand(help = "Starts the Kafka Consumer") {
    
    val groupID: String? by option(help = "Group ID of Consumer.").default("test")
    val topic1: String by option(help = "Topic of first consumer routine.").default("event_test")
    val topic2: String by option(help = "Topic of second consumer routine.").default("MEETUP_EVENT_STREAM_DE")
    val outputFile: String by option(help = "Name of file to output latency measures to.").default("results.json")
    val pollDuration: Long by option(help = "Name of file to output latency measures to.").long().default(15)
    val props = Properties()

    override fun run() {
        // Populate consumer properties
        props["bootstrap.servers"] = config["server-address"]
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = ByteArrayDeserializer::class.java
        props["session.timeout.ms"] = "10000"
        props["enable.auto.commit"] = "true"
        props["auto.offset.reset"] = "earliest"
        props["group.id"] = groupID
        val duration = Duration.ofSeconds(pollDuration)

        val consumerFunc = fun(cons: Consumer, topic: String): Map<String, Long>{
            val res = cons.consume(topic, duration)
            val map = HashMap<String, Long>()
            cons.finalize()
            res.iterator().forEach { 
                map.put(it.key()!!, it.timestamp())
            }
            return map            
        }

        println(String.format("Kafka Broker: %s, Group ID: %s, Topic: %s, Poll Duration: %d S", config["server-address"], groupID, topic1, duration.toSeconds()))
        val cons1 = Consumer(props)
        fun one() = GlobalScope.async {
            consumerFunc(cons1, topic1)
        }

        println(String.format("Kafka Broker: %s, Group ID: %s, Topic: %s, Poll Duration: %d S", config["server-address"], groupID, topic1, duration.toSeconds()))
        val cons2 = Consumer(props)
        fun two() = GlobalScope.async {
            consumerFunc(cons2, topic2)
        }
        
        val results = HashMap<String, Long>()
        runBlocking {
            val map1 = one().await()
            val map2 = two().await()
            
            map1.iterator().forEach { 
                val ts1 = it.value
                val ts2 = map2[it.key]
                if (ts2 != null) {
                    results.put(it.key, (ts1 - ts2))
                }
            }
        }

        val mapSerializer: KSerializer<Map<String, Long>> = MapSerializer(String.serializer(), Long.serializer())
        val jsonString = json.stringify(mapSerializer, results)
        
        println(String.format("Output File: %s", outputFile))
        File(outputFile).writeText(jsonString)
    }
}