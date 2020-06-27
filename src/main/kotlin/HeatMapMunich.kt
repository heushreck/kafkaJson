import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.*

@Serializable
class LatLon(val LAT: Double?, val LON: Double?)

fun main() {
    val properties = Properties()
    properties["bootstrap.servers"] = "localhost:29092"
    properties["group.id"] = "heatMapMunich"
    properties["enable.auto.commit"] = "true"
    properties["auto.commit.interval.ms"] = "1000"
    properties["session.timeout.ms"] = "10000"
    properties["auto.offset.reset"] = "earliest"

    properties["key.deserializer"] = StringDeserializer::class.java
    properties["value.deserializer"] = ByteArrayDeserializer::class.java

    val heatMapMunich = HeatMapMunich()
    val replace = heatMapMunich.consume("MEETUP_EVENTS_STREAM_DE_MUNICH_LL", properties)

    val path: Path = Paths.get("index.html")
    val charset: Charset = StandardCharsets.UTF_8

    var content = String(Files.readAllBytes(path), charset)
    content = content.replace("insert here".toRegex(), replace)
    Files.write(path, content.toByteArray(charset))
}

class HeatMapMunich {
    val json = Json(JsonConfiguration(ignoreUnknownKeys = true))
    fun consume(topicName:String, properties: Properties): String {
        val consumer: KafkaConsumer<String, ByteArray> = KafkaConsumer<String, ByteArray>(properties)
        consumer.subscribe(listOf(topicName))
        var s = ""
        println("Subscribed to topic $topicName")
        var running = true
        var count = 0
        while (running) {
            val records: ConsumerRecords<String?, ByteArray?> = consumer.poll(Duration.ofMillis(10000))
            for (record in records) {
                count++;
                System.out.printf(
                    "offset = %d, key = %s, value = %s\n",
                    record.offset(), record.key(), record.value()?.toString(Charsets.UTF_8)
                )
                val lat  = json.parse(LatLon.serializer(), record.value()!!.toString(Charsets.UTF_8)).LAT
                val lon  = json.parse(LatLon.serializer(), record.value()!!.toString(Charsets.UTF_8)).LON
                if(lat != null && lon != null){
                    if(s.length > 5) s += ", "
                    s += "new google.maps.LatLng($lat, $lon)"
                }
                if (count > 200){
                    running = false
                }
            }
        }
        return s;
    }
}