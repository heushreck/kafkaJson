import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class Consumer<X>(properties: Properties) {

    val properties: Properties = properties
    val consumer: KafkaConsumer<String, X> = KafkaConsumer<String, X>(properties)

    fun consume(topicName:String): ConsumerRecords<String?, X?>{
        consumer.subscribe(listOf(topicName))
        //println("Subscribed to topic $topicName")
        val records: ConsumerRecords<String?, X?> = consumer.poll(Duration.ofSeconds(15))
        
        return records        
    }

    fun finalize() {
        consumer.unsubscribe()
    }

}
