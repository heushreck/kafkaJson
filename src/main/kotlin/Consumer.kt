import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class Consumer(properties: Properties) {
    val consumer: KafkaConsumer<String, ByteArray> = KafkaConsumer<String, ByteArray>(properties)
    
    fun consume(topicName:String, pollDuration: Duration): ConsumerRecords<String?, ByteArray?>{
        consumer.subscribe(listOf(topicName))
        val records = consumer.poll(pollDuration)
        return records
    }

    fun finalize() {
        consumer.unsubscribe()
    }

}
