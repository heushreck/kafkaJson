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

    fun consume(topicName:String){
        consumer.subscribe(listOf(topicName))
        println("Subscribed to topic $topicName")
        val records: ConsumerRecords<String?, X?> = consumer.poll(10000)
        //consumer.seekToBeginning(consumer.assignment())
        //val records: ConsumerRecords<String?, X?> = consumer.poll(Duration.ofMillis(Long.MAX_VALUE))
        for (record in records) {
            System.out.printf(
                "topic = %s, offset = %d, key = %s, value = %s, timestamp = %s\n",
                topicName, record.offset(), record.key(), record.value().toString(), record.timestamp().toString()
            )
        }
    }
    fun finalize() {
        consumer.unsubscribe()
    }

}
