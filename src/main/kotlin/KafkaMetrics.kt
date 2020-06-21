import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Min
import java.util.*


fun main() {
    val kafkaMetrics = KafkaMetrics()
    kafkaMetrics.collectMetrics()
}

class KafkaMetrics {


    fun collectMetrics(){
        val tags = HashMap<String, String>()
        tags.put("client-id", "test")
        tags.put("topic", "event_test")
        val config = MetricConfig().tags(tags)
        val metrics = Metrics(config); // this is the global repository of metrics and sensors

        val sensor = metrics.sensor("message-sizes");

        var metricName = metrics.metricName("message-size-avg", "producer-metrics", "average message size");
        sensor.add(metricName, Avg());

        metricName = metrics.metricName("message-size-max", "producer-metrics");
        sensor.add(metricName, Max());

        metricName = metrics.metricName("message-size-min", "producer-metrics", "message minimum size", "client-id", "my-client", "topic", "event_test");
        sensor.add(metricName, Min());

        // as messages are sent we record the sizes
        sensor.record(100.0);
        println(metrics.metrics())
    }
}