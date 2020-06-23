import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.Max
import org.apache.kafka.common.metrics.stats.Min
import java.util.*


class KafkaMetrics (tags: Map<String,String>){

    val tags: Map<String, String> = tags

    fun collectMetrics(): Map<MetricName, KafkaMetric>{
        val config = MetricConfig().tags(tags)
        val metrics = Metrics(config); // this is the global repository of metrics and sensors

        val sensor = metrics.sensor("latency");

        var metricName = metrics.metricName("request-latency-avg", "producer-metrics", "average latency");
        sensor.add(metricName, Avg());

        metricName = metrics.metricName("request-size-avg", "producer-metrics", "average request size");
        sensor.add(metricName, Avg());

        // as messages are sent we record the sizes
        sensor.record(100.0);
        return metrics.metrics()
    }
}