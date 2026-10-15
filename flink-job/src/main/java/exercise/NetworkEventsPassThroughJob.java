package exercise;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NetworkEventsPassThroughJob {
    public static void main(String[] args) throws Exception {
        String bootstrap = getenv("KAFKA_BOOTSTRAP", "kafka:9093");
        String sourceTopic = getenv("NETWORK_EVENTS_TOPIC", "network-events");
        String sinkTopic = getenv("NETWORK_INCIDENTS_TOPIC", "network-incidents");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(sourceTopic)
                .setGroupId("network-events-pass-through")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "network-events")
                .sinkTo(sink)
                .name("network-incidents");

        env.execute("network-events-pass-through");
    }

    private static String getenv(String name, String fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }
}
