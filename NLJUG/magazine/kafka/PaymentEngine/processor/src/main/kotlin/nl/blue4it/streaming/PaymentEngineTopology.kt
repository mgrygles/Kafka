package nl.blue4it.streaming

import example.avro.CustomerId
import example.avro.Message
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import nl.blue4it.streaming.processor.TransactionCountProcessor
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.Stores
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.EnableKafkaStreams
import java.util.*

@SpringBootApplication
@EnableKafkaStreams
open class PaymentEngineTopology {

    @Bean
    open fun buildTopology(streamsBuilder: StreamsBuilder): Topology {
        println("building topology")

        return streamsBuilder.build()
            .addSource("Source","input-topic")
            .addProcessor<CustomerId, Message, CustomerId, Int>("Process", { TransactionCountProcessor() }, "Source")
            .addStateStore(
                Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("TransactionOverviewStore"),
                    getCustomerIdSerde(),
                    Serdes.Integer()
                ),"Process"
            )
            .addSink("Sink", "output-topic", getCustomerIdSerde().serializer(), IntegerSerializer(), "Process")
    }

    private fun getCustomerIdSerde(): SpecificAvroSerde<CustomerId> {
        val customerIdSerde: SpecificAvroSerde<CustomerId> = SpecificAvroSerde<CustomerId>()
        customerIdSerde.configure(getSchemaRegistryUrl(),true)
        return customerIdSerde
    }

    private fun getSchemaRegistryUrl(): MutableMap<String, String> = Collections.singletonMap(
        "schema.registry.url",
        "http://localhost:8085"
    )


    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(PaymentEngineTopology::class.java, *args)
        }
    }
}