package nl.blue4it.streaming.processor;

import example.avro.CustomerId;
import example.avro.Message;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import nl.blue4it.streaming.PaymentEngineTopology;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TransactionProcessorTest {

    private static final String SCHEMA_REGISTRY_SCOPE = TransactionProcessorTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    private TopologyTestDriver testDriver;

    private TestInputTopic<CustomerId, Message> inputTopic;

    private TestOutputTopic<CustomerId, Integer> outputTopic;

    @BeforeEach
    void beforeEach() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        new PaymentEngineTopology().buildTopology(builder);
        Topology topology = builder.build();

        // Dummy properties needed for test diver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create test driver
        testDriver = new TopologyTestDriver(topology, props);

        Serde<CustomerId> customerIdSerde = new SpecificAvroSerde<>();
        Serde<Message> messageSerde = new SpecificAvroSerde<>();

        // Configure Serdes to use the same mock schema registry URL
        Map<String, String> config = Map.of(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        customerIdSerde.configure(config, true);
        messageSerde.configure(config, false);

        // Define input and output topics to use in tests
        inputTopic = testDriver.createInputTopic(
                "input-topic",
                customerIdSerde.serializer(),
                messageSerde.serializer());

        outputTopic = testDriver.createOutputTopic(
                "output-topic",
                customerIdSerde.deserializer(),
                new IntegerDeserializer());
    }

    @AfterEach
    void afterEach() {
        testDriver.close();
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

    @Test
    public void testOneTransactionCount() throws InterruptedException {
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        CustomerId customer = new CustomerId("Mister X", "NL71BANK0332454676");
        Message message = new Message("", "NL71BANK0332454676", "NL71BANK0332454678", 1232322112L, true);
        inputTopic.pipeInput(customer, message);

        Thread.sleep(2000);
        //Read and validate output to match word as key and count as value
        assertThat(outputTopic.readValue(), equalTo(1));
        assertThat(outputTopic.isEmpty(), is(true));
    }
}