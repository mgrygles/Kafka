package nl.blue4it.basic;


import static org.assertj.core.api.Assertions.assertThat;

import example.avro.Balance;
import example.avro.Payment;

import nl.blue4it.basic.processor.PaymentProcessor;
import nl.blue4it.basic.service.PaymentService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = PaymentEngineApplication.class)
class EmbeddedKafkaIntegrationTest {

    @Autowired
    private PaymentService service;

    @Autowired
    private PaymentProcessor paymentProcessor;

    @Test
    public void testConsumer() throws Exception {
        service.processPayment(createPayment(), "payments");
        service.processPayment(createBalance(), "balance");

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    assertThat(paymentProcessor.getPayment()).isNotNull();
                    assertTrue(paymentProcessor.getPayment().getProcessed());
                    System.out.println("--------> Payment send to topic, proceed");
                   });
    }

    private Payment createPayment() {
        return Payment.newBuilder()
                .setName("Mister Blue")
                .setAmount(100.00F)
                .setBalance(500.00F)
                .setIban("NL63ABNA332454654")
                .setToIban("NL61RABO0332543675")
                .setProcessed(true)
                .build();
    }

    private Balance createBalance() {
        return Balance.newBuilder()
                .setAmount(500.00F)
                .setCurrency("EUR")
                .build();
    }

}