package nl.blue4it.streaming.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
open class TopicConfiguration {

    @Bean
    open fun input(): NewTopic {
        return TopicBuilder.name("input-topic")
            .partitions(1)
            .replicas(1)
            .compact()
            .build()
    }

    @Bean
    open fun output(): NewTopic {
        return TopicBuilder.name("count")
            .partitions(1)
            .replicas(1)
            .compact()
            .build()
    }
}