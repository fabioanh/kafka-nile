package dev.fanh.kafka.nile

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

class Consumer(servers: String, groupId: String, private val topic: String) {

    private val consumer: KafkaConsumer<String, String>

    init {
        consumer = KafkaConsumer(createConfig(servers, groupId))
    }

    private fun createConfig(servers: String, groupId: String): Properties {
        val properties = Properties()
        properties["bootstrap.servers"] = servers
        properties["group.id"] = groupId
        properties["enable.auto.commit"] = "true"
        properties["auto.commit.interval.ms"] = "1000"
        properties["auto.offset.reset"] = "earliest"
        properties["session.timeout.ms"] = "30000"
        properties["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        properties["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        return properties
    }

    public fun run(producer: Producer) {
        this.consumer.subscribe(listOf(this.topic))
        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))
            records.iterator().forEach {
                producer.process(it.value())
            }
        }
    }

}