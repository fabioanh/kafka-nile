package dev.fanh.kafka.nile

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

interface Producer {
    fun process(message: String)
    fun write(producer: KafkaProducer<String, String>, topic: String, message: String) {
        val producerRecord = ProducerRecord<String, String>(topic, message)
        producer.send(producerRecord)
    }

    fun createConfig(servers: String): Properties {
        val properties = Properties()
        properties["bootstrap.servers"] = servers
        properties["acks"] = "all"
        properties["retries"] = 0
        properties["batch.size"] = 1000
        properties["linger.ms"] = 1
        properties["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        properties["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        return properties
    }
}