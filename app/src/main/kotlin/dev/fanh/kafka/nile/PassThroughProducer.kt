package dev.fanh.kafka.nile

import org.apache.kafka.clients.producer.KafkaProducer

class PassThroughProducer(servers: String, private val topic: String) : Producer {

    private val producer: KafkaProducer<String, String>

    init {
        producer = KafkaProducer(createConfig(servers))
    }

    override fun process(message: String) {
        write(producer, topic, message)
    }
}