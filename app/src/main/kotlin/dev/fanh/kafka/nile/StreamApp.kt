package dev.fanh.kafka.nile

class StreamApp

fun main(args: Array<String>) {
    val servers = args[0]
    val groupId = args[1]
    val inTopic = args[2]
    val goodTopic = args[3]

    val consumer = Consumer(servers, groupId, inTopic)
    val producer = PassThroughProducer(servers, goodTopic)
    consumer.run(producer)
}