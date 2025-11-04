use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
};

pub fn init_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error")
}

pub fn init_consumer(brokers: &str, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[topic]).expect("Subscribe failed");
    consumer
}