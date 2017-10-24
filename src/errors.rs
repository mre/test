use rdkafka::error::KafkaError;

error_chain!{
    foreign_links {
        Io(::std::io::Error);
        Kafka(KafkaError);
    }
}