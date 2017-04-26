use rdkafka::error::KafkaError;


// Create the Error, ErrorKind, ResultExt, and Result types
error_chain!{
    foreign_links {
        Io(::std::io::Error);
        Kafka(KafkaError);
    }
}