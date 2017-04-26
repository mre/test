// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]

// Import the macro. Don't forget to add `error-chain` in your
// `Cargo.toml`!
#[macro_use]
extern crate error_chain;

extern crate memchr;

#[macro_use]
extern crate log;
extern crate clap;
extern crate futures;
extern crate futures_cpupool;
extern crate rdkafka;
extern crate tokio_core;
extern crate influxdb;

use clap::{App, Arg};
use futures::Future;
use futures::stream::Stream;
use futures_cpupool::CpuPool;
use tokio_core::reactor::Core;
use influxdb::AsyncDb;

use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::topic_partition_list::TopicPartitionList;

mod example_utils;
use example_utils::setup_logger;

mod errors;
use errors::*;

mod decoder;

// fn convert(msg: Message) -> String {
//     match msg.payload_view::<str>() {
//         Some(Ok(payload)) => format!("Payload len for {} is {}", payload, payload.len()),
//         Some(Err(_)) => format!("Error processing message payload"),
//         None => format!("No payload"),
//     }
// }

const INFLUXDB_BATCH_SIZE: usize = 100;


struct Producer {
    db: AsyncDb,
}






fn handle(brokers: &str, group_id: &str, topic: &str) -> Result<()> {
    // Create the event loop. The event loop will run on a single thread and drive the pipeline.
    let mut core = Core::new()?;

    // Create the CPU pool, for CPU-intensive message processing.
    let cpu_pool = CpuPool::new_num_cpus();

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let mut consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("broker.version.fallback", "0.8.2")
        .set("enable.partition.eof", "false")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set_default_topic_config(TopicConfig::new()
            .set("auto.offset.reset", "smallest")
            .finalize())
        .create::<StreamConsumer<_>>()?;

    let mut topics = TopicPartitionList::new();
    let partitions = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    // let partitions = vec![0];
    topics.add_topic_with_partitions(topic, &partitions);
    consumer.get_base_consumer().assign(&topics)?;

    let mut batch: Vec<&str> = Vec::with_capacity(INFLUXDB_BATCH_SIZE);

    // consumer.subscribe(&vec![input_topic]).expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    // let producer = ClientConfig::new()
    //     .set("bootstrap.servers", brokers)
    //     .set_default_topic_config(TopicConfig::new()
    //         .set("produce.offset.report", "true")
    //         .finalize())
    //     .create::<FutureProducer<_>>()
    //     .expect("Producer creation error");

    // Create a handle to the core, that will be used to provide additional asynchronous work
    // to the event loop.
    let handle = core.handle();

    let async_db = AsyncDb::new(core.handle(),
                                "http://influxdb:8086", // URL to InfluxDB
                                "metrics" /* Name of the database in InfluxDB */)
        .expect("Unable to create AsyncDb");


    // Create the outer pipeline on the message stream.
    // let mut counter = 0;
    let processed_stream = consumer.start()
        .filter_map(|result| {
            // Filter out errors
            match result {
                Ok(msg) => Some(msg),
                Err(kafka_error) => {
                    warn!("Error while receiving from Kafka: {:?}", kafka_error);
                    None
                }
            }
        })
        .for_each(|msg| {
            // Process each message
            // counter += 1;
            // // println!("{:?}: {:?}", counter, msg.payload());
            // if counter % 1000 == 0 {

            //     println!("{:?}", counter);
            // }
            // let producer = producer.clone();
            // let topic_name = output_topic.to_owned();
            // Create the inner pipeline, that represents the processing of a single event.
            let process_message = cpu_pool.spawn_fn(move || {
                    // Take ownership of the message, and runs an expensive computation on it,
                    // using one of the threads of the `cpu_pool`.
                    Ok(decoder::decode(msg.payload().unwrap()))
                    // println!("{:?}", msg);
                    // Ok(())
                })
                .and_then(|decoded| {
                    // Send the result of the computation to Kafka, asynchronously.
                    // info!("Decoded! {:?}", decoded);
                    async_db.add_data("cpu_load_short,host=server01,region=us-west value=0.64 \
                                       1434055562000000000");
                    Ok(())
                });
            // .and_then(|mut batch| {

            //     // Once the message has been produced, print the delivery report and terminate
            //     // the pipeline.
            //     // info!("Delivery report for result: {:?}", d_report);
            //     Ok(())
            // });
            // .or_else(|err| {
            //     // In case of error, this closure will be executed instead.
            //     warn!("Error while processing message: {:?}", err);
            //     Ok(())
            // });
            // Spawns the inner pipeline in the same event pool.
            handle.spawn(process_message);
            Ok(())
        });

    info!("Starting event loop");
    // Runs the event pool until the consumer terminates.
    core.run(processed_stream).unwrap();
    info!("Stream processing terminated");
    Ok(())
}

fn run() -> Result<()> {
    let matches = App::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
        .arg(Arg::with_name("brokers")
            .short("b")
            .long("brokers")
            .help("Broker list in kafka format")
            .takes_value(true)
            .default_value("localhost:9092"))
        .arg(Arg::with_name("group-id")
            .short("g")
            .long("group-id")
            .help("Consumer group id")
            .takes_value(true)
            .default_value("example_consumer_group_id"))
        .arg(Arg::with_name("log-conf")
            .long("log-conf")
            .help("Configure the logging format (example: 'rdkafka=trace')")
            .takes_value(true))
        .arg(Arg::with_name("topic")
            .long("topic")
            .help("Input topic")
            .takes_value(true)
            .required(true))
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let topic = matches.value_of("topic").unwrap();

    handle(brokers, group_id, topic)
}

fn main() {
    if let Err(ref e) = run() {
        use ::std::io::Write;
        let stderr = &mut ::std::io::stderr();
        let errmsg = "Error writing to stderr";

        writeln!(stderr, "error: {}", e).expect(errmsg);

        for e in e.iter().skip(1) {
            writeln!(stderr, "caused by: {}", e).expect(errmsg);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            writeln!(stderr, "backtrace: {:?}", backtrace).expect(errmsg);
        }

        ::std::process::exit(1);
    }
}
