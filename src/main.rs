use log::*;
use rumqttc::{Event, EventLoop, Incoming, MqttOptions, Publish, QoS, Request, Subscribe};

use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::fs;
use std::io::Write;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::time::SystemTime;
use structopt::StructOpt;
// use tokio::fs;
// use tokio::io::AsyncBufReadExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-recorder", about = "mqtt recorder written in rust")]

struct Opt {
    // The verbosity of the program
    #[structopt(short, long, default_value = "1")]
    verbose: u32,

    // The address to connect to
    #[structopt(short, long, default_value = "localhost")]
    address: String,

    // The port to connect to
    #[structopt(short, long, default_value = "1883")]
    port: u16,

    // Mode to run software in
    #[structopt(subcommand)]
    mode: Mode,

    // The file to either read from, or write to
    #[structopt(short, long, parse(from_os_str))]
    filename: PathBuf,
}

#[derive(Debug, StructOpt)]
pub enum Mode {
    // Records values from an MQTT Stream
    #[structopt(name = "record")]
    Record(RecordOptions),

    // Replay values from an input file
    #[structopt(name = "replay")]
    Replay(ReplayOtions),
}

#[derive(Debug, StructOpt)]
pub struct RecordOptions {
    #[structopt(short, long, default_value = "#")]
    // Topic to record, can be used multiple times for a set of topics
    topic: Vec<String>,
}

#[derive(Debug, StructOpt)]
pub struct ReplayOtions {
    #[structopt(short, long, default_value = "1.0")]
    // Topic to record
    speed: f64,
}

#[derive(Serialize, Deserialize)]
struct MqttMessage {
    time: f64,
    qos: u8,
    retain: bool,
    topic: String,
    msg_b64: String,
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let servername = format!("{}-{}", "mqtt-recorder-rs", now);
    let filename = opt.filename;

    match opt.verbose {
        1 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Info).init();
        }
        2 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Debug).init();
        }
        3 => {
            let _e = SimpleLogger::new().with_level(LevelFilter::Trace).init();
        }
        0 | _ => {}
    }

    let mut mqttoptions = MqttOptions::new(servername, &opt.address, opt.port);

    mqttoptions.set_keep_alive(5);
    let mut eventloop = EventLoop::new(mqttoptions, 20 as usize);
    let requests_tx = eventloop.requests_tx.clone();

    debug!("{:?}", filename);

    // Enter recording mode and open file readonly
    match opt.mode {
        Mode::Replay(replay) => {
            let mut file = fs::OpenOptions::new();
            let file = file.read(true).create_new(false).open(filename).unwrap();

            // Sends the recorded messages
            tokio::spawn(async move {
                let mut previous = -1.0;
                // text
                for line in io::BufReader::new(file).lines() {
                    if let Ok(line) = line {
                        let msg = serde_json::from_str::<MqttMessage>(&line);
                        if let Ok(msg) = msg {
                            if previous < 0.0 {
                                previous = msg.time;
                            }

                            tokio::time::sleep(std::time::Duration::from_millis(
                                ((msg.time - previous) * 1000.0 / replay.speed) as u64,
                            ))
                            .await;

                            previous = msg.time;

                            let qos = match msg.qos {
                                0 => QoS::AtMostOnce,
                                1 => QoS::AtLeastOnce,
                                2 => QoS::ExactlyOnce,
                                _ => QoS::AtMostOnce,
                            };
                            let publish =
                                Publish::new(msg.topic, qos, base64::decode(msg.msg_b64).unwrap());
                            let _e = requests_tx.send(publish.into()).await;
                        }
                    }
                }
            });

            // run the eventloop forever
            loop {
                let _res = eventloop.poll().await;
            }
        }
        // Enter recording mode and open file writeable
        Mode::Record(record) => {
            let mut file = fs::OpenOptions::new();
            let mut file = file.write(true).create_new(true).open(filename).unwrap();

            loop {
                let res = eventloop.poll().await;

                match res {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        let qos = match publish.qos {
                            QoS::AtMostOnce => 0,
                            QoS::AtLeastOnce => 1,
                            QoS::ExactlyOnce => 2,
                        };

                        let msg = MqttMessage {
                            time: SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64(),
                            retain: publish.retain,
                            topic: publish.topic.clone(),
                            msg_b64: base64::encode(&*publish.payload),
                            qos,
                        };

                        let serialized = serde_json::to_string(&msg).unwrap();
                        writeln!(file, "{}", serialized).unwrap();

                        debug!("{:?}", publish);
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(_connect))) => {
                        info!("Connected to: {}:{}", opt.address, opt.port);

                        for topic in &record.topic {
                            let subscription = Subscribe::new(topic, QoS::AtLeastOnce);
                            let _ = requests_tx.send(Request::Subscribe(subscription)).await;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
