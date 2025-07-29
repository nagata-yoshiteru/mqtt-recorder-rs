use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-recorder", about = "mqtt recorder written in rust")]
pub struct Opt {
    /// The verbosity of the program
    #[structopt(short, long, default_value = "1")]
    pub verbose: u32,

    /// The address to connect to
    #[structopt(short, long, default_value = "localhost")]
    pub address: String,

    /// The port to connect to
    #[structopt(short, long, default_value = "1883")]
    pub port: u16,

    /// certificate of trusted CA
    #[structopt(short, long)]
    pub cafile: Option<PathBuf>,

    /// Mode to run software in
    #[structopt(subcommand)]
    pub mode: Mode,
}

#[derive(Debug, StructOpt)]
pub enum Mode {
    // Records values from an MQTT Stream
    #[structopt(name = "record")]
    Record(RecordOptions),

    // Records values with intelligent topic-based directory organization
    #[structopt(name = "irecord")]
    IntelligentRecord(IntelligentRecordOptions),

    // Replay values from an input file
    #[structopt(name = "replay")]
    Replay(ReplayOptions),
}

#[derive(Debug, StructOpt)]
pub struct RecordOptions {
    #[structopt(short, long, default_value = "#")]
    /// Topic to record, can be used multiple times for a set of topics
    pub topic: Vec<String>,
    /// The directory to write mqtt message files to
    #[structopt(short, long, parse(from_os_str))]
    pub directory: PathBuf,
}

#[derive(Debug, StructOpt)]
pub struct IntelligentRecordOptions {
    #[structopt(short, long, default_value = "#")]
    /// Topic to record, can be used multiple times for a set of topics
    pub topic: Vec<String>,
    /// The directory to write mqtt message files to
    #[structopt(short, long, parse(from_os_str))]
    pub directory: PathBuf,
    /// Seconds to wait for messages before closing file (default: 30 seconds)
    #[structopt(long, default_value = "30")]
    pub sec: u64,
}

#[derive(Debug, StructOpt)]
pub struct ReplayOptions {
    #[structopt(short, long, default_value = "1.0")]
    /// Speed of the playback, 2.0 makes it twice as fast
    pub speed: f64,

    /// The directory to read replay values from
    #[structopt(short, long, parse(from_os_str))]
    pub directory: PathBuf,

    /// Start date and time (YYYY-MM-DD HH:MM)
    #[structopt(long)]
    pub start_time: Option<String>,

    /// End date and time (YYYY-MM-DD HH:MM)
    #[structopt(long)]
    pub end_time: Option<String>,

    #[structopt(
        name = "loop",
        short,
        long,
        parse(try_from_str),
        default_value = "false"
    )]
    pub loop_replay: bool,
}
