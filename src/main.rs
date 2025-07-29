use log::*;
use rumqttc::{ConnectionError, TlsConfiguration, Transport};
use rumqttc::{Event, EventLoop, Incoming, MqttOptions, Publish, QoS, Request, Subscribe};
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::{
    fs,
    io::{self, BufRead, Read, Write},
    path::PathBuf,
    time::{SystemTime, Instant},
    collections::HashMap,
};
use structopt::StructOpt;
use chrono::{NaiveDateTime, Local, Timelike};
// use tokio::fs;
// use tokio::io::AsyncBufReadExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-recorder", about = "mqtt recorder written in rust")]

struct Opt {
    //// The verbosity of the program
    #[structopt(short, long, default_value = "1")]
    verbose: u32,

    /// The address to connect to
    #[structopt(short, long, default_value = "localhost")]
    address: String,

    /// The port to connect to
    #[structopt(short, long, default_value = "1883")]
    port: u16,

    /// certificate of trusted CA
    #[structopt(short, long)]
    cafile: Option<PathBuf>,

    /// Mode to run software in
    #[structopt(subcommand)]
    mode: Mode,
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
    Replay(ReplayOtions),
}

#[derive(Debug, StructOpt)]
pub struct RecordOptions {
    #[structopt(short, long, default_value = "#")]
    // Topic to record, can be used multiple times for a set of topics
    topic: Vec<String>,
    // The directory to write mqtt message files to
    #[structopt(short, long, parse(from_os_str))]
    directory: PathBuf,
}

#[derive(Debug, StructOpt)]
pub struct IntelligentRecordOptions {
    #[structopt(short, long, default_value = "#")]
    // Topic to record, can be used multiple times for a set of topics
    topic: Vec<String>,
    // The directory to write mqtt message files to
    #[structopt(short, long, parse(from_os_str))]
    directory: PathBuf,
    // Seconds to wait for messages before closing file (default: 30 seconds)
    #[structopt(long, default_value = "30")]
    sec: u64,
}

#[derive(Debug, StructOpt)]
pub struct ReplayOtions {
    #[structopt(short, long, default_value = "1.0")]
    // Speed of the playback, 2.0 makes it twice as fast
    speed: f64,

    // The directory to read replay values from
    #[structopt(short, long, parse(from_os_str))]
    directory: PathBuf,

    // Start date and time (YYYY-MM-DD HH:MM)
    #[structopt(long)]
    start_time: Option<String>,

    // End date and time (YYYY-MM-DD HH:MM)
    #[structopt(long)]
    end_time: Option<String>,

    #[structopt(
        name = "loop",
        short,
        long,
        parse(try_from_str),
        default_value = "false"
    )]
    loop_replay: bool,
}

#[derive(Serialize, Deserialize)]
struct MqttMessage {
    time: f64,
    qos: u8,
    retain: bool,
    topic: String,
    msg_b64: String,
}

// ヘルパー関数：現在時刻に基づいてファイルパスを生成
fn get_current_file_path(base_dir: &PathBuf) -> PathBuf {
    let now = Local::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    let time_str = now.format("%Y-%m-%d-%H%M").to_string();
    
    let dir = base_dir.join(&date_str);
    dir.join(format!("mqtt-recorder-{}.json", time_str))
}

// ヘルパー関数：トピック名をファイルシステム用のパスに変換
fn topic_to_path(topic: &str) -> String {
    topic.replace('/', "-").replace('+', "plus").replace('#', "hash")
}

// ヘルパー関数：インテリジェント記録用のファイルパスを生成
fn get_intelligent_file_path(base_dir: &PathBuf, topic: &str) -> PathBuf {
    let now = Local::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    let timestamp = now.format("%Y%m%d-%H%M%S").to_string();
    
    // トピック名でディレクトリを作成
    let topic_dir = base_dir.join(topic).join(&date_str);
    
    // ファイル名を生成（トピック名も含める）
    let topic_filename = topic_to_path(topic);
    topic_dir.join(format!("mqtt-recorder-{}-{}.json", topic_filename, timestamp))
}

// インテリジェント記録用のファイル管理構造体
struct TopicFileManager {
    files: HashMap<String, (fs::File, PathBuf, Instant)>,
    base_dir: PathBuf,
    timeout_secs: u64,
}

impl TopicFileManager {
    fn new(base_dir: PathBuf, timeout_secs: u64) -> Self {
        Self {
            files: HashMap::new(),
            base_dir,
            timeout_secs,
        }
    }
    
    fn get_or_create_file(&mut self, topic: &str) -> Result<&mut fs::File, std::io::Error> {
        let now = Instant::now();
        
        // 既存のファイルをチェック（タイムアウトしていないか）
        if let Some((_, _, last_access)) = self.files.get(topic) {
            if now.duration_since(*last_access).as_secs() > self.timeout_secs {
                // タイムアウトしたファイルを削除
                self.files.remove(topic);
                info!("File for topic '{}' timed out, creating new file", topic);
            }
        }
        
        // ファイルが存在しない場合は新規作成
        if !self.files.contains_key(topic) {
            let file_path = get_intelligent_file_path(&self.base_dir, topic);
            
            // ディレクトリを作成
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file_path)?;
                
            info!("Created new file for topic '{}': {:?}", topic, file_path);
            self.files.insert(topic.to_string(), (file, file_path, now));
        } else {
            // アクセス時刻を更新
            if let Some((_, _, last_access)) = self.files.get_mut(topic) {
                *last_access = now;
            }
        }
        
        Ok(&mut self.files.get_mut(topic).unwrap().0)
    }
    
    fn cleanup_timeout_files(&mut self) {
        let now = Instant::now();
        let timeout_secs = self.timeout_secs;
        
        self.files.retain(|topic, (_, _, last_access)| {
            let should_keep = now.duration_since(*last_access).as_secs() <= timeout_secs;
            if !should_keep {
                info!("Closing file for topic '{}' due to timeout", topic);
            }
            should_keep
        });
    }
}

// ヘルパー関数：ディレクトリ内の指定された時間範囲のファイルを取得
fn get_files_in_range(
    base_dir: &PathBuf, 
    start_time: Option<String>, 
    end_time: Option<String>
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    
    // ディレクトリを再帰的に探索
    fn collect_json_files(dir: &PathBuf, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    collect_json_files(&path, files)?;
                } else if path.extension().and_then(|s| s.to_str()) == Some("json") {
                    files.push(path);
                }
            }
        }
        Ok(())
    }
    
    collect_json_files(base_dir, &mut files)?;
    
    // ファイル名に基づいて時間範囲でフィルタリング
    if start_time.is_some() || end_time.is_some() {
        let start_dt = start_time.as_ref().and_then(|s| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M").ok()
        });
        let end_dt = end_time.as_ref().and_then(|s| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M").ok()
        });
        
        files.retain(|path| {
            if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                if let Some(time_part) = filename.strip_prefix("mqtt-recorder-") {
                    if let Ok(file_dt) = NaiveDateTime::parse_from_str(time_part, "%Y-%m-%d-%H%M") {
                        let mut keep = true;
                        if let Some(start) = start_dt {
                            keep &= file_dt >= start;
                        }
                        if let Some(end) = end_dt {
                            keep &= file_dt <= end;
                        }
                        return keep;
                    }
                }
            }
            false
        });
    }
    
    // ファイル名でソート
    files.sort();
    Ok(files)
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let servername = format!("{}-{}", "mqtt-recorder-rs", now);

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

    if let Some(cafile) = opt.cafile {
        let mut file = fs::OpenOptions::new();
        let mut file = file.read(true).create_new(false).open(&cafile).unwrap();
        let mut vec = Vec::new();
        let _ = file.read_to_end(&mut vec).unwrap();

        let tlsconfig = TlsConfiguration::Simple {
            ca: vec,
            alpn: None,
            client_auth: None,
        };

        let transport = Transport::Tls(tlsconfig);
        mqttoptions.set_transport(transport);
    }

    mqttoptions.set_keep_alive(5);
    let mut eventloop = EventLoop::new(mqttoptions, 20 as usize);
    let requests_tx = eventloop.requests_tx.clone();

    // Enter recording mode and open file readonly
    match opt.mode {
        Mode::Replay(replay) => {
            let (stop_tx, stop_rx) = std::sync::mpsc::channel();

            // Sends the recorded messages
            tokio::spawn(async move {
                loop {
                    let mut previous = -1.0;
                    
                    // ディレクトリから再生対象のファイルリストを取得
                    let files = match get_files_in_range(&replay.directory, replay.start_time.clone(), replay.end_time.clone()) {
                        Ok(files) => files,
                        Err(e) => {
                            error!("Failed to get files in range: {:?}", e);
                            break;
                        }
                    };
                    
                    if files.is_empty() {
                        warn!("No files found in the specified directory or time range");
                        break;
                    }
                    
                    info!("Found {} files to replay", files.len());
                    
                    for file_path in files {
                        debug!("Processing file: {:?}", file_path);
                        let file = match fs::OpenOptions::new()
                            .read(true)
                            .create_new(false)
                            .open(&file_path) {
                            Ok(file) => file,
                            Err(e) => {
                                error!("Failed to open file {:?}: {:?}", file_path, e);
                                continue;
                            }
                        };
                        
                        for line in io::BufReader::new(&file).lines() {
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
                                    let publish = Publish::new(
                                        msg.topic,
                                        qos,
                                        base64::decode(msg.msg_b64).unwrap(),
                                    );
                                    let _e = requests_tx.send(publish.into()).await;
                                }
                            }
                        }
                    }

                    if !replay.loop_replay {
                        let _e = stop_tx.send(());
                        break;
                    }
                }
            });

            // run the eventloop forever
            while let Err(std::sync::mpsc::TryRecvError::Empty) = stop_rx.try_recv() {
                let _res = eventloop.poll().await.unwrap();
            }
        }
        // Enter recording mode and open file writeable
        Mode::Record(record) => {
            // 最初のファイルパスを生成
            let mut current_file_path = get_current_file_path(&record.directory);
            let mut current_minute = Local::now().minute();
            
            // ディレクトリを作成
            if let Some(parent) = current_file_path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&current_file_path)
                .unwrap();
            
            info!("Recording to: {:?}", current_file_path);

            loop {
                let res = eventloop.poll().await;

                match res {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        // 分が変わったかチェック（ファイル分割のため）
                        let now = Local::now();
                        if now.minute() != current_minute {
                            // 新しいファイルに切り替え
                            drop(file); // 古いファイルを閉じる
                            
                            current_file_path = get_current_file_path(&record.directory);
                            current_minute = now.minute();
                            
                            // 新しいディレクトリを作成
                            if let Some(parent) = current_file_path.parent() {
                                fs::create_dir_all(parent).unwrap();
                            }
                            
                            file = fs::OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(&current_file_path)
                                .unwrap();
                            
                            info!("Switched to new file: {:?}", current_file_path);
                        }
                        
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
                    Err(e) => {
                        error!("{:?}", e);
                        if let ConnectionError::Network(_e) = e {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
        // Enter intelligent recording mode
        Mode::IntelligentRecord(irecord) => {
            let mut file_manager = TopicFileManager::new(irecord.directory.clone(), irecord.sec);
            let cleanup_interval = tokio::time::Duration::from_secs(irecord.sec / 2); // クリーンアップは半分の間隔で実行
            let mut cleanup_timer = tokio::time::interval(cleanup_interval);
            
            loop {
                tokio::select! {
                    // メッセージ処理
                    res = eventloop.poll() => {
                        match res {
                            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                                let topic = &publish.topic;
                                
                                // ファイルを取得または作成
                                match file_manager.get_or_create_file(topic) {
                                    Ok(file) => {
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
                                        if let Err(e) = writeln!(file, "{}", serialized) {
                                            error!("Failed to write to file for topic '{}': {:?}", topic, e);
                                        }

                                        debug!("{:?}", publish);
                                    }
                                    Err(e) => {
                                        error!("Failed to get file for topic '{}': {:?}", topic, e);
                                    }
                                }
                            }
                            Ok(Event::Incoming(Incoming::ConnAck(_connect))) => {
                                info!("Connected to: {}:{}", opt.address, opt.port);

                                for topic in &irecord.topic {
                                    let subscription = Subscribe::new(topic, QoS::AtLeastOnce);
                                    let _ = requests_tx.send(Request::Subscribe(subscription)).await;
                                }
                            }
                            Err(e) => {
                                error!("{:?}", e);
                                if let ConnectionError::Network(_e) = e {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    // 定期的なファイルクリーンアップ
                    _ = cleanup_timer.tick() => {
                        file_manager.cleanup_timeout_files();
                    }
                }
            }
        }
    }
}
