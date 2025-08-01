use log::*;
use rumqttc::{ConnectionError, TlsConfiguration, Transport};
use rumqttc::{Event, EventLoop, Incoming, MqttOptions, Publish, QoS, Request, Subscribe};
use simple_logger::SimpleLogger;
use std::{
    fs,
    io::{self, BufRead, Read, Write},
    time::SystemTime,
};
use structopt::StructOpt;
use chrono::{Local, Timelike};

// 内部モジュールをインポート
use mqtt_recorder_rs::*;

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
            let mut file_manager = TopicFileManager::new(
                irecord.directory.clone(), 
                irecord.sec, 
                irecord.enable_stats, 
                irecord.stats_interval
            );
            let cleanup_interval = tokio::time::Duration::from_secs(irecord.sec / 2); // クリーンアップは半分の間隔で実行
            let mut cleanup_timer = tokio::time::interval(cleanup_interval);
            
            loop {
                tokio::select! {
                    // メッセージ処理
                    res = eventloop.poll() => {
                        match res {
                            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                                let topic = &publish.topic;
                                
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
                                
                                // 新しい write_message メソッドを使用（統計分析も含む）
                                if let Err(e) = file_manager.write_message(topic, &serialized) {
                                    error!("Failed to write message for topic '{}': {:?}", topic, e);
                                }

                                debug!("{:?}", publish);
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
