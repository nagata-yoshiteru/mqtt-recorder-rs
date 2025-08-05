use log::*;
use rumqttc::{TlsConfiguration, Transport, Client};
use rumqttc::{Event, Incoming, MqttOptions, QoS};
use simple_logger::SimpleLogger;
use std::{
    fs,
    io::{self, BufRead, Read, Write},
    time::{SystemTime, Duration},
};
use structopt::StructOpt;
use chrono::{Local, Timelike};

// 内部モジュールをインポート
use mqtt_recorder_rs::*;

fn main() {
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

    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = Client::new(mqttoptions, 20);

    match opt.mode {
        Mode::Replay(replay) => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let (stop_tx, stop_rx) = std::sync::mpsc::channel();

            // Sends the recorded messages
            rt.spawn(async move {
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
                                    let payload = base64::decode(msg.msg_b64).unwrap();
                                    let _e = client.publish(msg.topic, qos, msg.retain, payload);
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
                match eventloop.recv() {
                    Ok(Ok(_)) => {},
                    _ => break,
                }
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
                let res = eventloop.recv();

                match res {
                    Ok(Ok(Event::Incoming(Incoming::Publish(publish)))) => {
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
                    Ok(Ok(Event::Incoming(Incoming::ConnAck(_connect)))) => {
                        info!("Connected to: {}:{}", opt.address, opt.port);

                        for topic in &record.topic {
                            let _ = client.subscribe(topic, QoS::AtLeastOnce);
                        }
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        break;
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
                irecord.stats_interval,
                !irecord.disable_all_topic_record // 全トピック記録の有効/無効を設定
            );
            let cleanup_interval = std::time::Duration::from_secs(irecord.sec / 2);
            let mut last_cleanup = std::time::Instant::now();
            
            loop {
                let res = eventloop.recv();

                match res {
                    Ok(Ok(Event::Incoming(Incoming::Publish(publish)))) => {
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
                    Ok(Ok(Event::Incoming(Incoming::ConnAck(_connect)))) => {
                        info!("Connected to: {}:{}", opt.address, opt.port);

                        for topic in &irecord.topic {
                            let _ = client.subscribe(topic, QoS::AtLeastOnce);
                        }
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        break;
                    }
                    _ => {}
                }
                
                // 定期的なファイルクリーンアップを同期的に実行
                if last_cleanup.elapsed() >= cleanup_interval {
                    file_manager.cleanup_timeout_files();
                    last_cleanup = std::time::Instant::now();
                }
            }
        }
    }
}
