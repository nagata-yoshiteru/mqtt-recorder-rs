use std::{
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};
use log::*;
use serde::Serialize;
use crate::MqttMessage;

#[derive(Debug, Serialize)]
struct CsvRecord {
    time: f64,
    qos: u8,
    retain: bool,
    topic: String,
    msg: String,
}

/// JSON形式のログファイルをCSV形式に変換
pub fn convert_json_to_csv(input_dir: &PathBuf, output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting conversion from {:?} to {:?}", input_dir, output_dir);
    
    // 入力ディレクトリを再帰的に探索してJSONファイルを処理
    convert_directory(input_dir, output_dir, input_dir)?;
    
    info!("Conversion completed successfully");
    Ok(())
}

/// ディレクトリを再帰的に処理
fn convert_directory(
    current_dir: &Path,
    output_base: &Path,
    input_base: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    
    for entry in fs::read_dir(current_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            // サブディレクトリを再帰的に処理
            convert_directory(&path, output_base, input_base)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("json") {
            // JSONファイルを変換
            convert_json_file(&path, output_base, input_base)?;
        }
    }
    
    Ok(())
}

/// 単一のJSONファイルをCSVに変換
fn convert_json_file(
    json_file: &Path,
    output_base: &Path,
    input_base: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    
    // 相対パスを計算
    let relative_path = json_file.strip_prefix(input_base)?;
    
    // 出力ファイルパスを生成（.jsonを.csvに変更）
    let output_path = output_base.join(relative_path).with_extension("csv");
    
    // 出力ディレクトリを作成
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    
    info!("Converting {:?} to {:?}", json_file, output_path);
    
    // JSONファイルを読み込んでCSVに変換
    let input_file = fs::File::open(json_file)?;
    let reader = BufReader::new(input_file);
    
    let output_file = fs::File::create(&output_path)?;
    let mut csv_writer = csv::WriterBuilder::new()
        .quote(b'\'')  // シングルクォートを使用
        .from_writer(output_file);
    
    let mut record_count = 0;
    let mut error_count = 0;
    
    for line in reader.lines() {
        match line {
            Ok(line_content) => {
                if line_content.trim().is_empty() {
                    continue;
                }
                
                match process_json_line(&line_content) {
                    Ok(csv_record) => {
                        if let Err(e) = csv_writer.serialize(&csv_record) {
                            error!("Failed to write CSV record: {:?}", e);
                            error_count += 1;
                        } else {
                            record_count += 1;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to process line in {:?}: {:?}", json_file, e);
                        error_count += 1;
                    }
                }
            }
            Err(e) => {
                error!("Failed to read line from {:?}: {:?}", json_file, e);
                error_count += 1;
            }
        }
    }
    
    csv_writer.flush()?;
    
    if error_count > 0 {
        warn!("Converted {} records with {} errors from {:?}", record_count, error_count, json_file);
    } else {
        info!("Successfully converted {} records from {:?}", record_count, json_file);
    }
    
    Ok(())
}

/// JSONの行をCSVレコードに変換
fn process_json_line(line: &str) -> Result<CsvRecord, Box<dyn std::error::Error>> {
    // JSONをMqttMessageとしてパース
    let mqtt_msg: MqttMessage = serde_json::from_str(line)?;
    
    // Base64デコード
    let decoded_msg = match base64::decode(&mqtt_msg.msg_b64) {
        Ok(decoded_bytes) => {
            // バイト列を文字列に変換（UTF-8として）
            match String::from_utf8(decoded_bytes) {
                Ok(utf8_string) => utf8_string,
                Err(_) => {
                    // UTF-8として無効な場合は、バイト列を16進数表現で表示
                    let bytes = base64::decode(&mqtt_msg.msg_b64)?;
                    format!("0x{}", hex::encode(bytes))
                }
            }
        }
        Err(e) => {
            warn!("Failed to decode base64: {:?}", e);
            format!("DECODE_ERROR: {}", mqtt_msg.msg_b64)
        }
    };
    
    Ok(CsvRecord {
        time: mqtt_msg.time,
        qos: mqtt_msg.qos,
        retain: mqtt_msg.retain,
        topic: mqtt_msg.topic,
        msg: decoded_msg,
    })
}
