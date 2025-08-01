use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    time::Instant,
};
use serde_json::Value;
use chrono::Local;
use log::*;
use base64;

/// JSONの値の種類を表す
#[derive(Debug, Clone)]
pub enum JsonValueType {
    Number(Vec<f64>),
    String(Vec<String>),
    Boolean(Vec<bool>),
    Other,
}

impl JsonValueType {
    /// 新しい値を追加
    pub fn add_value(&mut self, value: &Value) {
        match (self, value) {
            (JsonValueType::Number(ref mut vec), Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    vec.push(f);
                }
            }
            (JsonValueType::String(ref mut vec), Value::String(s)) => {
                vec.push(s.clone());
            }
            (JsonValueType::Boolean(ref mut vec), Value::Bool(b)) => {
                vec.push(*b);
            }
            _ => {} // 型が一致しない場合は何もしない
        }
    }

    /// 統計を計算（分散 for 数値、ユニーク数 for その他）
    pub fn calculate_stat(&self) -> f64 {
        match self {
            JsonValueType::Number(values) => {
                if values.is_empty() {
                    return 0.0;
                }
                
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter()
                    .map(|x| (x - mean).powi(2))
                    .sum::<f64>() / values.len() as f64;
                variance
            }
            JsonValueType::String(values) => {
                let unique_count = values.iter().collect::<std::collections::HashSet<_>>().len();
                unique_count as f64
            }
            JsonValueType::Boolean(values) => {
                let unique_count = values.iter().collect::<std::collections::HashSet<_>>().len();
                unique_count as f64
            }
            JsonValueType::Other => 0.0,
        }
    }
}

/// 各トピックの統計情報を管理
pub struct TopicStats {
    data: HashMap<String, JsonValueType>, // キーパス -> 値のリスト
    last_stats_time: Instant,
    stats_start_time: Instant, // 統計開始時刻
    stats_file: Option<File>,
    stats_interval_secs: u64, // 統計計算間隔（秒）
}

impl TopicStats {
    pub fn new(stats_file_path: PathBuf, stats_interval_secs: u64) -> Result<Self, std::io::Error> {
        let stats_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(stats_file_path)?;
            
        let now = Instant::now();
        Ok(TopicStats {
            data: HashMap::new(),
            last_stats_time: now,
            stats_start_time: now,
            stats_file: Some(stats_file),
            stats_interval_secs,
        })
    }

    /// JSONメッセージを解析して統計データに追加
    pub fn add_message(&mut self, json_str: &str) {
        if let Ok(mqtt_message) = serde_json::from_str::<serde_json::Value>(json_str) {
            // MQTTメッセージの構造から msg_b64 フィールドを取得
            if let Some(msg_b64) = mqtt_message.get("msg_b64").and_then(|v| v.as_str()) {
                // Base64デコードしてからJSONとしてパース
                if let Ok(decoded_bytes) = base64::decode(msg_b64) {
                    if let Ok(decoded_str) = String::from_utf8(decoded_bytes) {
                        if let Ok(payload_json) = serde_json::from_str::<serde_json::Value>(&decoded_str) {
                            self.extract_values("", &payload_json);
                        } else {
                            // JSON以外のペイロードの場合は統計対象外
                            debug!("Payload is not JSON: {}", decoded_str);
                        }
                    }
                }
            }
        } else {
            debug!("Failed to parse MQTT message JSON: {}", json_str);
        }
    }

    /// JSON値を再帰的に解析してキーパスと値を抽出
    fn extract_values(&mut self, prefix: &str, value: &Value) {
        match value {
            Value::Object(map) => {
                for (key, val) in map {
                    let new_prefix = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    self.extract_values(&new_prefix, val);
                }
            }
            Value::Array(arr) => {
                for (index, val) in arr.iter().enumerate() {
                    let new_prefix = format!("{}[{}]", prefix, index);
                    self.extract_values(&new_prefix, val);
                }
            }
            _ => {
                // リーフノードの値を記録
                if !prefix.is_empty() {
                    self.add_value_to_stats(prefix, value);
                }
            }
        }
    }

    /// 統計データに値を追加
    fn add_value_to_stats(&mut self, key_path: &str, value: &Value) {
        let entry = self.data.entry(key_path.to_string()).or_insert_with(|| {
            match value {
                Value::Number(_) => JsonValueType::Number(Vec::new()),
                Value::String(_) => JsonValueType::String(Vec::new()),
                Value::Bool(_) => JsonValueType::Boolean(Vec::new()),
                _ => JsonValueType::Other,
            }
        });
        
        entry.add_value(value);
    }

    /// 統計計算間隔が経過したかチェック
    pub fn should_calculate_stats(&self) -> bool {
        self.last_stats_time.elapsed().as_secs() >= self.stats_interval_secs
    }

    /// 統計を計算してファイルに出力
    pub fn calculate_and_write_stats(&mut self) -> Result<(), std::io::Error> {
        if self.data.is_empty() {
            return Ok(());
        }

        let now = Local::now();
        let start_time_chrono = now - chrono::Duration::seconds(self.last_stats_time.elapsed().as_secs() as i64);
        let start_timestamp = start_time_chrono.format("%Y-%m-%d %H:%M:%S").to_string();
        let end_timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let time_range = format!("{} - {}", start_timestamp, end_timestamp);
        
        let mut stats_parts = vec![time_range];
        
        // キーパスでソートして一貫した出力順序を保つ
        let mut sorted_keys: Vec<_> = self.data.keys().collect();
        sorted_keys.sort();
        
        for key in sorted_keys {
            if let Some(value_type) = self.data.get(key) {
                let stat = value_type.calculate_stat();
                stats_parts.push(format!("{}:{:.3}", key, stat));
            }
        }
        
        let stats_line = stats_parts.join(", ") + "\n";
        
        if let Some(ref mut file) = self.stats_file {
            file.write_all(stats_line.as_bytes())?;
            file.flush()?;
            info!("Wrote stats: {}", stats_line.trim());
        }
        
        // データをクリアして次の統計期間に備える
        self.data.clear();
        self.last_stats_time = Instant::now();
        self.stats_start_time = Instant::now();
        
        Ok(())
    }

    /// 強制的に統計を計算（ファイル分割時など）
    pub fn force_calculate_stats(&mut self) -> Result<(), std::io::Error> {
        self.calculate_and_write_stats()
    }
}

/// 全トピックの統計を管理
pub struct StatsManager {
    topic_stats: HashMap<String, TopicStats>,
    base_dir: PathBuf,
    stats_enabled: bool,
    stats_interval_secs: u64,
}

impl StatsManager {
    pub fn new(base_dir: PathBuf, stats_enabled: bool, stats_interval_secs: u64) -> Self {
        StatsManager {
            topic_stats: HashMap::new(),
            base_dir,
            stats_enabled,
            stats_interval_secs,
        }
    }

    /// メッセージを統計に追加
    pub fn add_message(&mut self, topic: &str, json_payload: &str) {
        if !self.stats_enabled {
            return; // 統計が無効な場合は何もしない
        }

        // トピックの統計が存在しない場合は作成
        if !self.topic_stats.contains_key(topic) {
            let stats_file_path = self.get_stats_file_path(topic);
            match TopicStats::new(stats_file_path, self.stats_interval_secs) {
                Ok(stats) => {
                    self.topic_stats.insert(topic.to_string(), stats);
                }
                Err(e) => {
                    error!("Failed to create stats file for topic '{}': {:?}", topic, e);
                    return;
                }
            }
        }

        // メッセージを統計に追加
        if let Some(stats) = self.topic_stats.get_mut(topic) {
            stats.add_message(json_payload);
        }
    }

    /// 定期的な統計計算チェック
    pub fn check_and_calculate_stats(&mut self) {
        if !self.stats_enabled {
            return; // 統計が無効な場合は何もしない
        }

        let topics_to_update: Vec<String> = self.topic_stats
            .iter()
            .filter(|(_, stats)| stats.should_calculate_stats())
            .map(|(topic, _)| topic.clone())
            .collect();

        for topic in topics_to_update {
            if let Some(stats) = self.topic_stats.get_mut(&topic) {
                if let Err(e) = stats.calculate_and_write_stats() {
                    error!("Failed to calculate stats for topic '{}': {:?}", topic, e);
                }
            }
        }
    }

    /// 特定のトピックの統計を強制計算（ファイル分割時）
    pub fn force_calculate_stats_for_topic(&mut self, topic: &str) {
        if !self.stats_enabled {
            return; // 統計が無効な場合は何もしない
        }

        if let Some(stats) = self.topic_stats.get_mut(topic) {
            if let Err(e) = stats.force_calculate_stats() {
                error!("Failed to force calculate stats for topic '{}': {:?}", topic, e);
            }
        }
    }

    /// 統計ファイルのパスを生成
    fn get_stats_file_path(&self, topic: &str) -> PathBuf {
        let topic_filename = topic.replace('/', "-").replace('+', "plus").replace('#', "hash");
        let stats_filename = format!("mqtt-recorder-{}-stats.txt", topic_filename);
        
        // トピックのディレクトリ構造内に統計ファイルを配置
        let topic_dir = self.base_dir.join(topic);
        // ディレクトリが存在しない場合は作成
        let _ = std::fs::create_dir_all(&topic_dir);
        
        topic_dir.join(stats_filename)
    }
}
