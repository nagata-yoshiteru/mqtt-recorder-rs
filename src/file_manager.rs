use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    time::Instant,
};
use chrono::Local;
use log::*;
use crate::stats::StatsManager;

/// ヘルパー関数：現在時刻に基づいてファイルパスを生成
pub fn get_current_file_path(base_dir: &PathBuf) -> PathBuf {
    let now = Local::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    let time_str = now.format("%Y-%m-%d-%H%M").to_string();
    
    let dir = base_dir.join(&date_str);
    dir.join(format!("mqtt-recorder-{}.json", time_str))
}

/// ヘルパー関数：トピック名をファイルシステム用のパスに変換
pub fn topic_to_path(topic: &str) -> String {
    topic.replace('/', "-").replace('+', "plus").replace('#', "hash")
}

/// ヘルパー関数：全トピック用のファイルパスを生成（ファイル番号付き）
pub fn get_all_topics_file_path(base_dir: &PathBuf, base_timestamp: &str, file_number: u32) -> PathBuf {
    let now = Local::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    
    // 全トピック用のディレクトリを作成
    let all_topics_dir = base_dir.join("#").join(&date_str);
    
    if file_number == 0 {
        all_topics_dir.join(format!("mqtt-recorder-#-{}.json", base_timestamp))
    } else {
        all_topics_dir.join(format!("mqtt-recorder-#-{}-{}.json", base_timestamp, file_number))
    }
}

/// ヘルパー関数：ベースタイムスタンプを使用してファイルパスを生成（ファイル番号付き）
pub fn get_intelligent_file_path(base_dir: &PathBuf, topic: &str, base_timestamp: &str, file_number: u32) -> PathBuf {
    let now = Local::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    
    // トピック名でディレクトリを作成
    let topic_dir = base_dir.join(topic).join(&date_str);
    
    // ファイル名を生成（トピック名も含める）
    let topic_filename = topic_to_path(topic);
    
    if file_number == 0 {
        topic_dir.join(format!("mqtt-recorder-{}-{}.json", topic_filename, base_timestamp))
    } else {
        topic_dir.join(format!("mqtt-recorder-{}-{}-{}.json", topic_filename, base_timestamp, file_number))
    }
}

/// インテリジェント記録用のファイル管理構造体
pub struct TopicFileManager {
    files: HashMap<String, (fs::File, PathBuf, Instant, u32, u32)>, // (ファイル, パス, 最終アクセス, メッセージ数, ファイル番号)
    base_timestamps: HashMap<String, String>, // トピックごとのベースタイムスタンプ
    all_topics_file: Option<(fs::File, PathBuf, Instant, u32, u32)>, // 全トピック用ファイル (ファイル, パス, 最終アクセス, メッセージ数, ファイル番号)
    all_topics_base_timestamp: Option<String>, // 全トピック用ベースタイムスタンプ
    base_dir: PathBuf,
    timeout_secs: u64,
    max_messages_per_file: u32,
    stats_manager: StatsManager,
    all_topics_enabled: bool, // 全トピック記録が有効かどうか
}

impl TopicFileManager {
    pub fn new(base_dir: PathBuf, timeout_secs: u64, stats_enabled: bool, stats_interval_secs: u64, all_topics_enabled: bool) -> Self {
        let stats_manager = StatsManager::new(base_dir.clone(), stats_enabled, stats_interval_secs);
        Self {
            files: HashMap::new(),
            base_timestamps: HashMap::new(),
            all_topics_file: None,
            all_topics_base_timestamp: None,
            base_dir,
            timeout_secs,
            max_messages_per_file: 100_000, // 10万メッセージまで
            stats_manager,
            all_topics_enabled,
        }
    }
    
    pub fn get_or_create_file(&mut self, topic: &str) -> Result<&mut fs::File, std::io::Error> {
        let now = Instant::now();
        let mut create_new_file = false;
        let mut file_number = 0;
        let mut use_existing_timestamp = false;
        
        // 既存のファイルをチェック（タイムアウトまたはメッセージ数制限）
        let should_remove = if let Some((_, _, last_access, message_count, current_file_number)) = self.files.get(topic) {
            let timed_out = now.duration_since(*last_access).as_secs() > self.timeout_secs;
            let message_limit_reached = *message_count >= self.max_messages_per_file;
            
            if timed_out {
                info!("File for topic '{}' timed out, creating new file", topic);
                // タイムアウトの場合は統計を強制計算してからベースタイムスタンプもクリア
                self.stats_manager.force_calculate_stats_for_topic(topic);
                self.base_timestamps.remove(topic);
                create_new_file = true;
                true
            } else if message_limit_reached {
                file_number = current_file_number + 1;
                use_existing_timestamp = true; // 既存のタイムスタンプを使用
                info!("File for topic '{}' reached message limit ({}), creating new file with number {}", 
                      topic, self.max_messages_per_file, file_number);
                // メッセージ数制限に達した場合も統計を強制計算
                self.stats_manager.force_calculate_stats_for_topic(topic);
                create_new_file = true;
                true
            } else {
                false
            }
        } else {
            create_new_file = true;
            false
        };
        
        if should_remove {
            self.files.remove(topic);
        }
        
        // ファイルが存在しない場合は新規作成
        if create_new_file || !self.files.contains_key(topic) {
            let file_path = if use_existing_timestamp {
                // 既存のベースタイムスタンプを使用
                let base_timestamp = self.base_timestamps.get(topic).unwrap();
                get_intelligent_file_path(&self.base_dir, topic, base_timestamp, file_number)
            } else {
                // 新しいタイムスタンプを生成してベースタイムスタンプとして保存
                let timestamp = Local::now().format("%Y%m%d-%H%M%S").to_string();
                self.base_timestamps.insert(topic.to_string(), timestamp.clone());
                get_intelligent_file_path(&self.base_dir, topic, &timestamp, file_number)
            };
            
            // ディレクトリを作成
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file_path)?;
                
            info!("Created new file for topic '{}': {:?}", topic, file_path);
            self.files.insert(topic.to_string(), (file, file_path, now, 0, file_number));
        } else {
            // アクセス時刻を更新し、メッセージ数をインクリメント
            if let Some((_, _, last_access, message_count, _)) = self.files.get_mut(topic) {
                *last_access = now;
                *message_count += 1;
            }
        }
        
        Ok(&mut self.files.get_mut(topic).unwrap().0)
    }

    /// 全トピック用のファイルを取得または作成
    pub fn get_or_create_all_topics_file(&mut self) -> Result<Option<&mut fs::File>, std::io::Error> {
        if !self.all_topics_enabled {
            return Ok(None);
        }

        let now = Instant::now();
        let mut create_new_file = false;
        let mut file_number = 0;
        let mut use_existing_timestamp = false;
        
        // 既存のファイルをチェック（タイムアウトまたはメッセージ数制限）
        let should_remove = if let Some((_, _, last_access, message_count, current_file_number)) = &self.all_topics_file {
            let timed_out = now.duration_since(*last_access).as_secs() > self.timeout_secs;
            let message_limit_reached = *message_count >= self.max_messages_per_file;
            
            if timed_out {
                info!("All-topics file timed out, creating new file");
                // タイムアウトの場合はベースタイムスタンプもクリア
                self.all_topics_base_timestamp = None;
                create_new_file = true;
                true
            } else if message_limit_reached {
                file_number = current_file_number + 1;
                use_existing_timestamp = true; // 既存のタイムスタンプを使用
                info!("All-topics file reached message limit ({}), creating new file with number {}", 
                      self.max_messages_per_file, file_number);
                create_new_file = true;
                true
            } else {
                false
            }
        } else {
            create_new_file = true;
            false
        };
        
        if should_remove {
            self.all_topics_file = None;
        }
        
        // ファイルが存在しない場合は新規作成
        if create_new_file || self.all_topics_file.is_none() {
            let file_path = if use_existing_timestamp {
                // 既存のベースタイムスタンプを使用
                let base_timestamp = self.all_topics_base_timestamp.as_ref().unwrap();
                get_all_topics_file_path(&self.base_dir, base_timestamp, file_number)
            } else {
                // 新しいタイムスタンプを生成してベースタイムスタンプとして保存
                let timestamp = Local::now().format("%Y%m%d-%H%M%S").to_string();
                self.all_topics_base_timestamp = Some(timestamp.clone());
                get_all_topics_file_path(&self.base_dir, &timestamp, file_number)
            };
            
            // ディレクトリを作成
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            
            let file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file_path)?;
                
            info!("Created new all-topics file: {:?}", file_path);
            self.all_topics_file = Some((file, file_path, now, 0, file_number));
        } else {
            // アクセス時刻を更新し、メッセージ数をインクリメント
            if let Some((_, _, last_access, message_count, _)) = &mut self.all_topics_file {
                *last_access = now;
                *message_count += 1;
            }
        }
        
        Ok(Some(&mut self.all_topics_file.as_mut().unwrap().0))
    }
    
    pub fn cleanup_timeout_files(&mut self) {
        let now = Instant::now();
        let timeout_secs = self.timeout_secs;
        
        // タイムアウトしたトピックを収集
        let mut topics_to_remove = Vec::new();
        
        self.files.retain(|topic, (_, _, last_access, _, _)| {
            let should_keep = now.duration_since(*last_access).as_secs() <= timeout_secs;
            if !should_keep {
                info!("Closing file for topic '{}' due to timeout", topic);
                topics_to_remove.push(topic.clone());
            }
            should_keep
        });
        
        // タイムアウトしたトピックの統計を強制計算してからベースタイムスタンプもクリア
        for topic in &topics_to_remove {
            self.stats_manager.force_calculate_stats_for_topic(topic);
            self.base_timestamps.remove(topic);
        }
        
        // 全トピックファイルのタイムアウトチェック
        if let Some((_, _, last_access, _, _)) = &self.all_topics_file {
            if now.duration_since(*last_access).as_secs() > timeout_secs {
                info!("Closing all-topics file due to timeout");
                self.all_topics_file = None;
                self.all_topics_base_timestamp = None;
            }
        }
    }

    /// メッセージを書き込み、統計分析も実行
    pub fn write_message(&mut self, topic: &str, json_message: &str) -> Result<(), std::io::Error> {
        use std::io::Write;
        
        // トピック別ファイルに書き込み
        {
            let file = self.get_or_create_file(topic)?;
            writeln!(file, "{}", json_message)?;
            file.flush()?;
        }
        
        // 全トピックファイルに書き込み（有効な場合のみ）
        if let Some(all_topics_file) = self.get_or_create_all_topics_file()? {
            writeln!(all_topics_file, "{}", json_message)?;
            all_topics_file.flush()?;
        }
        
        // メッセージ数をインクリメント
        if let Some((_, _, ref mut last_access, ref mut message_count, _)) = self.files.get_mut(topic) {
            *last_access = std::time::Instant::now();
            *message_count += 1;
        }
        
        // 全トピックファイルのメッセージ数もインクリメント
        if let Some((_, _, ref mut last_access, ref mut message_count, _)) = &mut self.all_topics_file {
            *last_access = std::time::Instant::now();
            *message_count += 1;
        }
        
        // 統計分析にメッセージを追加
        self.stats_manager.add_message(topic, json_message);
        
        // 定期的な統計計算をチェック
        self.stats_manager.check_and_calculate_stats();
        
        Ok(())
    }

    /// ファイル分割時に統計を強制計算
    pub fn force_stats_calculation(&mut self, topic: &str) {
        self.stats_manager.force_calculate_stats_for_topic(topic);
    }
}
