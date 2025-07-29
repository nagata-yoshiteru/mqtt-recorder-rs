use std::{fs, path::PathBuf};
use chrono::NaiveDateTime;

/// ヘルパー関数：ディレクトリ内の指定された時間範囲のファイルを取得
pub fn get_files_in_range(
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
                    // 複数のパターンに対応
                    // 1. 標準記録: mqtt-recorder-yyyy-mm-dd-hhmm.json
                    // 2. インテリジェント記録（番号なし）: mqtt-recorder-{topic}-yyyymmdd-hhmmss.json
                    // 3. インテリジェント記録（番号付き）: mqtt-recorder-{topic}-yyyymmdd-hhmmss-{number}.json
                    
                    // 標準記録のパターン（yyyy-mm-dd-hhmm）
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
                    
                    // インテリジェント記録のパターンを処理
                    // 最後の部分がタイムスタンプかタイムスタンプ-番号かを判定
                    let parts: Vec<&str> = time_part.split('-').collect();
                    if parts.len() >= 3 {
                        // 最後の要素が数字かチェック（ファイル番号の可能性）
                        let last_part = parts[parts.len() - 1];
                        
                        let timestamp_part = if last_part.chars().all(|c| c.is_ascii_digit()) && parts.len() >= 4 {
                            // ファイル番号付きのパターン: mqtt-recorder-{topic}-yyyymmdd-hhmmss-{number}
                            // 最後から2つ目の部分までを結合してタイムスタンプとする
                            parts[parts.len() - 4..parts.len() - 1].join("-")
                        } else {
                            // 番号なしのパターン: mqtt-recorder-{topic}-yyyymmdd-hhmmss
                            // 最後から2つの部分を結合してタイムスタンプとする
                            if parts.len() >= 3 {
                                parts[parts.len() - 2..].join("-")
                            } else {
                                return false;
                            }
                        };
                        
                        // yyyymmdd-hhmmss形式のタイムスタンプをパース
                        if let Ok(file_dt) = NaiveDateTime::parse_from_str(&timestamp_part, "%Y%m%d-%H%M%S") {
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
            }
            false
        });
    }
    
    // ファイル名でソート
    files.sort();
    Ok(files)
}
