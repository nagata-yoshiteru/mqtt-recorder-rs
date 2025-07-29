# mqtt-recorder-rs


[![Latest version](https://img.shields.io/crates/v/mqtt-recorder-rs.svg)](https://crates.io/crates/mqtt-recorder-rs)
![License](https://img.shields.io/crates/l/mqtt-recorder-rs.svg)

Quickly written mqtt logger and replay tool written in rust.

Records MQTT messages to timestamped JSON files with automatic time-based splitting (by minute). Files are organized in date-based directory structure for easy management. The replay function supports time range filtering and can replay the contents over an MQTT broker with configurable speed.


## Installation

Assuming Cargo installed

    cargo install mqtt-recorder-rs

## Building
### for Development
```.sh
cargo build
ls target/debug
```

### for Release
```.sh
cargo rustc --release -- -C link-args=-Wl,-x,-S
ls target/release
```

## Recording 

Records MQTT messages to a directory with automatic time-based file splitting. Files are organized by date and split by minute intervals.

### Basic recording (all topics):
    mqtt-recorder-rs -a localhost record -d ./data

### Recording specific topics:
    mqtt-recorder-rs -a localhost record -d ./data -t "topic1/+/temp" -t "topic2/+/battery"

### File structure created:
    data/
    ├── 2025-07-25/
    │   ├── mqtt-recorder-2025-07-25-1000.json
    │   ├── mqtt-recorder-2025-07-25-1001.json
    │   └── mqtt-recorder-2025-07-25-1002.json
    └── 2025-07-26/
        ├── mqtt-recorder-2025-07-26-0900.json
        └── mqtt-recorder-2025-07-26-0901.json

## Intelligent Recording

Records MQTT messages with topic-based directory organization and intelligent file splitting based on message intervals. Files are split when no messages are received for a specified duration or when reaching the maximum message count per file (100,000 messages).

### Basic intelligent recording:
    mqtt-recorder-rs -a localhost irecord -d ./data

### Intelligent recording with custom timeout (60 seconds):
    mqtt-recorder-rs -a localhost irecord -d ./data --sec 60

### Recording specific topics with intelligent mode:
    mqtt-recorder-rs -a localhost irecord -d ./data -t "sensor/+" --sec 30

### Intelligent file structure created:
    data/
    ├── chincha/
    │   └── shimo/
    │       └── 2025-07-25/
    │           ├── mqtt-recorder-chincha-shimo-20250725-100230.json      # Base file
    │           ├── mqtt-recorder-chincha-shimo-20250725-100230-1.json    # After 100k messages
    │           └── mqtt-recorder-chincha-shimo-20250725-103045.json      # After timeout
    └── sensor/
        ├── temperature/
        │   └── 2025-07-25/
        │       └── mqtt-recorder-sensor-temperature-20250725-100515.json
        └── humidity/
            └── 2025-07-25/
                ├── mqtt-recorder-sensor-humidity-20250725-100630.json
                └── mqtt-recorder-sensor-humidity-20250725-100630-1.json

## Replaying

Replays recorded MQTT messages from a directory. Supports time range filtering and playback speed control.

### Replay all files in directory:
    mqtt-recorder-rs -a localhost replay -d ./data

### Replay with time range filtering:
    mqtt-recorder-rs -a localhost replay -d ./data --start-time "2025-07-25 10:00" --end-time "2025-07-25 12:00"

### Replay at 2x speed with looping:
    mqtt-recorder-rs -a localhost replay -d ./data --speed 2.0 --loop true

### Replay at half speed:
    mqtt-recorder-rs -a localhost replay -d ./data --speed 0.5

## Features

### Standard Recording Mode (`record`)
- **Automatic time-based file splitting**: Records are automatically split into separate files every minute
- **Date-based directory organization**: Files are organized in `YYYY-MM-DD/` directories

### Intelligent Recording Mode (`irecord`)
- **Topic-based directory organization**: Each topic gets its own directory hierarchy
- **Dual file splitting criteria**: Files are split based on message intervals (configurable timeout) OR message count (100,000 messages per file)
- **Automatic file numbering**: When message limit is reached, files are numbered sequentially (-1, -2, -3, etc.)
- **Per-topic timeout management**: Each topic manages its own file timeout independently
- **Automatic cleanup**: Inactive files are automatically closed when timeout is reached

### Replay Features
- **Time range filtering**: Replay specific time ranges using `--start-time` and `--end-time` options
- **Playback speed control**: Adjust replay speed with `--speed` parameter (e.g., 2.0 for 2x speed, 0.5 for half speed)
- **Loop playback**: Continuously replay data with `--loop true`
- **Recursive file discovery**: Automatically finds and processes all JSON files in the specified directory

### General Features
- **TLS/SSL support**: Connect to secure MQTT brokers using certificate files
- **Multiple topic subscription**: Subscribe to multiple topic patterns simultaneously
- **Flexible topic patterns**: Support for MQTT wildcards (`+` and `#`)

## Time Format

When using `--start-time` and `--end-time` options, use the format: `YYYY-MM-DD HH:MM`

Examples:
- `--start-time "2025-07-25 09:30"`
- `--end-time "2025-07-25 18:45"`
