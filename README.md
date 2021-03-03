# mqtt-recorder-rs


[![Latest version](https://img.shields.io/crates/v/mqtt-recorder-rs.svg)](https://crates.io/crates/mqtt-recorder-rs)
![License](https://img.shields.io/crates/l/mqtt-recorder-rs.svg)

Quickly written mqtt logger and replay tool written in rust.

Stores each subscribed value in a json object consisting of a line. Each line is an MQTT publish. The replay function can use this to replay the contents 
over an MQTT broker


## Installation

Assuming Cargo installed

    cargo install mqtt-recorder-rs

## Recording 

Example recording two specific topics to : 
   
    mqtt-recorder-rs -a localhost record -t "topic1/+/temp" -t "topic2/+/battery" -f loggfile.json
  
## Replaying

Example replaying the values at normal speed, restaring and looping the replayed values constantly

    mqtt-recorder-rs -a localhost replay -f loggifle.json --loop true --speed 1.0
