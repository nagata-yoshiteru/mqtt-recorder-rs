use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MqttMessage {
    pub time: f64,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    pub msg_b64: String,
}
