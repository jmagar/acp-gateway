use std::path::PathBuf;

pub struct ServerConfig {
    pub port: u16,
    pub data_dir: PathBuf,
}

impl ServerConfig {
    pub fn from_env() -> Self {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3001);
        let data_dir = std::env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data"));

        Self { port, data_dir }
    }
}
