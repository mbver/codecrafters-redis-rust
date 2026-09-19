use std::{env, path::PathBuf};

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
    pub dir: PathBuf,
    pub rdb_filename: Option<String>,
    pub append_only: String,
    pub append_dirname: String,
    pub append_filename: String,
    pub append_fsync: String,
}

impl Default for Config {
    fn default() -> Self {
        let pwd = std::env::current_dir().expect("failed to retrieve current dir");
        Config {
            port: 6379,
            replica_of: None,
            dir: pwd,
            rdb_filename: None,
            append_only: "no".to_string(),
            append_dirname: "appendonlydir".to_string(),
            append_filename: "appendonly.aof".to_string(),
            append_fsync: "everysec".to_string(),
        }
    }
}

pub fn parse_args() -> Config {
    let mut config = Config::default();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => {
                config.port = args
                    .next()
                    .expect("missing port")
                    .parse()
                    .expect("invalid port");
            }
            "--replicaof" => {
                let host_port = args.next().expect("missing host-port");
                let mut parts = host_port.split_whitespace();
                let host = parts.next().expect("missing host").to_string();
                let port = parts
                    .next()
                    .expect("missing port")
                    .parse::<u16>()
                    .expect("invalid port");
                config.replica_of = Some((host, port));
            }
            "--dir" => {
                let dir = args.next().expect("missing rdb dir value");
                config.dir = PathBuf::from(dir);
            }
            "--dbfilename" => {
                config.rdb_filename = Some(args.next().expect("missing rdb filename value"));
            }
            "--appendonly" => {
                config.append_only = args.next().expect("missing appendonly value");
            }
            "--appenddirname" => {
                config.append_dirname = args.next().expect("missing appenddirname value");
            }
            "--appendfilename" => {
                config.append_filename = args.next().expect("missing appendfilename value");
            }
            "--appendfsync" => {
                config.append_fsync = args.next().expect("missing appendfsync value");
            }
            _ => {
                eprintln!("unknown argument");
                std::process::exit(1);
            }
        }
    }
    config
}
