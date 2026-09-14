use std::env;

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
    pub dir: Option<String>,
    pub rdb_filename: Option<String>,
}

pub fn parse_args() -> Config {
    let mut config = Config {
        port: 6379,
        replica_of: None,
        dir: None,
        rdb_filename: None,
    };
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
                let dir = args.next().expect("missing rdb dir");
                config.dir = Some(dir);
            }
            "--dbfilename" => {
                let rdb_filename = args.next().expect("missing rdb filename");
                config.rdb_filename = Some(rdb_filename);
            }
            _ => {
                eprintln!("unknown argument");
                std::process::exit(1);
            }
        }
    }
    config
}
