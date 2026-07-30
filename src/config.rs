use std::env;

pub struct Config {
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
}

pub fn parse_args() -> Config {
    let mut config = Config {
        port: 6379,
        replica_of: None,
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
            _ => {
                eprintln!("unknown argument");
                std::process::exit(1);
            }
        }
    }
    config
}
