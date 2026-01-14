#![allow(unused_imports)]
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Write, Read, Error};
use std::net::{TcpStream, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

struct RESPDataType;
impl RESPDataType {
    const ARRAY: u8 = b'*';
    const BULK: u8 = b'$';
}

type Store = Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>;

// evaluate what the arguments passed in to the server are
// call appropriate functions based on RESP request type
fn evaluate_resp(mut cmd: &[u8], store: &Store) -> String {
    let mut contentLen: u8 = 0;
    
    // array processing
    if cmd[0] == RESPDataType::ARRAY {
        contentLen = cmd[1] - b'0';
        cmd = &cmd[4..];
    }

    match cmd[0] {
        RESPDataType::BULK => {
            let args: Vec<String> = evaluate_bulk_string(cmd, contentLen);

            match args[0].as_str() {
                "PING" | "ping" => {
                    "+PONG\r\n".to_string()
                }
                "ECHO" | "echo" => {
                    format!("${}\r\n{}\r\n", args[1].len(), args[1])
                } 
                "SET" | "set" => {
                    if args.len() < 3 {
                        return "-ERR wrong number of arguments for 'SET'\r\n".to_string();
                    }

                    let key = args[1].clone();
                    let value = args[2].clone();

                    if args.len() == 3 {
                        // no expiration
                        let mut map = store.lock().unwrap();
                        map.insert(key, (value, None));
                        "+OK\r\n".to_string()
                    }
                    else if args.len() == 5 {
                        // expiration
                        if args[3].to_ascii_uppercase() == "PX" {
                            let ms: u64 = args[4].parse().unwrap();
                            let expire_time = Instant::now() + Duration::from_millis(ms);
                            let mut map = store.lock().unwrap();
                            map.insert(key, (value, Some(expire_time)));
                            "+OK\r\n".to_string()
                        } 
                        else if args[3].to_ascii_uppercase() == "EX" {
                            let secs: u64 = args[4].parse().unwrap();
                            let expire_time =  Instant::now() + Duration::from_secs(secs);
                            return "+OK\r\n".to_string();
                        }
                        else {
                            "-ERR unsupported SET option\r\n".to_string()
                        }
                    }
                    else {
                        "-ERR wrong number of arguments for 'SET'\r\n".to_string()
                    }
                }
                "GET" | "get" => {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'get'\r\n".to_string();
                    }
                    
                    let key = &args[1];
                    let mut map = store.lock().unwrap();
                    match map.get(key) {
                        Some((_v, Some(exp))) if Instant::now() >= *exp => {
                            // expired value, delete and act as if missing
                            map.remove(key);
                            "$-1\r\n".to_string()
                        }
                        // if not expired 
                        Some((v, _)) => format!("${}\r\n{}\r\n", v.len(), v),
                        None => "$-1\r\n".to_string(),
                    }
                }
                _ => "-not_supported command\r\n".to_string(),
            }
        }
        _ => "-not_supported data type\r\n".to_string(),
    }
}

// decode contents of a bulk string
// return contents as an array of strings
fn evaluate_bulk_string(mut cmd: &[u8], mut len: u8) -> Vec<String> {
    let mut args:Vec<String> = Vec::new();

    while len > 0 {
        let curr_word_len: usize = (cmd[1] - b'0') as usize;
        let curr_word: Cow<'_,str> = String::from_utf8_lossy(&cmd[4..curr_word_len+4]);
        args.push(curr_word.into());

        cmd = &cmd[curr_word_len + 4 + 2..];
        len -= 1;
    }

    args
}


// changes stream state to mutable
// reads a 20 byte cmd from stream 
// writes '+PONG\r\n' for every read 
// keeps loop running til error or disconnect
fn handle_stream(stream: TcpStream, store: Store) {
    let mut stream: TcpStream = stream;
    let mut cmd: [u8; 1024] = [0u8; 1024];
    while let Ok(n) = stream.read(&mut cmd) {
        let val: String = evaluate_resp(&cmd, &store);
        let _ = stream.write(val.as_bytes());
    }
}


// opens a TCP server on the Redis defualt port.
// Accepts incoming client connections and spawns a new thread per connection 
fn main() {    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store: Store = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        let store_clone = Arc::clone(&store);
        thread::spawn(move || 
            match stream {
                Ok(_stream) => {
                    println!("accepted a new connection");
                    handle_stream(_stream, store_clone);
                }
                Err(e) => {
                    println!("error: {}", e);
                }            
            }
        );
             
         
     }
}
