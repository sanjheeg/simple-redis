#![allow(unused_imports)]
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Write, Read, Error};
use std::net::{TcpStream, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;

struct RESPDataType;
impl RESPDataType {
    const ARRAY: u8 = b'*';
    const BULK: u8 = b'$';
}


// evaluate what the arguments passed in to the server are
// call appropriate functions based on RESP request type
fn evaluate_resp(mut cmd: &[u8], store: &Arc<Mutex<HashMap<String, String>>>) -> String {
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
                    let mut map = store.lock().unwrap();
                    map.insert(key, value);
                    "+OK\r\n".to_string()
                }
                //"GET" | "get" => {

                //}
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
        let mut curr_word_len: usize = (cmd[1] - b'0') as usize;
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
fn handle_stream(stream: TcpStream, store: Arc<Mutex<HashMap<String, String>>>) {
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
    let store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

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
