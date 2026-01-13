#![allow(unused_imports)]
use std::io::{Write, Read};
use std::net::{TcpStream, TcpListener};
use std::thread;
use std::borrow::Cow;
use std::io::Error;



struct RESPDataType;
impl RESPDataType {
    const ARRAY: u8 = b'*';
    const BULK: u8 = b'$';
}


fn evaluate_resp(mut cmd: &[u8]) -> String {
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
                "PING" | "ping" => "+PONG\r\n".to_string(),
                "ECHO" | "echo" => format!("${}\r\n{}\r\n", args[1].len(), args[1]),
                _ => "-not_supported command\r\n".to_string(),
            }
        }
        _ => "-not_supported data type\r\n".to_string(),
 
    }
    
}

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
fn handle_stream(stream: TcpStream) {
    let mut stream: TcpStream = stream;
    let mut cmd: [u8; 1024] = [0u8; 1024];
    while let Ok(n) = stream.read(&mut cmd) {
        let val: String = evaluate_resp(&cmd);
        let _ = stream.write(val.as_bytes());
    }
}


// opens a TCP server on the Redis defualt port.
// Accepts incoming client connections and spawns a new thread per connection 
fn main() {    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || 
            match stream {
                Ok(_stream) => {
                    println!("accepted a new connection");
                    handle_stream(_stream);
                }
                Err(e) => {
                    println!("error: {}", e);
                }            
            }
        );
             
         
     }
}
