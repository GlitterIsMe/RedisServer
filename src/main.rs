use std::env;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::net::TcpListener;
use std::thread;
use std::io::prelude::*;

extern crate threadpool;
mod redis_server;
mod simple_mem_db;
mod executor;
mod tikv;

fn main(){
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
    let pd_endpoint = vec!["127.0.0.1:2379".to_string()];
    let listener = TcpListener::bind(&addr).unwrap();
    //let db = simple_mem_db::SimpleMemDB::new();
    let db = tikv::tikv_db::TikvDB::connect(pd_endpoint).unwrap();
    let mut server = redis_server::Server::new(db, "8080".to_string());
    for connection in listener.incoming(){
        match connection{
            Ok(stream) =>{
                server.new_connection(stream);
            }
            Err(_) =>{
                println!("connect failed");
            }
        }
    }
    println!("Listening on: {}", addr);
}
