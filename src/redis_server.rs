use std::net::TcpStream;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock, Mutex};
use std::io::prelude::*;
use std::thread;
use threadpool::ThreadPool;
use std::time::Duration;

use crate::executor::Executor;

struct Client<E: DB>{
    db: Arc<RwLock<E>>,
    id: u64,
    tx: Sender<Executor<E>>,
}

impl<E: DB> Client<E>{
    fn new(db: Arc<RwLock<E>>, id: u64, tx: Sender<Executor<E>>) -> Self{
        println!("NewClient: {}", id);
        Client{
            db,
            id,
            tx,
        }
    }

    fn spawn(&self, mut connection: TcpStream){
        // 从stream中读取数据
        // 通过sender发送给server
        let tx = self.tx.clone();
        let db = self.db.clone();
        thread::spawn(move ||{
            loop{
                let mut inited = false;
                loop{
                    let mut raw_command = vec![0; 40960];
                    let res= connection.read(&mut raw_command);
                    if inited{
                        match res{
                            Ok(l) =>{
                                // received a command
                                if l != 0{
                                    //println!("read message from conenction {:?}", l);
                                    //stream.write("+OK\r\n".as_bytes());
                                    let (sender, receiver) = channel();
                                    let db = db.clone();
                                    tx.send(Executor::new(db, raw_command,sender)).unwrap();
                                    match receiver.recv_timeout(Duration::from_secs(10)){
                                        Ok(res) =>{
                                            connection.write(res.as_bytes()).unwrap();
                                        }
                                        Err(_) =>{
                                            connection.write("-ReceiveTimeout\r\n".as_bytes()).unwrap();
                                        }
                                    }
                                }
                            }
                            _ =>{
                                println!("error");
                            }
                        }
                    }else{
                        println!("new client");
                        connection.write("+OK\r\n".as_bytes()).unwrap();
                        inited = true;
                    }
                }
            }
        });
    }
}

struct TikvClient{

}
#[derive(Debug)]
pub enum DBError{
    NotFound,
    AlreadyExited(String),
    Other,
}

pub trait DB: Send + Sync + 'static{
    fn raw_put(&mut self, key: String, value: String) -> Result<String, DBError>;

    fn raw_get(&self, key: String) -> Result<String, DBError>;

    fn txn_put(&self){

    }

    fn txn_get(&self){

    }
}

#[derive(Clone)]
pub enum Operation{
    Set(String, String, bool),
    Get(String),
    GetSet(String, String),
    StrLen(String),
    Append(String, String),
    SetRange(String, usize, String),
    GetRange(String, i32, i32),
    Mset(Vec<(String, String)>),
    Mget(Vec<String>),
    Other,
    NotParsed,
}

const MAX_BGWORK_NUM: usize = 32;

pub struct Server<E: DB>{
    db: Arc<RwLock<E>>,
    client_seq: u64,
    port: String,
    connections: Vec<Client<E>>,
    executor_tx: Sender<Executor<E>>,
    exec_pool: ThreadPool,
}

impl<E: DB> Server<E>{
    pub fn new(db: E, port: String) -> Self{
        let (executor_tx, executor_rx) = channel();
        let server = Server{
            db: Arc::new(RwLock::new(db)),
            client_seq: 0,
            port,
            connections: vec![],
            executor_tx,
            exec_pool: ThreadPool::new(MAX_BGWORK_NUM),
        };
        let thread_pool = server.exec_pool.clone();
        thread::spawn(move | |{
            loop{
                let res = executor_rx.recv();
                match res{
                    Ok(mut executor) => {
                        thread_pool.execute(move ||{
                            executor.parse();
                            executor.exec_command();
                        });
                    },
                    Err(_) => (),
                }
            }
        });
        server
    }

    pub fn new_connection(&mut self, stream: TcpStream){
        let client_id = self.client_seq + 1;
        let client = Client::new(self.db.clone(), client_id,  self.executor_tx.clone());
        client.spawn(stream);
        self.connections.push(client);
    }
}