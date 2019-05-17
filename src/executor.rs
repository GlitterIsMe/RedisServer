use crate::redis_server::DB;
use crate::redis_server::Operation;
use crate::redis_server::DBError;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::{Arc, RwLock};
pub struct Executor<E: DB>{
    db: Arc<RwLock<E>>,
    raw_command: Vec<u8>,
    op: Operation,
    result_sender: Sender<String>,
}

impl<E: DB> Executor<E>{
    pub fn new(db: Arc<RwLock<E>>, raw_command: Vec<u8>, tx: Sender<String>) -> Self{
        Executor{
            db,
            raw_command,
            result_sender: tx,
            op: Operation::NotParsed,
        }
    }

    fn response(&self, res: String){
        self.result_sender.send(res).unwrap();
    }

    pub fn exec_command(&mut self){
        let op = self.op.clone();
        match op{
            Operation::Other =>{
                self.response("-NOT SURPPORTED\r\n".to_string());
            }

            Operation::NotParsed =>{
                self.response("-UNKNWO COMMAND\r\n".to_string());
            }

            Operation::Set(key, value, nx) =>{
                ///TODO: surport setnx
                let res = self.set(vec![(key.clone(), value.clone())]);
                self.response(res);
            }

            Operation::Mset(kvs) =>{
                let res = self.set(kvs.to_vec());
                self.response(res);
            }

            Operation::Get(key) =>{
                self.response(self.get(vec![key.clone()]));
            }

            Operation::Mget(keys) =>{
                self.response(self.get(keys.to_vec()));
            }

            Operation::GetSet(key, value) =>{
                let res = self.getset(key.clone(), value.clone());
                self.response(res);
            }

            Operation::Append(key, append) =>{
                let res = self.append(key.clone(), append.clone());
                self.response(res);
            }

            Operation::StrLen(key) =>{
                self.response(self.strlen(key.clone()));
            }

            Operation::GetRange(key, start, end) =>{
                let (start, end) = (start, end);
                self.response(self.get_range(key.clone(), start, end));
            }

            Operation::SetRange(key, off, data) => {
                let res = self.set_range(key.clone(), off, data.clone());
                self.response(res);
            }
        }
    }

    pub fn parse(&mut self){
        let mut command_string = String::from_utf8(self.raw_command.clone()).unwrap();
        command_string = command_string.trim().to_string();
        //command_string = command_string.to_uppercase();
        let args: Vec<&str> = command_string.split("\r\n").collect();
        assert!(args.len() > 2);
        //println!("raw command {:?}", args);
        self.op = self.get_op(&args);
    }

    fn get_op(&self, args: &Vec<&str>) -> Operation{
        let arg_num: u16 = args[0].trim_start_matches("*").parse().unwrap();
        println!("arg_num is {}", arg_num);
        /*for arg in args{
            println!("{:?}", arg);
        }*/
        let op_str = args[2].to_uppercase();
        println!("OP[{}]", op_str);
        match op_str.as_ref(){
            "GET" => Operation::Get(args[4].to_string()),
            "STRLEN" => Operation::StrLen(args[4].to_string()),
            "SET" => Operation::Set(args[4].to_string(), args[6].to_string(), false),
            "SETNX" => Operation::Set(args[4].to_string(), args[6].to_string(), true),
            "GETSET" => Operation::GetSet(args[4].to_string(), args[6].to_string()),
            "APPEND" => Operation::Append(args[4].to_string(), args[6].to_string()),
            "SETRANGE" => {
                let off: usize = args[6].parse().unwrap();
                Operation::SetRange(args[4].to_string(), off, args[8].to_string())
            },
            "GETRANGE" => {
                let start_off: i32 = args[6].parse().unwrap();
                let end_off: i32 = args[8].parse().unwrap();
                Operation::GetRange(args[4].to_string(), start_off, end_off)
            },
            "MSET" =>{
                let mut pos: usize = 4;
                let mut args_kv: Vec<(String, String)> = Vec::new();
                while pos < args.len(){
                    args_kv.push((args[pos].to_string(), args[pos + 2].to_string()));
                    pos += 4;
                }
                Operation::Mset(args_kv)
            },
            "MGET" =>{
                let mut pos: usize = 4;
                let mut args_kv: Vec<String> = Vec::new();
                while pos < args.len(){
                    args_kv.push(args[pos].to_string());
                    pos += 2;
                }
                Operation::Mget(args_kv)
            },

            _ => Operation::NotParsed,

        }
    }

    fn gen_multi_reply(&self, reply: Vec<String>) -> String{
        let field_num = reply.len();
        let iter = reply.iter().map(|x| format!("${}\r\n{}\r\n", x.len(), x));
        let mut res = format!("*{}\r\n", field_num);
        for item in iter{
            res += &item;
        }
        res
    }

    fn set(&mut self, kvs: Vec<(String, String)>) -> String{
        let mut succ_count = 0;
        for (key, value) in &kvs{
            self.db.write().unwrap().raw_put(key.clone(), value.clone()).unwrap();
            succ_count += 1;
        }
        if succ_count != kvs.len(){
            return "-FAILED\r\n".to_string();
        }else{
            return "+OK\r\n".to_string();
        }
    }

    fn get(&self, keys: Vec<String>) -> String{
        let mut values = Vec::new();
        let mut all_done = true;
        for key in keys{
            match self.db.read().unwrap().raw_get(key){
                Ok(value) => {
                    values.push(value);
                },
                Err(e) => {
                    match e {
                        DBError::NotFound => {
                            all_done = false;
                            break;
                        }

                        _ => (),
                    }
                }
            }
        }
        if all_done{
            self.gen_multi_reply(values)
        }else{
            "-FAILED\r\n".to_string()
        }
    }

    fn getset(&mut self, key: String, value: String) -> String{
        let res;
        {
            res = self.db.read().unwrap().raw_get(key.clone())
        }

        {
            match res{
                Ok(old_value) =>{
                    self.db.write().unwrap().raw_put(key, value).unwrap();
                    return format!("+{}\r\n", old_value);
                },

                Err(e) =>{
                    match e {
                        DBError::NotFound => {
                            self.db.write().unwrap().raw_put(key, value.clone()).unwrap();
                            return format!("+{}\r\n", value);
                        },
                        _ => return "-FAILED BY ERROR\r\n".to_string(),
                    }
                }
            }
        }

    }

    fn strlen(&self, key: String) -> String{
        match self.db.read().unwrap().raw_get(key){
            Ok(value) =>{
                return format!("+{}\r\n", value.len());
            },

            Err(e) =>{
                match e {
                    DBError::NotFound => return "-FAILED NOT FOUND\r\n".to_string(),
                    _ => return "-FAILED BY ERROR\r\n".to_string(),
                }
            }
        }
    }

    fn append(&mut self, key: String, value: String) -> String{
        let res;
        {
            res = self.db.read().unwrap().raw_get(key.clone());
        }
        {
            match res{
                Ok(old_value) =>{
                    self.db.write().unwrap().raw_put(key, old_value + &value).unwrap();
                    return "+OK\r\n".to_string();
                },

                Err(e) =>{
                    match e {
                        DBError::NotFound => return "-FAILED NOT FOUND\r\n".to_string(),
                        _ => return "-FAILED BY ERROR\r\n".to_string(),
                    }
                }
            }
        }

    }

    fn get_range(&self, key: String, start: i32, end: i32) -> String{
        match self.db.read().unwrap().raw_get(key.clone()){
            Ok(value) =>{
                let start_abs = if start < 0{
                    value.len() as i32 + start
                }else{
                    start
                };
                let mut end_abs = if end < 0{
                    value.len() as i32 + end
                }else{
                    end
                };
                if start_abs > end_abs{
                    return "-FAILED\r\n".to_string();
                }

                if start_abs >= value.len() as i32{
                    return "+\r\n".to_string();
                }

                if end_abs >= value.len() as i32{
                    end_abs = value.len() as i32 - 1;
                }

                let start_abs = start_abs as usize;
                let end_abs = end_abs as usize;
                let slice = value.get(start_abs..end_abs);
                match slice{
                    Some(res) =>{
                        return format!("+{}\r\n", res);
                    },

                    None =>{
                        return "-FAILED\r\n".to_string();
                    }
                }
            },

            Err(e) =>{
                match e {
                    DBError::NotFound => return "-FAILED NOT FOUND\r\n".to_string(),
                    _ => return "-FAILED BY ERROR\r\n".to_string(),
                }
            }
        }
    }

    fn set_range(&mut self, key: String, off: usize, data: String) -> String{
        let res;
        {
            res = self.db.read().unwrap().raw_get(key.clone());
        }

        {
            match res{
                Ok(old_value) =>{
                    let mut new_value = old_value.clone();
                    if off >= new_value.len(){
                        // off execeeds len of old value and write "0" bytes
                        let nil: Vec<u8> = vec![0; off - new_value.len()];
                        let nil = String::from_utf8(nil).unwrap();
                        new_value += &nil;
                        new_value += &data;
                    }else if off + data.len() >= new_value.len(){
                        new_value.truncate(off);
                        new_value += &data;
                    }else{
                        println!("value is {}, off is {}, data.len() is {}",old_value, off, data.len());
                        new_value.replace_range(off..(off + data.len()), &data);
                    }
                    let new_len = new_value.len();
                    self.db.write().unwrap().raw_put(key, new_value).unwrap();
                    return format!("+{}\r\n", new_len);
                },

                Err(e) =>{
                    match e {
                        DBError::NotFound => {
                            let nil: Vec<u8> = vec![0; off];
                            let nil = String::from_utf8(nil).unwrap();
                            let new_value = nil + &data;
                            self.db.write().unwrap().raw_put(key, new_value.clone()).unwrap();
                            return format!("+{}\r\n", new_value.len());
                        },
                        _ => return "-FAILED BY ERROR\r\n".to_string(),
                    }
                }
            }
        }

    }
}

#[cfg(test)]
mod tests{
    use crate::simple_mem_db;
    use crate::redis_server::DB;
    use crate::executor::Executor;
    use std::sync::{Arc, RwLock};
    use std::sync::mpsc::{channel, Sender, Receiver};

    fn gen_redis_code(raw_code: String) -> String{
        let args: Vec<&str> = raw_code.split(" ").collect();
        let args_num = args.len();
        let args = args.iter().map(|x| format!("${}\r\n{}\r\n", x.len(), x));
        let mut output = format!("*{}\r\n", args_num);
        for item in args{
            output += &item;
        }
        //println!("get command {}", output);
        output
    }

    fn exec<E: DB>(db: Arc<RwLock<E>>, command: String, tx: Sender<String>){
        let mut executor = Executor::new(
            db.clone(),
            command.into_bytes(),
            tx.clone());
        executor.parse();
        executor.exec_command();
    }

    #[test]
    fn test_executor_basic_put_get(){
        let db = simple_mem_db::SimpleMemDB::new();
        let db = Arc::new(RwLock::new(db));
        let put_command = gen_redis_code("set foo bar".to_string());
        let (tx, rx) = channel();
        exec(db.clone(), put_command, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());

        let get_command = gen_redis_code("get foo".to_string());
        exec(db.clone(), get_command, tx.clone());
        assert_eq!(rx.recv().unwrap(), "*1\r\n$3\r\nbar\r\n".to_string());

        let get_command = gen_redis_code("get no".to_string());
        exec(db.clone(), get_command, tx.clone());
        assert_eq!(rx.recv().unwrap(), "-FAILED\r\n".to_string());
    }

    #[test]
    fn test_executor_multi_put_get(){
        let db = simple_mem_db::SimpleMemDB::new();
        let db = Arc::new(RwLock::new(db));
        let mset_command = gen_redis_code("mset foo1 bar1 foo2 bar2 foo3 bar3".to_string());
        let (tx, rx) = channel();
        exec(db.clone(), mset_command, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());

        let mget_command = gen_redis_code("mget foo1 foo2 foo3".to_string());
        exec(db.clone(), mget_command, tx.clone());
        assert_eq!(rx.recv().unwrap(), "*3\r\n$4\r\nbar1\r\n$4\r\nbar2\r\n$4\r\nbar3\r\n".to_string());
    }

    #[test]
    fn test_executor_strlen(){
        let db = simple_mem_db::SimpleMemDB::new();
        let db = Arc::new(RwLock::new(db));
        let set_command = gen_redis_code("set foo bar".to_string());
        let strlen_command = gen_redis_code("strlen foo".to_string());
        let (tx, rx) = channel();
        exec(db.clone(), set_command, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());

        exec(db.clone(), strlen_command, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+3\r\n".to_string());
    }


    #[test]
    fn test_executor_getset(){
        let db = simple_mem_db::SimpleMemDB::new();
        let db = Arc::new(RwLock::new(db));
        let command1 = gen_redis_code("set foo bar".to_string());
        let command2 = gen_redis_code("getset foo bar2".to_string());
        let command3 = gen_redis_code("getset foo3 bar3".to_string());
        let (tx, rx) = channel();
        exec(db.clone(), command1, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());

        exec(db.clone(), command2, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+bar\r\n".to_string());

        exec(db.clone(), command3, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+bar3\r\n".to_string());
    }

    #[test]
    fn test_executor_append(){
        println!("init db");
        let db = simple_mem_db::SimpleMemDB::new();
        let db = Arc::new(RwLock::new(db));
        println!("get new db");
        let command1 = gen_redis_code("set hello world".to_string());
        let command2 = gen_redis_code("append hello bye".to_string());
        let command3 = gen_redis_code("get hello".to_string());
        let (tx, rx) = channel();
        println!("before new db");
        exec(db.clone(), command1, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());
        println!("finish set");

        exec(db.clone(), command2, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());

        exec(db.clone(), command3, tx.clone());
        assert_eq!(rx.recv().unwrap(), "*1\r\n$8\r\nworldbye\r\n".to_string());
    }

    #[test]
    fn test_executor_getrange(){
        let db = simple_mem_db::SimpleMemDB::new();
        let db = Arc::new(RwLock::new(db));
        let command1 = gen_redis_code("set me 18696192030".to_string());
        let command2 = gen_redis_code("getrange me 1 -1".to_string());
        let command3 = gen_redis_code("getrange me -1 1".to_string());
        let (tx, rx) = channel();
        exec(db.clone(), command1, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());

        exec(db.clone(), command2, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+869619203\r\n".to_string());

        exec(db.clone(), command3, tx.clone());
        assert_eq!(rx.recv().unwrap(), "-FAILED\r\n".to_string());
    }

    #[test]
    fn test_executor_setrange(){
        let db = simple_mem_db::SimpleMemDB::new();
        let db = Arc::new(RwLock::new(db));
        let command1 = gen_redis_code("set me 18696192030".to_string());
        let command2 = gen_redis_code("setrange me 4 00".to_string());
        let command3 = gen_redis_code("setrange me 11 ++++++".to_string());
        let command4 = gen_redis_code("setrange empty 5 hahhaha".to_string());
        let (tx, rx) = channel();
        exec(db.clone(), command1, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+OK\r\n".to_string());

        exec(db.clone(), command2, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+11\r\n".to_string());

        exec(db.clone(), command3, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+17\r\n".to_string());

        exec(db.clone(), command4, tx.clone());
        assert_eq!(rx.recv().unwrap(), "+12\r\n".to_string());
    }
}