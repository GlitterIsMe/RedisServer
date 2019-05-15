use crate::redis_server::DB;
use crate::redis_server::DBError;
use std::collections::btree_map::BTreeMap;

#[derive(Clone)]
pub struct SimpleMemDB{
    table: BTreeMap<String, String>,
}

impl SimpleMemDB {
    pub fn new() -> Self{
        SimpleMemDB{
            table: BTreeMap::new(),
        }
    }
}

impl DB for SimpleMemDB{
    fn raw_put(&mut self, key: String, value: String) -> Result<String, DBError>{
        self.table.insert(key, value);
        Ok("Ok".to_string())
    }

    fn raw_get(&self, key: String) -> Result<String, DBError>{
        match self.table.get(&key){
            Some(value) => Ok(value.clone()),
            None => Err(DBError::NotFound),
        }
    }
}