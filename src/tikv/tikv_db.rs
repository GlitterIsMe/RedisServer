use std::sync::{Arc, RwLock};
use std::result;
use std::collections::HashMap;
use std::fmt;

use super::pd_client::PDClient;
use super::tikv_client::KVClient;
use super::context::{RawContext, RegionContext, Region, Peer};

use crate::redis_server::DB;

use grpcio::{Environment, EnvBuilder};
use kvproto::metapb;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug)]
pub enum Error {
    TiKVError(Stirng),
    PdError(String),
    OperationError(String),
    Other,
}

pub type Result<T> = result::Result<T, Error>;


pub struct TikvDB {
    pd: Arc<PDClient>,
    kvserver: Arc<RwLock<HashMap<String, Arc<KVClient>>>>,
    env: Arc<Environment>,
}

impl TikvDB {
    pub fn connect(end_points: Vec<String>) -> Result<TikvDB> {
        // config中存储了pd的endpoint
        // 新建一个新的grpc enviroment
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix("tikc_client".to_string())
                .build(),
        );
        // 与pd连接
        let pd = Arc::new(PDClient::new(Arc::clone(&env), &end_points, )?);
        // 与tikv连接
        let tikv = Default::default();

        Ok(TikvDB{
            pd,
            kvserver: tikv,
            env,
        })
    }

    fn locate_key(&self, key: &Key)-> Region{
        self.pd.get_region(key.as_ref())
    }

    fn load_store(&self, id: u64) -> metapb::Store{
        println!("reload info for store {}", id);
        self.pd.get_store(id)
    }

    fn kv_client(&self, context: RegionContext) -> Result<(RegionContext, Arc<KVClient>)> {
        if let Some(conn) = self.kvserver.read().unwrap().get(context.address()) {
            // 从client的hashmap中记录的addr与cilent映射直接获得client，如果没有则需要重新获取
            return Ok((context, Arc::clone(conn)));
        };
        println!("connect to tikv endpoint: {:?}", context.address());
        let tikv = Arc::clone(&self.kvserver);
        // 去连接这个TiKV Server
        let client = Arc::new(KVClient::new(
            Arc::clone(&self.env),
            context.address(),
        )?);
        // 记录这个新的addr与client的映射
        self.kvserver.write().unwrap().insert(context.address().to_owned(), Arc::clone(&client));
        Ok((context, client))
    }

    fn get_region_context(&self, key: &Key) -> (RegionContext, Arc<KVClient>){
        // 定位key在哪个region
        let location = self.locate_key(key);
        // 获取到region之后获取peer
        let peer = location.peer().unwrap();
        // 从peer获取store id
        let store_id = peer.get_store_id();
        // 获取store
        let store = self.load_store(store_id);
        // 把region和store都返回
        let region_contex = RegionContext{
            region: location,
            store,
        };
        self.kv_client(region_contex).unwrap()
    }

    fn get_raw_context(&self, key: &Key, cf: Option<String>) -> RawContext {
        //获取raw contxt
        let (region, client) = self.get_region_context(key);
        RawContext::new(region, client, cf)
    }

    pub fn tikv_raw_put(&self, key: Key, value: Value, cf: Option<String>) -> Result<()> {
        if value.is_empty() {
            Err(Error::OpError)
        } else {
            let context = self.get_raw_context(&key, cf);
            context.client().raw_put(context, key, value);
            Ok(())
        }
    }

    pub fn tikv_raw_get(&self, key: Key, cf: Option<String>)-> Option<Value>{
        let context = self.get_raw_context(&key, cf);
        let v = context.client().raw_get(context, key);
        if v.is_empty(){
            None
        }else{
            Some(v)
        }
    }
}

use crate::redis_server::DBError;

impl DB for TikvDB{
    fn raw_put(&mut self, key: String, value: String) -> result::Result<String, DBError>{
        if self.tikv_raw_put(key.into_bytes(), value.into_bytes(), None).is_ok(){
            Ok("OK".to_string())
        }else{
            Err(DBError::Other)
        }
    }

    fn raw_get(&self, key: String) -> result::Result<String, DBError>{
        if let Some(v) = self.tikv_raw_get(key.into_bytes(), None){
            Ok(String::from_utf8(v).unwrap())
        }else{
            Err(DBError::NotFound)
        }
    }
}

#[cfg(test)]
mod test{
    use crate::tikv::tikv_db::TikvDB;
    use crate::redis_server::DB;

    #[test]
    fn test_tikv_new_db(){
        let end_point = vec!["127.0.0.1:2379".to_string()];
        let tikv_db = TikvDB::connect(end_point).unwrap();
    }

    #[test]
    fn test_tikv_basic_put_get(){
        let end_point = vec!["127.0.0.1:2379".to_string()];
        let mut tikv_db = TikvDB::connect(end_point).unwrap();
        let key = "foo".to_string();
        let value = "bar".to_string();
        assert_eq!(tikv_db.raw_put(key.clone(), value).unwrap(), "OK".to_string());
        assert_eq!(tikv_db.raw_get(key.clone()).unwrap(), "bar".to_string());
    }
}