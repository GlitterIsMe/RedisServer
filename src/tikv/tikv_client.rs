use std::{fmt, sync::Arc, time::Duration};
use std::result;

use futures::Future;
use grpcio::{CallOption, Environment};
use grpcio::ChannelBuilder;
use kvproto::{errorpb, kvrpcpb, tikvpb_grpc::TikvClient};
use protobuf;

use super::tikv_db::RawContext;
use super::tikv_db::Key;
use super::tikv_db::Value;
use super::tikv_db::Result;

pub struct KVClient{
    client: Arc<TikvClient>,
    address: String,
}

impl KVClient{
    pub fn new(env: Arc<Environment>, addr: &str) -> Result<KVClient>{
        let addr = addr
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        let cb = ChannelBuilder::new(env)
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3));

        let channel = cb.connect(addr);

        let tikv_client = TikvClient::new(channel);
        let client = Arc::new(tikv_client);
        Ok(KVClient{
            client,
            address: addr.to_owned(),
        })
    }
    pub fn raw_put(
        &self,
        context: RawContext,
        key: Key,
        value: Value,
    ) {
        let mut req = self.new_raw_put_req(context, key, value);
        self.client.raw_put(&req);
    }

    fn new_raw_put_req(&self, context: RawContext, key: Key, value: Value) -> RawPutRequest{
        let mut req = RawPutRequest::new();
        let (region, cf) = context.into_inner();
        req.set_context(region.into());
        if let Some(cf) = cf {
            req.set_cf(cf.into_inner());
        }
        req.set_key(key.into_inner());
        req.set_value(key.into_inner());
        req
    }

    pub fn raw_get(
        &self,
        context: RawContext,
        key: Key,
    ) -> Value {
        // RawContext包含region、对应的kvclient、以及cf信息
        // raw_request宏是用于生成一个request
        let mut req = self.new_raw_get_req(context, key);
        let res = self.client.raw_get(&req);
        res.take_value()
        // 通过TiKVClient就可以进行RPC调用
    }

    fn new_raw_get_req(&self, context: RawContext, key: Key) -> RawGetRequest{
        let mut req = RawGetRequest::new();
        let (region, cf) = context.into_inner();
        req.set_context(region.into());
        if let Some(cf) = cf {
            req.set_cf(cf.into_inner());
        }
        req.set_key(key.into_inner());
        req
    }



}