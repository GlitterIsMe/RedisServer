use std::sync::Arc;
use std::ops::{Deref, DerefMut};

use protobuf;
use kvproto::{kvrpcpb, metapb};

use super::tikv_db::{Result, Key, Value, Error};
use super::tikv_client::KVClient;
use protobuf::Message;

pub struct RawContext {
    region: RegionContext,
    client: Arc<KVClient>,
    cf: Option<String>,
}

impl RawContext {
    pub fn new(region: RegionContext, client: Arc<KVClient>, cf: Option<String>) -> Self {
        RawContext { region, client, cf }
    }

    pub fn client(&self) -> Arc<KVClient> {
        Arc::clone(&self.client)
    }

    pub fn into_inner(self) -> (RegionContext, Option<String>) {
        (self.region, self.cf)
    }
}


pub struct RegionContext {
    pub region: Region,
    pub store: metapb::Store,
}


impl RegionContext {
    pub fn address(&self) -> &str {
        self.store.get_address()
    }

    pub fn start_key(&self) -> Key {
        self.region.start_key().to_vec().into()
    }

    pub fn end_key(&self) -> Key {
        self.region.end_key().to_vec().into()
    }

    pub fn range(&self) -> (Key, Key) {
        (self.start_key(), self.end_key())
    }
}

impl From<RegionContext> for kvrpcpb::Context {
    fn from(mut ctx: RegionContext) -> kvrpcpb::Context {
        let mut kvctx = kvrpcpb::Context::new();
        kvctx.set_region_id(ctx.region.id);
        kvctx.set_region_epoch(ctx.region.take_region_epoch());
        kvctx.set_peer(ctx.region.peer().expect("leader must exist").into_inner());
        kvctx
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct Region {
    pub region: metapb::Region,
    pub leader: Option<Peer>,
}

impl Deref for Region {
    type Target = metapb::Region;

    fn deref(&self) -> &Self::Target {
        &self.region
    }
}

impl DerefMut for Region {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.region
    }
}

impl Region {
    pub fn new(region: metapb::Region, leader: Option<metapb::Peer>) -> Self {
        Region {
            region,
            leader: leader.map(Peer),
        }
    }

    /*pub fn switch_peer(&mut self, _to: StoreId) -> Result<()> {
        unimplemented!()
    }*/

    pub fn contains(&self, key: &Key) -> bool {
        let key: &[u8] = key.as_ref();
        let start_key = self.region.get_start_key();
        let end_key = self.region.get_end_key();
        start_key <= key && (end_key > key || end_key.is_empty())
    }

    pub fn context(&self) -> Result<kvrpcpb::Context> {
        self.leader
            .as_ref()
            .ok_or_else(|| Error::Other)
            .map(|l| {
                let mut ctx = kvrpcpb::Context::default();
                ctx.set_region_id(self.region.get_id());
                ctx.set_region_epoch(Clone::clone(self.region.get_region_epoch()));
                ctx.set_peer(Clone::clone(l));
                ctx
            })
    }

    pub fn start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    pub fn end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }

    /*pub fn ver_id(&self) -> RegionVerId {
        let region = &self.region;
        let epoch = region.get_region_epoch();
        RegionVerId {
            id: region.get_id(),
            conf_ver: epoch.get_conf_ver(),
            ver: epoch.get_version(),
        }
    }*/

    pub fn id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn peer(&self) -> Result<Peer> {
        self.leader
            .as_ref()
            .map(Clone::clone)
            .map(Into::into)
            .ok_or_else(|| Error::Other)
    }

    pub fn meta(&self) -> metapb::Region {
        Clone::clone(&self.region)
    }
}


#[derive(Clone, Default, Debug, PartialEq)]
pub struct Peer(metapb::Peer);

impl From<metapb::Peer> for Peer {
    fn from(peer: metapb::Peer) -> Peer {
        Peer(peer)
    }
}

impl Deref for Peer {
    type Target = metapb::Peer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Peer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Peer {
    pub fn into_inner(self) -> metapb::Peer {
        self.0
    }
}

