use std::{
    fmt,
    sync::{Arc, RwLock},
    time::Duration,
};

use std::collections::HashSet;

use futures::Future;
use grpcio::{CallOption, Environment, ChannelBuilder};
use kvproto::{metapb, pdpb, pdpb_grpc::PdClient as RpcClient};

use super::tikv_db::Result;
use super::tikv_db::Error;
use super::context::Region;

fn connect_pd_client(
    env: Arc<Environment>,
    addr: &str,
) -> Result<(pdpb_grpc::PdClient, pdpb::GetMembersResponse)> {
    let addr = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let cb = ChannelBuilder::new(env)
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3));

    let channel = cb.connect(addr);

    let pd_client = pdpb_grpc::PdClient::new(channel);
    let option = CallOption::default().timeout(timeout);
    let resp = pd_client.get_members_opt(&pdpb::GetMembersRequest::new(), option).unwrap();
    Ok((pd_client, resp))
}

fn try_connect_pd_client(
    env: &Arc<Environment>,
    addr: &str,
    cluster_id: u64,
) -> Result<(pdpb_grpc::PdClient, pdpb::GetMembersResponse)> {
    let (client, r) = connect_pd_client(Arc::clone(&env), addr)?;
    let new_cluster_id = r.get_header().get_cluster_id();
    if new_cluster_id != cluster_id {
        Err(Error::InOtherCluster)
    } else {
        Ok((client, r))
    }
}

fn try_connect_pd_leader(
    env: &Arc<Environment>,
    previous: &pdpb::GetMembersResponse,
) -> Result<(pdpb_grpc::PdClient, pdpb::GetMembersResponse)> {
    // previous是最后一次connect endpoint的时候的回复
    // 从这个回复可以知道前一次的leader、members以及cluster_id
    let previous_leader = previous.get_leader();
    let members = previous.get_members();
    let cluster_id = previous.get_header().get_cluster_id();
    let mut resp = None;
    // Try to connect to other members, then the previous leader.
    'outer: for m in members
        .iter()
        .filter(|m| *m != previous_leader)
        .chain(&[previous_leader.clone()])
        {
            for ep in m.get_client_urls() {
                match try_connect_pd_client(env, ep.as_str(), cluster_id) {
                    Ok((_, r)) => {
                        resp = Some(r);
                        break 'outer;
                    }
                    Err(e) => {
                        error!("failed to connect to {}, {:?}", ep, e);
                        continue;
                    }
                }
            }
        }

    // Then try to connect the PD cluster leader.
    if let Some(resp) = resp {
        let leader = resp.get_leader().clone();
        for ep in leader.get_client_urls() {
            let r = try_connect_pd_client(&env, ep.as_str(), cluster_id);
            if r.is_ok() {
                return r;
            }
        }
    }

    Err(internal_err!("failed to connect to {:?}", members))
}

fn new_pd_request(cluster_id: u64) -> pdpb::GetRegionRequest{

    let mut request = pdpb::GetRegionRequest::new();
    let mut header = ::kvproto::pdpb::RequestHeader::new();
    header.set_cluster_id(cluster_id);
    request.set_header(header);
    request
}

pub struct LeaderClient {
    pub client: pdpb_grpc::PdClient,
    pub members: pdpb::GetMembersResponse,
    env: Arc<Environment>,
    cluster_id: u64,
}

impl LeaderClient{
    pub fn new(
        env: Arc<Environment>,
        endpoints: &[String]
    ) -> Result<Arc<RwLock<LeaderClient>>> {
        let (client, members) = LeaderClient::validate_endpoints(&env, endpoints)?;
        let cluster_id = members.get_header().get_cluster_id();
        let client = Arc::new(RwLock::new(LeaderClient {
            client,
            members,
            env,
            cluster_id,
        }));
        Ok(client)
    }

    pub fn validate_endpoints(
        env: &Arc<Environment>,
        endpoints: &[String]
    ) -> Result<(pdpb_grpc::PdClient, pdpb::GetMembersResponse)> {
        let len = endpoints.len();
        let mut endpoints_set = HashSet::with_capacity_and_hasher(len, Default::default());

        let mut members = None;
        let mut cluster_id = None;
        for ep in endpoints {
            if !endpoints_set.insert(ep) {
                return Err(Error::DeduplicatedMember);
            }

            let (_, resp) = match connect_pd_client(Arc::clone(&env), ep) {
                // connect this endpoint
                Ok(resp) => resp,
                // Ignore failed PD node.
                Err(e) => {
                    println!("failed to connect pd_client@[{}]", ep);
                    continue;
                }
            };

            // Check cluster ID.
            let cid = resp.get_header().get_cluster_id();
            if let Some(sample) = cluster_id {
                if sample != cid {
                    return Err(Error::PDError);
                }
            } else {
                cluster_id = Some(cid);
            }

            if members.is_none() {
                members = Some(resp);
            }
        }

        match members {
            Some(members) => {
                let (client, members) = try_connect_leader(&env, &members)?;
                println!("All PD endpoints are consistent: {:?}", endpoints);
                Ok((client, members))
            }
            _ => Err(Error::PDClientResponseFailed),
        }
    }
}

pub struct PDClient{
    cluster_id: u64,
    // PD也是一个集群， 与PD的交互通过leader进行
    leader: Arc<RwLock<LeaderClient>>,
}

impl PDClient{
    pub fn new(env: Arc<Environment>, endpoints: &[String]) -> Result<PDClient>{
        let leader = LeaderClient::new(env, endpoints)?;
        let cluster_id = leader.read().unwrap().cluster_id();
        Ok(PdClient {
            cluster_id,
            leader,
        })
    }

    fn get_leader(&self) -> pdpb::Member {
        self.leader.read().unwrap().members.get_leader().clone()
    }

    fn get_region_and_leader(&self, key: &[u8]) -> (metapb::Region, Option<metapb::Peer>){
        let mut req = new_pd_request(self.cluster_id);
        req.set_region_key(key.to_owned());
        let key = req.get_region_key().to_owned();
        // 通过rpc调用去获得当前key的region
        let res = self.leader.read().unwrap().get_region(&req).unwrap();
        let region = if res.has_region(){
            resp.take_region()
        }else{
            println!("not get a region");
            Error::PDError;
        };

        let leader = if resp.has_leader(){
            Some(resp.take_leader())
        }else{
            None
        };

        Ok((region, leader))
    }

    pub fn get_region(&self, key: &[u8]) -> Region{
        let (region, leader) = self.get_region_and_leader(key);
        Region::new(region, leader)
    }
}