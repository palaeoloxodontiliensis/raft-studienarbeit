use openraft::Raft;
use std::io::Cursor;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SetResponse {
    pub value: Option<String>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = SetRequest,
        R = SetResponse,
        NodeId = u64,
);

pub type ExampleRaft = Raft<TypeConfig>;