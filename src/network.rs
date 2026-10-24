// src/network.rs
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::types::{ExampleRaft, TypeConfig};

pub type NodeId = u64;

type AppendResult = Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>;
type VoteResult = Result<VoteResponse<NodeId>, RaftError<NodeId>>;
type SnapshotResult =
Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>;

/// Nachrichten, die zwischen den simulierten Knoten verschickt werden.
pub enum RpcMessage {
    AppendEntries(AppendEntriesRequest<TypeConfig>, oneshot::Sender<AppendResult>),
    Vote(VoteRequest<NodeId>, oneshot::Sender<VoteResult>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>, oneshot::Sender<SnapshotResult>),
}

/// Zentraler "Netzwerk-Bus": kennt die Inbox jedes Knotens.
#[derive(Clone, Default)]
pub struct Router {
    pub inboxes: Arc<RwLock<HashMap<NodeId, mpsc::UnboundedSender<RpcMessage>>>>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// Knoten ans Netzwerk anschließen (oder wieder anschließen).
    pub async fn register(&self, id: NodeId, tx: mpsc::UnboundedSender<RpcMessage>) {
        self.inboxes.write().await.insert(id, tx);
    }

    /// Knoten vom Netzwerk trennen (simuliert Ausfall / Partition).
    pub async fn disconnect(&self, id: NodeId) {
        self.inboxes.write().await.remove(&id);
    }
}

/// Verbindung von einem Knoten zu genau einem Ziel-Knoten.
pub struct ChannelNetwork {
    target: NodeId,
    router: Router,
}

fn unreachable_err<E: std::error::Error>(msg: &str) -> RPCError<NodeId, BasicNode, E> {
    let e = io::Error::new(io::ErrorKind::NotConnected, msg.to_string());
    RPCError::Unreachable(Unreachable::new(&e))
}

impl ChannelNetwork {
    /// Gemeinsame Logik für alle drei RPC-Typen.
    async fn call<Req, Resp, E>(
        &self,
        rpc: Req,
        wrap: impl FnOnce(Req, oneshot::Sender<Result<Resp, E>>) -> RpcMessage,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, E>>
    where
        E: std::error::Error,
    {
        let (tx, rx) = oneshot::channel();

        // Lock nicht über das await auf die Antwort halten
        let sender = {
            let inboxes = self.router.inboxes.read().await;
            inboxes.get(&self.target).cloned()
        };
        let sender = sender.ok_or_else(|| unreachable_err("Knoten offline"))?;

        sender
            .send(wrap(rpc, tx))
            .map_err(|_| unreachable_err("Inbox geschlossen"))?;

        match rx.await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
            Err(_) => Err(unreachable_err("Antwort verloren")),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for Router {
    type Network = ChannelNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        ChannelNetwork {
            target,
            router: self.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for ChannelNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        self.call(rpc, RpcMessage::AppendEntries).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.call(rpc, RpcMessage::Vote).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
    InstallSnapshotResponse<NodeId>,
    RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
    self.call(rpc, RpcMessage::InstallSnapshot).await
    }
}

/// Empfangsschleife eines Knotens: reicht eingehende RPCs an sein Raft-Objekt weiter.
pub async fn run_inbox_loop(raft: ExampleRaft, mut rx: mpsc::UnboundedReceiver<RpcMessage>) {
    while let Some(msg) = rx.recv().await {
        match msg {
            RpcMessage::AppendEntries(req, reply) => {
                let _ = reply.send(raft.append_entries(req).await);
            }
            RpcMessage::Vote(req, reply) => {
                let _ = reply.send(raft.vote(req).await);
            }
            RpcMessage::InstallSnapshot(req, reply) => {
                let _ = reply.send(raft.install_snapshot(req).await);
            }
        }
    }
}