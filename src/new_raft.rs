use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{mpsc, oneshot, watch, RwLock};
use tokio::time::Instant;

pub type NodeId = u64;
pub type Term = u64;

// ===================== Log =====================

#[derive(Clone, Debug)]
pub struct LogEntry<C> {
    pub term: Term,
    pub command: C,
}

// ===================== RPC-Typen =====================

#[derive(Clone, Debug)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: Term,
}

#[derive(Clone, Debug)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Clone, Debug)]
pub struct AppendRequest<C> {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<C>>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug)]
pub struct AppendResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: u64,    // bei Erfolg: neuer Stand
    pub conflict_index: u64, // bei Misserfolg: wo der Leader weiterprobieren soll
}

/// Was ein Knoten von außen über das Netzwerk empfängt.
pub enum Rpc<C> {
    Vote(VoteRequest, oneshot::Sender<VoteResponse>),
    Append(AppendRequest<C>, oneshot::Sender<AppendResponse>),
}

// ===================== "Netzwerk": jetzt registry statt Router =====================

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Default)]
pub struct FaultConfig {
    pub drop_permille: u32,   // 0..1000, Wahrscheinlichkeit für Paketverlust
    pub extra_delay: Duration,
}

#[derive(Default)]
pub struct NetworkMetrics {
    pub votes_sent: AtomicU64,
    pub appends_sent: AtomicU64,
    pub dropped: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct RpcCounts {
    pub votes_sent: u64,
    pub appends_sent: u64,
    pub dropped: u64,
}

pub struct Registry<C> {
    inner: Arc<RwLock<HashMap<NodeId, mpsc::UnboundedSender<Rpc<C>>>>>,
    fault: Arc<RwLock<FaultConfig>>,
    metrics: Arc<NetworkMetrics>,
}

impl<C> Clone for Registry<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fault: self.fault.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<C> Default for Registry<C> {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            fault: Arc::new(RwLock::new(FaultConfig::default())),
            metrics: Arc::new(NetworkMetrics::default()),
        }
    }
}

impl<C: Clone + Send + 'static> Registry<C> {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, id: NodeId, tx: mpsc::UnboundedSender<Rpc<C>>) {
        self.inner.write().await.insert(id, tx);
    }

    pub async fn disconnect(&self, id: NodeId) {
        self.inner.write().await.remove(&id);
    }

    /// Paketverlust (0..1000 Promille) und künstliche Verzögerung einstellen.
    pub async fn set_fault(&self, drop_permille: u32, extra_delay: Duration) {
        *self.fault.write().await = FaultConfig { drop_permille, extra_delay };
    }

    pub fn counts(&self) -> RpcCounts {
        RpcCounts {
            votes_sent: self.metrics.votes_sent.load(Ordering::Relaxed),
            appends_sent: self.metrics.appends_sent.load(Ordering::Relaxed),
            dropped: self.metrics.dropped.load(Ordering::Relaxed),
        }
    }

    async fn get(&self, id: NodeId) -> Option<mpsc::UnboundedSender<Rpc<C>>> {
        self.inner.read().await.get(&id).cloned()
    }

    /// true = diese Nachricht fällt aus (simulierter Paketverlust).
    async fn apply_fault(&self) -> bool {
        let f = self.fault.read().await.clone();
        if !f.extra_delay.is_zero() {
            tokio::time::sleep(f.extra_delay).await;
        }
        f.drop_permille > 0 && rand::thread_rng().gen_range(0..1000) < f.drop_permille
    }

    pub async fn send_vote(&self, target: NodeId, req: VoteRequest) -> Option<VoteResponse> {
        self.metrics.votes_sent.fetch_add(1, Ordering::Relaxed);
        if self.apply_fault().await {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let tx = self.get(target).await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Rpc::Vote(req, reply_tx)).ok()?;
        reply_rx.await.ok()
    }

    pub async fn send_append(&self, target: NodeId, req: AppendRequest<C>) -> Option<AppendResponse> {
        self.metrics.appends_sent.fetch_add(1, Ordering::Relaxed);
        if self.apply_fault().await {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let tx = self.get(target).await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Rpc::Append(req, reply_tx)).ok()?;
        reply_rx.await.ok()
    }
}

// ===================== Öffentliche API =====================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
pub struct Metrics {
    pub role: Role,
    pub term: Term,
    pub leader_id: Option<NodeId>,
    pub last_log_index: u64,
    pub commit_index: u64,
    pub last_applied: u64,
}

#[derive(Debug)]
pub struct NotLeader {
    pub leader_hint: Option<NodeId>,
}

#[derive(Clone)]
pub struct RaftConfig {
    pub election_timeout: (Duration, Duration), // (min, max)
    pub heartbeat_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout: (Duration::from_millis(150), Duration::from_millis(300)),
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}

/// Handle, über das die Außenwelt (dein main.rs) mit einem Knoten spricht.
pub struct RaftHandle<C, R> {
    pub id: NodeId,
    propose_tx: mpsc::UnboundedSender<(C, oneshot::Sender<Result<R, NotLeader>>)>,
    pub metrics: watch::Receiver<Metrics>,
}

impl<C, R> Clone for RaftHandle<C, R> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            propose_tx: self.propose_tx.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<C: Send + 'static, R: Send + 'static> RaftHandle<C, R> {
    /// Schreibt einen Befehl. Wartet, bis er committed und angewendet ist.
    pub async fn propose(&self, command: C) -> Result<R, NotLeader> {
        let (tx, rx) = oneshot::channel();
        if self.propose_tx.send((command, tx)).is_err() {
            return Err(NotLeader { leader_hint: None });
        }
        rx.await.unwrap_or(Err(NotLeader { leader_hint: None }))
    }

    pub fn current_metrics(&self) -> Metrics {
        self.metrics.borrow().clone()
    }
}

// ===================== Interne Ereignisse =====================

enum Internal {
    VoteReply(NodeId, Term, VoteResponse),
    AppendReply(NodeId, Term, u64 /* prev_sent */, u64 /* entries_len */, AppendResponse),
}

// ===================== Der eigentliche Knoten =====================

struct Node<C, R, F> {
    id: NodeId,
    role: Role,
    term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,

    log: Vec<LogEntry<C>>, // 1-indexiert gedacht: log[i] hat Raft-Index i+1
    commit_index: u64,
    last_applied: u64,

    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    votes_received: HashSet<NodeId>,

    peer_ids: Vec<NodeId>,
    registry: Registry<C>,

    rpc_rx: mpsc::UnboundedReceiver<Rpc<C>>,
    propose_rx: mpsc::UnboundedReceiver<(C, oneshot::Sender<Result<R, NotLeader>>)>,
    internal_tx: mpsc::UnboundedSender<Internal>,
    internal_rx: mpsc::UnboundedReceiver<Internal>,

    metrics_tx: watch::Sender<Metrics>,
    apply_fn: F,
    config: RaftConfig,
    election_deadline: Instant,

    pending_proposals: HashMap<u64, oneshot::Sender<Result<R, NotLeader>>>,
}

impl<C, R, F> Node<C, R, F>
where
    C: Clone + Send + 'static,
    R: Send + 'static,
    F: FnMut(&C) -> R + Send + 'static,
{
    async fn run(mut self) {
        self.reset_election_timer();
        self.publish_metrics();

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(self.election_deadline), if self.role != Role::Leader => {
                    self.start_election();
                }
                _ = tokio::time::sleep(self.config.heartbeat_interval), if self.role == Role::Leader => {
                    self.send_heartbeats();
                }
                Some(rpc) = self.rpc_rx.recv() => {
                    self.handle_rpc(rpc);
                }
                Some((cmd, reply)) = self.propose_rx.recv() => {
                    self.handle_propose(cmd, reply);
                }
                Some(ev) = self.internal_rx.recv() => {
                    self.handle_internal(ev);
                }
            }
        }
    }

    // ---------- Hilfsfunktionen auf dem Log ----------

    fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn term_at(&self, index: u64) -> Term {
        if index == 0 {
            0
        } else {
            self.log.get(index as usize - 1).map(|e| e.term).unwrap_or(0)
        }
    }

    fn quorum(&self) -> usize {
        (self.peer_ids.len() + 1) / 2 + 1
    }

    fn reset_election_timer(&mut self) {
        let (min, max) = self.config.election_timeout;
        let millis = rand::thread_rng().gen_range(min.as_millis() as u64..=max.as_millis() as u64);
        self.election_deadline = Instant::now() + Duration::from_millis(millis);
    }

    fn publish_metrics(&self) {
        let _ = self.metrics_tx.send(Metrics {
            role: self.role,
            term: self.term,
            leader_id: self.leader_id,
            last_log_index: self.last_log_index(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
        });
    }

    // ---------- Rollenwechsel ----------

    fn become_follower(&mut self, term: Term) {
        self.role = Role::Follower;
        self.term = term;
        self.voted_for = None;
        self.reset_election_timer();
        self.publish_metrics();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let next = self.last_log_index() + 1;
        for &peer in &self.peer_ids {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }
        self.match_index.insert(self.id, self.last_log_index());
        self.publish_metrics();
        self.send_heartbeats();
    }

    // ---------- Leader-Wahl ----------

    fn start_election(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes_received.clear();
        self.reset_election_timer();
        self.publish_metrics();

        if self.quorum() <= 1 {
            // Einzelknoten-"Cluster": sofort Leader.
            self.become_leader();
            return;
        }

        let last_log_index = self.last_log_index();
        let last_log_term = self.term_at(last_log_index);

        for &peer in &self.peer_ids {
            let req = VoteRequest {
                term: self.term,
                candidate_id: self.id,
                last_log_index,
                last_log_term,
            };
            let registry = self.registry.clone();
            let internal_tx = self.internal_tx.clone();
            let term = self.term;
            tokio::spawn(async move {
                if let Some(resp) = registry.send_vote(peer, req).await {
                    let _ = internal_tx.send(Internal::VoteReply(peer, term, resp));
                }
                // None = Paket verloren / Knoten offline: einfach keine Antwort,
                // genau wie beim echten Netzwerk-Timeout.
            });
        }
    }

    // ---------- Log-Replikation ----------

    fn send_heartbeats(&mut self) {
        let peers = self.peer_ids.clone();
        for peer in peers {
            self.replicate_to(peer);
        }
    }

    fn replicate_to(&mut self, peer: NodeId) {
        let next = *self.next_index.get(&peer).unwrap_or(&1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.term_at(prev_log_index);
        let entries: Vec<LogEntry<C>> = if (next as usize) <= self.log.len() {
            self.log[(next as usize - 1)..].to_vec()
        } else {
            Vec::new()
        };
        let entries_len = entries.len() as u64;

        let req = AppendRequest {
            term: self.term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };

        let registry = self.registry.clone();
        let internal_tx = self.internal_tx.clone();
        let term = self.term;
        tokio::spawn(async move {
            if let Some(resp) = registry.send_append(peer, req).await {
                let _ = internal_tx.send(Internal::AppendReply(peer, term, prev_log_index, entries_len, resp));
            }
        });
    }

    fn advance_commit_index(&mut self) {
        let mut indices: Vec<u64> = self
            .peer_ids
            .iter()
            .map(|p| *self.match_index.get(p).unwrap_or(&0))
            .collect();
        indices.push(self.last_log_index()); // eigener Stand
        indices.sort_unstable_by(|a, b| b.cmp(a)); // absteigend

        let majority_index = indices[self.quorum() - 1];

        // Sicherheitsregel: nur Einträge aus dem EIGENEN Term direkt committen.
        if majority_index > self.commit_index && self.term_at(majority_index) == self.term {
            self.commit_index = majority_index;
            self.apply_committed();
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let result = {
                let entry = &self.log[self.last_applied as usize - 1];
                (self.apply_fn)(&entry.command)
            };
            if let Some(tx) = self.pending_proposals.remove(&self.last_applied) {
                let _ = tx.send(Ok(result));
            }
        }
        self.publish_metrics();
    }

    // ---------- Eingehende RPCs ----------

    fn handle_rpc(&mut self, rpc: Rpc<C>) {
        match rpc {
            Rpc::Vote(req, reply) => {
                if req.term > self.term {
                    self.become_follower(req.term);
                }

                let my_last_term = self.term_at(self.last_log_index());
                let up_to_date = req.last_log_term > my_last_term
                    || (req.last_log_term == my_last_term && req.last_log_index >= self.last_log_index());
                let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);
                let grant = req.term >= self.term && can_vote && up_to_date;

                if grant {
                    self.voted_for = Some(req.candidate_id);
                    self.reset_election_timer();
                }
                self.publish_metrics();
                let _ = reply.send(VoteResponse { term: self.term, vote_granted: grant });
            }

            Rpc::Append(req, reply) => {
                if req.term < self.term {
                    let _ = reply.send(AppendResponse {
                        term: self.term,
                        success: false,
                        match_index: 0,
                        conflict_index: self.last_log_index() + 1,
                    });
                    return;
                }

                if req.term > self.term || self.role != Role::Follower {
                    self.become_follower(req.term);
                }
                self.leader_id = Some(req.leader_id);
                self.reset_election_timer();

                if req.prev_log_index > 0 {
                    if req.prev_log_index > self.last_log_index() {
                        let _ = reply.send(AppendResponse {
                            term: self.term,
                            success: false,
                            match_index: 0,
                            conflict_index: self.last_log_index() + 1,
                        });
                        return;
                    }
                    if self.term_at(req.prev_log_index) != req.prev_log_term {
                        let conflict_term = self.term_at(req.prev_log_index);
                        let mut idx = req.prev_log_index;
                        while idx > 1 && self.term_at(idx - 1) == conflict_term {
                            idx -= 1;
                        }
                        let _ = reply.send(AppendResponse {
                            term: self.term,
                            success: false,
                            match_index: 0,
                            conflict_index: idx,
                        });
                        return;
                    }
                }

                let mut index = req.prev_log_index;
                for entry in req.entries {
                    index += 1;
                    if (index as usize) <= self.log.len() {
                        if self.log[index as usize - 1].term != entry.term {
                            self.log.truncate(index as usize - 1);
                            self.log.push(entry);
                        }
                    } else {
                        self.log.push(entry);
                    }
                }

                if req.leader_commit > self.commit_index {
                    self.commit_index = req.leader_commit.min(self.last_log_index());
                    self.apply_committed();
                }

                self.publish_metrics();
                let _ = reply.send(AppendResponse {
                    term: self.term,
                    success: true,
                    match_index: index,
                    conflict_index: 0,
                });
            }
        }
    }

    fn handle_propose(&mut self, command: C, reply: oneshot::Sender<Result<R, NotLeader>>) {
        if self.role != Role::Leader {
            let _ = reply.send(Err(NotLeader { leader_hint: self.leader_id }));
            return;
        }
        self.log.push(LogEntry { term: self.term, command });
        let index = self.last_log_index();
        self.match_index.insert(self.id, index);
        self.pending_proposals.insert(index, reply);
        self.send_heartbeats(); // sofort replizieren statt auf Timer zu warten
    }

    fn handle_internal(&mut self, ev: Internal) {
        match ev {
            Internal::VoteReply(peer, term, resp) => {
                if resp.term > self.term {
                    self.become_follower(resp.term);
                    return;
                }
                if self.role != Role::Candidate || term != self.term {
                    return; // veraltete Antwort
                }
                if resp.vote_granted {
                    self.votes_received.insert(peer);
                    if self.votes_received.len() + 1 >= self.quorum() {
                        self.become_leader();
                    }
                }
            }
            Internal::AppendReply(peer, term, prev_sent, entries_len, resp) => {
                if resp.term > self.term {
                    self.become_follower(resp.term);
                    return;
                }
                if self.role != Role::Leader || term != self.term {
                    return;
                }
                if resp.success {
                    let new_match = prev_sent + entries_len;
                    let m = self.match_index.entry(peer).or_insert(0);
                    if new_match > *m {
                        *m = new_match;
                    }
                    self.next_index.insert(peer, new_match + 1);
                    self.advance_commit_index();
                } else {
                    self.next_index.insert(peer, resp.conflict_index.max(1));
                    self.replicate_to(peer); // sofort erneut versuchen
                }
            }
        }
    }
}

// ===================== Einstiegspunkt =====================

/// Startet einen Knoten als eigenen Task.
/// `rpc_rx` muss VOR dem Aufruf erzeugt und sein `tx`-Gegenstück in der
/// `Registry` registriert worden sein (siehe main.rs).
pub fn spawn_raft_node<C, R, F>(
    id: NodeId,
    rpc_rx: mpsc::UnboundedReceiver<Rpc<C>>,
    peer_ids: Vec<NodeId>,
    registry: Registry<C>,
    apply_fn: F,
    config: RaftConfig,
) -> (RaftHandle<C, R>, tokio::task::JoinHandle<()>)
where
    C: Clone + Send + 'static,
    R: Send + 'static,
    F: FnMut(&C) -> R + Send + 'static,
{
    let (propose_tx, propose_rx) = mpsc::unbounded_channel();
    let (internal_tx, internal_rx) = mpsc::unbounded_channel();
    let (metrics_tx, metrics_rx) = watch::channel(Metrics {
        role: Role::Follower,
        term: 0,
        leader_id: None,
        last_log_index: 0,
        commit_index: 0,
        last_applied: 0,
    });

    let node = Node {
        id,
        role: Role::Follower,
        term: 0,
        voted_for: None,
        leader_id: None,
        log: Vec::new(),
        commit_index: 0,
        last_applied: 0,
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        votes_received: HashSet::new(),
        peer_ids,
        registry,
        rpc_rx,
        propose_rx,
        internal_tx,
        internal_rx,
        metrics_tx,
        apply_fn,
        config,
        election_deadline: Instant::now(),
        pending_proposals: HashMap::new(),
    };

    let join = tokio::spawn(node.run());
    let handle = RaftHandle { id, propose_tx, metrics: metrics_rx };
    (handle, join)
}