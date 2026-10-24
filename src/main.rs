    // src/main.rs
    mod new_raft;

    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use tokio::sync::mpsc;

    use new_raft::{spawn_raft_node, RaftConfig, RaftHandle, Registry, Role, Rpc, RpcCounts};

    #[derive(Clone, Debug)]
    struct Command {
        key: String,
        value: String,
    }

    #[derive(Debug)]
    struct CommandResult {
        previous: Option<String>,
    }

    #[tokio::main]
    async fn main() -> anyhow::Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
            )
            .init();

        let ids: Vec<u64> = vec![1, 2, 3];
        let registry: Registry<Command> = Registry::new();

        // 1. Inbox-Channels erzeugen und VOR dem Start registrieren
        let mut rx_map = HashMap::new();
        for &id in &ids {
            let (tx, rx) = mpsc::unbounded_channel::<Rpc<Command>>();
            registry.register(id, tx).await;
            rx_map.insert(id, rx);
        }

        // 2. Knoten starten
        let mut handles: BTreeMap<u64, RaftHandle<Command, CommandResult>> = BTreeMap::new();
        let mut joins: HashMap<u64, tokio::task::JoinHandle<()>> = HashMap::new();

        for &id in &ids {
            let rx = rx_map.remove(&id).unwrap();
            let peer_ids: Vec<u64> = ids.iter().cloned().filter(|&p| p != id).collect();

            // Jeder Knoten hat seinen eigenen lokalen Key-Value-Store.
            let mut kv: HashMap<String, String> = HashMap::new();
            let apply_fn = move |cmd: &Command| -> CommandResult {
                let previous = kv.insert(cmd.key.clone(), cmd.value.clone());
                CommandResult { previous }
            };

            let (handle, join) =
                spawn_raft_node(id, rx, peer_ids, registry.clone(), apply_fn, RaftConfig::default());
            handles.insert(id, handle);
            joins.insert(id, join);
        }

        // 3. Auf Leader-Wahl warten
        let leader = wait_for_leader(&handles, None).await;
        println!("Leader gewählt: Knoten {leader}");

        // 4. Schreiben
        let resp = handles[&leader]
            .propose(Command { key: "foo".into(), value: "bar".into() })
            .await;
        println!("Write committed: {:?}", resp);

        tokio::time::sleep(Duration::from_millis(300)).await;
        print_status(&handles);

        // 5. Leader-Absturz simulieren: Task hart abbrechen + aus der Registry entfernen
        println!("\n>>> Knoten {leader} stürzt ab");
        if let Some(join) = joins.get(&leader) {
            join.abort();
        }
        registry.disconnect(leader).await;

        // 6. Neuwahl abwarten
        let new_leader = wait_for_leader(&handles, Some(leader)).await;
        println!("Neuer Leader: Knoten {new_leader}");

        let resp = handles[&new_leader]
            .propose(Command { key: "foo".into(), value: "baz".into() })
            .await;
        println!("Write nach Failover committed: {:?}", resp);

        tokio::time::sleep(Duration::from_millis(300)).await;
        print_status(&handles);

        measure_election_latency(20, 0).await;    // Baseline, kein Paketverlust
        let c = registry.counts();
        println!("Votes gesendet: {}, Appends gesendet: {}, verworfen: {}", c.votes_sent, c.appends_sent, c.dropped);
        measure_election_latency(20, 300).await;  // 30% Paketverlust
        let c = registry.counts();
        println!("Votes gesendet: {}, Appends gesendet: {}, verworfen: {}", c.votes_sent, c.appends_sent, c.dropped);

        Ok(())
    }

    /// Wartet, bis irgendein Knoten (außer optional `exclude`) Leader ist.
    async fn wait_for_leader(
        handles: &BTreeMap<u64, RaftHandle<Command, CommandResult>>,
        exclude: Option<u64>,
    ) -> u64 {
        loop {
            for (&id, h) in handles {
                if Some(id) == exclude {
                    continue;
                }
                if h.current_metrics().role == Role::Leader {
                    return id;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn print_status(handles: &BTreeMap<u64, RaftHandle<Command, CommandResult>>) {
        println!("--- Status ---");
        for (id, h) in handles {
            let m = h.current_metrics();
            println!(
                "Knoten {id}: role={:?} term={} leader={:?} log={} commit={} applied={}",
                m.role, m.term, m.leader_id, m.last_log_index, m.commit_index, m.last_applied
            );
        }
    }

    async fn measure_election_latency(rounds: usize, drop_permille: u32) {
        let mut durations = Vec::with_capacity(rounds);
        let mut total = RpcCounts::default();

        for _ in 0..rounds {
            let ids: Vec<u64> = vec![1, 2, 3];
            let registry: Registry<Command> = Registry::new();
            registry.set_fault(drop_permille, Duration::ZERO).await;

            let mut handles = BTreeMap::new();
            let mut joins = Vec::new();

            let start = std::time::Instant::now();
            for &id in &ids {
                let (tx, rx) = mpsc::unbounded_channel::<Rpc<Command>>();
                registry.register(id, tx).await;
                let peer_ids: Vec<u64> = ids.iter().cloned().filter(|&p| p != id).collect();
                let apply_fn = |_: &Command| CommandResult { previous: None };
                let (handle, join) =
                    spawn_raft_node(id, rx, peer_ids, registry.clone(), apply_fn, RaftConfig::default());
                handles.insert(id, handle);
                joins.push(join);
            }

            wait_for_leader(&handles, None).await;
            durations.push(start.elapsed());

            for j in joins {
                j.abort();
            }

            let c = registry.counts();
            total.votes_sent += c.votes_sent;
            total.appends_sent += c.appends_sent;
            total.dropped += c.dropped;
        }

        durations.sort();
        println!(
            "Election-Latenz über {rounds} Durchläufe (drop={drop_permille}‰): min={:?} median={:?} max={:?}",
            durations[0],
            durations[rounds / 2],
            durations[rounds - 1]
        );

        println!(
            "  → Votes: {}, Appends: {}, verworfen: {}",
            total.votes_sent, total.appends_sent, total.dropped
        );
    }