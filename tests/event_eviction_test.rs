use acp_gateway::manager::{ActiveSession, SessionManager, MAX_EVENTS_PER_SESSION, BROADCAST_CAPACITY};
use acp_gateway::types::{SessionMeta, SessionStatus, StoredEvent};
use chrono::Utc;
use tempfile::TempDir;
use tokio::sync::broadcast;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_meta(id: &str) -> SessionMeta {
    SessionMeta {
        session_id: id.to_string(),
        agent: "test".to_string(),
        cwd: "/tmp".to_string(),
        mcp_servers: vec![],
        status: SessionStatus::Active,
        created_at: Utc::now(),
    }
}

fn make_active_session(id: &str) -> ActiveSession {
    let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
    ActiveSession::new(make_meta(id), tx, None)
}

async fn make_manager() -> (SessionManager, TempDir) {
    let dir = TempDir::new().unwrap();
    let manager = SessionManager::new(dir.path().join("sessions.jsonl"))
        .await
        .unwrap();
    (manager, dir)
}

/// Push `count` events directly into an `ActiveSession`'s `EventLog`,
/// replicating the same eviction logic used by `SessionManager::push_event`.
async fn push_direct(session: &ActiveSession, count: usize) {
    for i in 0..count {
        let mut guard = session.events.write().await;
        let index = guard.next_index;
        guard.next_index += 1;
        let event = StoredEvent {
            index,
            event_type: "test".to_string(),
            data: serde_json::json!(i),
        };
        guard.events.push_back(event);
        if guard.events.len() > MAX_EVENTS_PER_SESSION {
            guard.events.pop_front();
        }
    }
}

// ── test 1: monotonic indices after eviction ─────────────────────────────────

/// Push MAX_EVENTS_PER_SESSION + 50 events directly into an in-memory
/// ActiveSession and verify the ring-buffer invariants:
///   - next_index == total pushed
///   - buffer length is capped at MAX_EVENTS_PER_SESSION
///   - all indices are strictly increasing (no reset after eviction)
///   - first retained index is 50 (the first 50 were evicted)
#[tokio::test]
async fn test_monotonic_indices_after_eviction() {
    let total = MAX_EVENTS_PER_SESSION + 50;
    let session = make_active_session("eviction-monotonic");
    push_direct(&session, total).await;

    let guard = session.events.read().await;

    assert_eq!(
        guard.next_index, total,
        "next_index should equal total events pushed"
    );
    assert_eq!(
        guard.events.len(),
        MAX_EVENTS_PER_SESSION,
        "buffer should be capped at MAX_EVENTS_PER_SESSION"
    );

    let indices: Vec<usize> = guard.events.iter().map(|e| e.index).collect();

    // Strictly increasing — no reset after eviction
    for window in indices.windows(2) {
        assert_eq!(
            window[1],
            window[0] + 1,
            "non-monotonic indices: {} followed by {}",
            window[0],
            window[1]
        );
    }

    // First 50 events were evicted; oldest retained index == 50
    assert_eq!(
        indices[0], 50,
        "first retained index should be 50 after evicting the first 50 events"
    );
    assert_eq!(
        *indices.last().unwrap(),
        total - 1,
        "last retained index should be total-1"
    );
}

// ── test 2: get_events pagination after eviction ─────────────────────────────

/// After pushing MAX_EVENTS_PER_SESSION + 100 events through a real
/// SessionManager, verify that get_events correctly handles three
/// pagination scenarios:
///
///   a) from = MAX_EVENTS_PER_SESSION - 1  (within the live window; boundary)
///   b) from = MAX_EVENTS_PER_SESSION + 50 (well into the post-eviction range)
///   c) from = 0                            (before the oldest retained index)
#[tokio::test]
async fn test_get_events_pagination_after_eviction() {
    let (manager, _dir) = make_manager().await;
    let total = MAX_EVENTS_PER_SESSION + 100;

    let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
    manager.insert(
        "paginate".to_string(),
        ActiveSession::new(make_meta("paginate"), tx, None),
    );

    for i in 0..total {
        manager
            .push_event("paginate", "test".to_string(), serde_json::json!(i))
            .await;
    }

    // After pushing MAX+100 events the ring buffer holds indices [100 .. MAX+99].

    // ── scenario a: from is at the boundary between evicted and live ──────
    // from = MAX - 1 = 9_999, which is within the live window [100..10_099].
    let from_a = MAX_EVENTS_PER_SESSION - 1;
    let events_a = manager.get_events("paginate", from_a, 5).await;
    assert!(
        !events_a.is_empty(),
        "scenario a: expected events with index >= {from_a}"
    );
    for e in &events_a {
        assert!(
            e.index >= from_a,
            "scenario a: event index {} < from {}",
            e.index,
            from_a
        );
    }
    assert!(events_a.len() <= 5);

    // ── scenario b: from is after the eviction start ─────────────────────
    let from_b = MAX_EVENTS_PER_SESSION + 50;
    let events_b = manager.get_events("paginate", from_b, 5).await;
    assert!(
        !events_b.is_empty(),
        "scenario b: expected events with index >= {from_b}"
    );
    for e in &events_b {
        assert!(
            e.index >= from_b,
            "scenario b: event index {} < from {}",
            e.index,
            from_b
        );
    }
    assert!(events_b.len() <= 5);

    // ── scenario c: from = 0 (entirely in the evicted range) ─────────────
    // The oldest retained index is 100; get_events should return events
    // starting from index 100, NOT from index 0.
    let events_c = manager.get_events("paginate", 0, 5).await;
    assert!(
        !events_c.is_empty(),
        "scenario c: expected events even though from=0 is fully evicted"
    );
    let first_index = events_c[0].index;
    assert_eq!(
        first_index, 100,
        "scenario c: oldest retained index should be 100, got {first_index}"
    );
}

// ── test 3: session resume preserves event counter ───────────────────────────

/// Simulates a session resume: create a session, push 50 events, then
/// create a new ActiveSession that is pre-loaded with those 50 events
/// (as if rehydrated from disk).  Push 5 more events and assert the new
/// indices continue from 50..=54, not from 0..=4.
#[tokio::test]
async fn test_session_resume_preserves_event_counter() {
    // Phase 1: original session — push 50 events
    let original = make_active_session("resume-original");
    push_direct(&original, 50).await;

    let (prior_events, prior_next_index) = {
        let guard = original.events.read().await;
        assert_eq!(guard.next_index, 50);
        (guard.events.clone(), guard.next_index)
    };

    // Phase 2: new session initialised with the prior event log (simulate resume)
    let resumed = make_active_session("resume-new");
    {
        let mut guard = resumed.events.write().await;
        guard.events = prior_events;
        guard.next_index = prior_next_index; // 50 — next push gets index 50
    }

    // Push 5 more events via the same direct helper
    push_direct(&resumed, 5).await;

    // Verify resumed event indices are 50, 51, 52, 53, 54 (not 0..4)
    let guard = resumed.events.read().await;
    assert_eq!(
        guard.next_index, 55,
        "next_index should be 55 after 50+5 events"
    );

    // The last 5 events in the buffer are the resumed ones
    let last_five: Vec<&StoredEvent> = {
        let all: Vec<&StoredEvent> = guard.events.iter().collect();
        all[all.len() - 5..].to_vec()
    };

    for (i, event) in last_five.iter().enumerate() {
        let expected_index = 50 + i;
        assert_eq!(
            event.index, expected_index,
            "resumed event {} has index {} but expected {}",
            i, event.index, expected_index
        );
    }
}
