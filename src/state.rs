use std::sync::Mutex;

#[derive(Debug)]
pub struct State {
    /// Last term server has seen.
    ///
    /// initialized to 0, increases monotonically.
    ///
    /// Needed to updated on stable storage before responding RPCs.
    current_term: Mutex<u64>,

    /// 'candidateId' that received vote in current term.
    /// None if none.
    ///
    /// Needed to updated on stable storage before responding RPCs.
    voted_for: Mutex<Option<u64>>,
}
