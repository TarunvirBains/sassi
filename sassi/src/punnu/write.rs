use crate::cacheable::Cacheable;
use crate::punnu::events::PunnuEvent;
use crate::punnu::state::L1State;

pub(crate) struct PreparedWrite<T: Cacheable, R> {
    state: L1State<T>,
    events: Vec<PunnuEvent<T>>,
    result: R,
}

impl<T: Cacheable, R> PreparedWrite<T, R> {
    pub(crate) fn new(state: L1State<T>, events: Vec<PunnuEvent<T>>, result: R) -> Self {
        Self {
            state,
            events,
            result,
        }
    }

    pub(crate) fn state(&self) -> &L1State<T> {
        &self.state
    }

    pub(crate) fn into_parts(self) -> (L1State<T>, Vec<PunnuEvent<T>>, R) {
        (self.state, self.events, self.result)
    }
}
