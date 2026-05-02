use crate::cacheable::Cacheable;
use crate::punnu::events::PunnuEvent;
use crate::punnu::state::L1State;

pub(crate) struct PreparedWrite<T: Cacheable, R> {
    pub(crate) state: L1State<T>,
    pub(crate) events: Vec<PunnuEvent<T>>,
    pub(crate) result: R,
}

impl<T: Cacheable, R> PreparedWrite<T, R> {
    pub(crate) fn new(state: L1State<T>, events: Vec<PunnuEvent<T>>, result: R) -> Self {
        Self {
            state,
            events,
            result,
        }
    }
}
