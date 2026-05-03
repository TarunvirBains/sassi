use crate::filters::{FilterOptions, build_predicate};
use crate::heatmap::{Heatmap, build_heatmap};
use crate::model::{IsHighDanger, IsOneTimer, IsRebound, Shot, ShowcaseRow, TeamSummary};
use sassi::{MemQ, Punnu, Sassi};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub struct Showcase {
    pub pool: Arc<Punnu<Shot>>,
    pub team_summaries: Arc<Punnu<TeamSummary>>,
    pub sassi: Sassi,
    pub filters: FilterOptions,
    pub source_count: usize,
}

#[derive(Debug, Clone)]
pub struct FilteredView {
    pub shots: Vec<Arc<Shot>>,
    pub heatmap: Heatmap,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TraitStats {
    pub high_danger_count: usize,
    pub high_danger_sample: Option<u64>,
    pub rebound_count: usize,
    pub rebound_sample: Option<u64>,
    pub one_timer_count: usize,
    pub one_timer_sample: Option<u64>,
    pub showcase_row_count: usize,
    pub showcase_row_kinds: Vec<&'static str>,
    pub showcase_row_sample: Option<String>,
}

impl Showcase {
    pub fn from_shots(
        shots: impl IntoIterator<Item = Shot>,
        filters: FilterOptions,
    ) -> Result<Self, Box<dyn Error>> {
        let pool = Arc::new(Punnu::<Shot>::builder().build());
        let team_summaries = Arc::new(Punnu::<TeamSummary>::builder().build());
        let shots = shots.into_iter().collect::<Vec<_>>();
        let source_count = shots.len();
        let summaries = team_summaries_from_shots(&shots);

        for shot in shots {
            block_on_ready(pool.insert(shot))?;
        }
        for summary in summaries {
            block_on_ready(team_summaries.insert(summary))?;
        }

        let mut sassi = Sassi::new();
        sassi.register::<Shot>(pool.clone());
        sassi.register::<TeamSummary>(team_summaries.clone());

        Ok(Self {
            pool,
            team_summaries,
            sassi,
            filters,
            source_count,
        })
    }

    pub fn filtered_view(&self, width: usize, height: usize) -> FilteredView {
        let predicate = build_predicate(self.filters);
        let shots = self
            .pool
            .scope(vec![MemQ::filter_basic(predicate)])
            .collect();
        let heatmap = build_heatmap(shots.iter().map(Arc::as_ref), width, height);

        FilteredView { shots, heatmap }
    }

    pub fn trait_stats(&self) -> TraitStats {
        let high_danger = self.sassi.all_impl::<dyn IsHighDanger>();
        let rebounds = self.sassi.all_impl::<dyn IsRebound>();
        let one_timers = self.sassi.all_impl::<dyn IsOneTimer>();
        let showcase_rows = self.sassi.all_impl::<dyn ShowcaseRow>();
        let showcase_row_kinds = showcase_rows
            .iter()
            .map(|row| row.row_kind())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let showcase_row_sample = showcase_rows
            .iter()
            .find(|row| row.row_kind() == "team")
            .map(|row| row.row_label())
            .or_else(|| showcase_rows.first().map(|row| row.row_label()));

        TraitStats {
            high_danger_count: high_danger
                .iter()
                .filter(|shot| shot.is_high_danger())
                .count(),
            high_danger_sample: high_danger
                .iter()
                .find(|shot| shot.is_high_danger())
                .map(|shot| shot.shot_id()),
            rebound_count: rebounds.iter().filter(|shot| shot.is_rebound()).count(),
            rebound_sample: rebounds
                .iter()
                .find(|shot| shot.is_rebound())
                .map(|shot| shot.shot_id()),
            one_timer_count: one_timers.iter().filter(|shot| shot.is_one_timer()).count(),
            one_timer_sample: one_timers
                .iter()
                .find(|shot| shot.is_one_timer())
                .map(|shot| shot.shot_id()),
            showcase_row_count: showcase_rows.len(),
            showcase_row_kinds,
            showcase_row_sample,
        }
    }

    pub fn toggle_high_danger(&mut self) {
        self.filters.high_danger = !self.filters.high_danger;
    }

    pub fn toggle_rebound(&mut self) {
        self.filters.on_rebound = !self.filters.on_rebound;
    }

    pub fn cycle_period(&mut self) {
        self.filters.period = match self.filters.period {
            None => Some(1),
            Some(1) => Some(2),
            Some(2) => Some(3),
            Some(_) => None,
        };
    }

    pub fn summary_line(&self) -> String {
        let view = self.filtered_view(40, 17);
        let stats = self.trait_stats();
        format!(
            "{} of {} shots | {} teams | high-danger {} | rebounds {} | one-timers {} | registry rows {}",
            view.shots.len(),
            self.source_count,
            self.team_summaries.len(),
            stats.high_danger_count,
            stats.rebound_count,
            stats.one_timer_count,
            stats.showcase_row_count
        )
    }
}

fn team_summaries_from_shots(shots: &[Shot]) -> Vec<TeamSummary> {
    let mut teams = BTreeMap::<String, TeamSummary>::new();
    for shot in shots {
        let entry = teams
            .entry(shot.team.clone())
            .or_insert_with(|| TeamSummary {
                id: shot.team.clone(),
                team: shot.team.clone(),
                shots: 0,
                goals: 0,
                total_xg_milli: 0,
            });
        entry.shots += 1;
        entry.goals += u32::from(shot.goal);
        entry.total_xg_milli += (shot.xg * 1_000.0).round() as u32;
    }
    teams.into_values().collect()
}

fn block_on_ready<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);

    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => {
            panic!("bardownski uses an L1-only Punnu path that should resolve without a runtime")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Showcase;
    use crate::filters::FilterOptions;
    use crate::model::Shot;

    fn shot(id: u64, xg: f32, shot_type: &str, on_rebound: bool) -> Shot {
        Shot {
            id,
            period: 1,
            x: 0,
            y: 0,
            xg,
            shot_type: shot_type.to_owned(),
            on_rebound,
            team: "CGY".to_owned(),
            goal: false,
        }
    }

    #[test]
    fn trait_stats_should_count_registered_shots_by_trait_semantics() {
        let app = Showcase::from_shots(
            vec![
                shot(1, 0.20, "One-Timer", true),
                shot(2, 0.05, "Wrist Shot", false),
            ],
            FilterOptions::default(),
        )
        .expect("showcase should load");

        let stats = app.trait_stats();

        assert_eq!(stats.high_danger_count, 1);
    }

    #[test]
    fn trait_stats_should_query_showcase_rows_across_shots_and_team_summaries() {
        let app = Showcase::from_shots(
            vec![
                shot(1, 0.20, "One-Timer", true),
                shot(2, 0.05, "Wrist Shot", false),
            ],
            FilterOptions::default(),
        )
        .expect("showcase should load");

        let stats = app.trait_stats();

        assert_eq!(app.team_summaries.len(), 1);
        assert_eq!(stats.showcase_row_count, 3);
        assert_eq!(stats.showcase_row_kinds, vec!["shot", "team"]);
        assert!(stats.showcase_row_sample.unwrap().contains("CGY"));
    }
}
