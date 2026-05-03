use crate::model::Shot;
use sassi::{BasicPredicate, Cacheable};

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct FilterOptions {
    pub period: Option<u8>,
    pub high_danger: bool,
    pub on_rebound: bool,
}

pub fn build_predicate(filters: FilterOptions) -> BasicPredicate<Shot> {
    let f = Shot::fields();
    let mut predicates = Vec::new();

    if let Some(period) = filters.period {
        predicates.push(f.period.eq(period));
    }

    if filters.high_danger {
        predicates.push(high_danger_predicate());
    }

    if filters.on_rebound {
        predicates.push(f.on_rebound.eq(true));
    }

    match predicates.len() {
        0 => BasicPredicate::True,
        1 => predicates
            .pop()
            .expect("one predicate exists when len returned 1"),
        _ => BasicPredicate::And(predicates),
    }
}

pub fn high_danger_predicate() -> BasicPredicate<Shot> {
    let f = Shot::fields();
    f.x.between(-30, 30) & f.y.between(-15, 15) & f.xg.gte(0.15)
}

#[cfg(test)]
mod tests {
    use super::{FilterOptions, build_predicate};
    use crate::model::Shot;

    fn shot(period: u8, x: i32, y: i32, xg: f32, on_rebound: bool) -> Shot {
        Shot {
            id: 1,
            period,
            x,
            y,
            xg,
            shot_type: "Wrist Shot".to_owned(),
            on_rebound,
            team: "CGY".to_owned(),
            goal: false,
        }
    }

    #[test]
    fn build_predicate_should_match_high_danger_definition() {
        let predicate = build_predicate(FilterOptions {
            high_danger: true,
            ..Default::default()
        });

        assert!(!predicate.evaluate(&shot(1, 45, 0, 0.30, false)));
    }

    #[test]
    fn build_predicate_should_combine_period_and_rebound_filters() {
        let predicate = build_predicate(FilterOptions {
            period: Some(2),
            on_rebound: true,
            ..Default::default()
        });

        assert!(!predicate.evaluate(&shot(1, 0, 0, 0.20, true)));
    }
}
