use sassi::Cacheable;
use serde::{Deserialize, Serialize};

#[derive(Cacheable, Serialize, Deserialize, Debug, Clone)]
pub struct Shot {
    pub id: u64,
    pub period: u8,
    pub x: i32,
    pub y: i32,
    pub xg: f32,
    pub shot_type: String,
    pub on_rebound: bool,
    pub team: String,
    pub goal: bool,
}

#[derive(Cacheable, Serialize, Deserialize, Debug, Clone)]
pub struct TeamSummary {
    pub id: String,
    pub team: String,
    pub shots: u32,
    pub goals: u32,
    pub total_xg_milli: u32,
}

pub trait IsHighDanger: Send + Sync {
    fn shot_id(&self) -> u64;
    fn is_high_danger(&self) -> bool;
}

pub trait IsRebound: Send + Sync {
    fn shot_id(&self) -> u64;
    fn is_rebound(&self) -> bool;
}

pub trait IsOneTimer: Send + Sync {
    fn shot_id(&self) -> u64;
    fn is_one_timer(&self) -> bool;
}

pub trait ShowcaseRow: Send + Sync {
    fn row_kind(&self) -> &'static str;
    fn row_label(&self) -> String;
}

pub fn is_high_danger_shot(shot: &Shot) -> bool {
    (-30..=30).contains(&shot.x) && (-15..=15).contains(&shot.y) && shot.xg >= 0.15
}

pub fn is_one_timer_shot_type(shot_type: &str) -> bool {
    if shot_type.eq_ignore_ascii_case("one-timer") || shot_type.eq_ignore_ascii_case("one timer") {
        return true;
    }

    let normalized = shot_type.to_ascii_lowercase();
    let mut previous_was_one = false;
    for word in normalized
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
    {
        if previous_was_one && word == "timer" {
            return true;
        }
        previous_was_one = word == "one";
    }

    false
}

impl TeamSummary {
    pub fn average_xg(&self) -> f32 {
        if self.shots == 0 {
            return 0.0;
        }
        self.total_xg_milli as f32 / self.shots as f32 / 1_000.0
    }
}

#[sassi::trait_impl]
impl IsHighDanger for Shot {
    fn shot_id(&self) -> u64 {
        self.id
    }

    fn is_high_danger(&self) -> bool {
        is_high_danger_shot(self)
    }
}

#[sassi::trait_impl]
impl ShowcaseRow for Shot {
    fn row_kind(&self) -> &'static str {
        "shot"
    }

    fn row_label(&self) -> String {
        format!("{} #{}", self.team, self.id)
    }
}

#[sassi::trait_impl]
impl ShowcaseRow for TeamSummary {
    fn row_kind(&self) -> &'static str {
        "team"
    }

    fn row_label(&self) -> String {
        format!(
            "{}: {} shots, {} goals, {:.2} avg xG",
            self.team,
            self.shots,
            self.goals,
            self.average_xg()
        )
    }
}

#[sassi::trait_impl]
impl IsRebound for Shot {
    fn shot_id(&self) -> u64 {
        self.id
    }

    fn is_rebound(&self) -> bool {
        self.on_rebound
    }
}

#[sassi::trait_impl]
impl IsOneTimer for Shot {
    fn shot_id(&self) -> u64 {
        self.id
    }

    fn is_one_timer(&self) -> bool {
        is_one_timer_shot_type(&self.shot_type)
    }
}
