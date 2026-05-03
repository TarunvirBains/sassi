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
