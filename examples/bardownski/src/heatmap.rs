use crate::model::Shot;

pub const RINK_X_MIN: i32 = -100;
pub const RINK_X_MAX: i32 = 100;
pub const RINK_Y_MIN: i32 = -42;
pub const RINK_Y_MAX: i32 = 42;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Heatmap {
    pub width: usize,
    pub height: usize,
    pub cells: Vec<u16>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Bin {
    pub col: usize,
    pub row: usize,
}

impl Heatmap {
    pub fn max_density(&self) -> u16 {
        self.cells.iter().copied().max().unwrap_or(0)
    }

    pub fn density_at(&self, col: usize, row: usize) -> u16 {
        self.cells
            .get(row.saturating_mul(self.width).saturating_add(col))
            .copied()
            .unwrap_or(0)
    }
}

pub fn bin_for_shot(shot: &Shot, width: usize, height: usize) -> Option<Bin> {
    if width == 0
        || height == 0
        || !(RINK_X_MIN..=RINK_X_MAX).contains(&shot.x)
        || !(RINK_Y_MIN..=RINK_Y_MAX).contains(&shot.y)
    {
        return None;
    }

    let x_span = (RINK_X_MAX - RINK_X_MIN) as f32;
    let y_span = (RINK_Y_MAX - RINK_Y_MIN) as f32;
    let col = ((shot.x - RINK_X_MIN) as f32 / x_span * (width.saturating_sub(1)) as f32).round();
    let row = ((RINK_Y_MAX - shot.y) as f32 / y_span * (height.saturating_sub(1)) as f32).round();

    Some(Bin {
        col: col.clamp(0.0, width.saturating_sub(1) as f32) as usize,
        row: row.clamp(0.0, height.saturating_sub(1) as f32) as usize,
    })
}

pub fn build_heatmap<'a>(
    shots: impl IntoIterator<Item = &'a Shot>,
    width: usize,
    height: usize,
) -> Heatmap {
    let mut cells = vec![0_u16; width.saturating_mul(height)];
    for shot in shots {
        let Some(bin) = bin_for_shot(shot, width, height) else {
            continue;
        };
        let idx = bin.row * width + bin.col;
        cells[idx] = cells[idx].saturating_add(1);
    }

    Heatmap {
        width,
        height,
        cells,
    }
}

#[cfg(test)]
mod tests {
    use super::{Bin, bin_for_shot, build_heatmap};
    use crate::model::Shot;

    fn shot(x: i32, y: i32) -> Shot {
        Shot {
            id: 1,
            period: 1,
            x,
            y,
            xg: 0.10,
            shot_type: "Wrist Shot".to_owned(),
            on_rebound: false,
            team: "CGY".to_owned(),
            goal: false,
        }
    }

    #[test]
    fn bin_for_shot_should_place_center_ice_in_middle_cell() {
        assert_eq!(
            bin_for_shot(&shot(0, 0), 11, 7),
            Some(Bin { col: 5, row: 3 })
        );
    }

    #[test]
    fn build_heatmap_should_count_multiple_shots_in_same_bin() {
        let shots = vec![shot(0, 0), shot(1, 1)];

        let heatmap = build_heatmap(&shots, 11, 7);

        assert_eq!(heatmap.cells[3 * 11 + 5], 2);
    }
}
