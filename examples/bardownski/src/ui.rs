use crate::app::Showcase;
use crate::heatmap::Heatmap;
use crate::model::Shot;
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use std::error::Error;
use std::io::{self, Stdout};
use std::time::Duration;

pub fn run(mut app: Showcase) -> Result<(), Box<dyn Error>> {
    let mut terminal = TerminalGuard::enter()?;

    loop {
        terminal.terminal.draw(|frame| {
            let area = frame.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(12),
                    Constraint::Length(8),
                ])
                .split(area);

            let grid_area = chunks[1];
            let grid_width = grid_area.width.saturating_sub(2).max(1) as usize;
            let grid_height = grid_area.height.saturating_sub(2).max(1) as usize;
            let view = app.filtered_view(grid_width, grid_height);
            let stats = app.trait_stats();

            render_header(frame, chunks[0], &app, view.shots.len());
            render_heatmap(frame, grid_area, &view.heatmap);
            render_bottom(frame, chunks[2], &view.shots, &stats);
        })?;

        if event::poll(Duration::from_millis(200))? {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char('h') => app.toggle_high_danger(),
                KeyCode::Char('r') => app.toggle_rebound(),
                KeyCode::Char('p') => app.cycle_period(),
                _ => {}
            }
        }
    }

    Ok(())
}

fn render_header(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    app: &Showcase,
    filtered_count: usize,
) {
    let period = app
        .filters
        .period
        .map(|period| period.to_string())
        .unwrap_or_else(|| "all".to_owned());
    let status = vec![
        Span::styled(
            "Bardownski",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "  {} / {} shots  period:{}  high-danger:{}  rebound:{}",
            filtered_count,
            app.source_count,
            period,
            flag(app.filters.high_danger),
            flag(app.filters.on_rebound)
        )),
        Span::styled(
            "   p/h/r toggle  q quit",
            Style::default().fg(Color::DarkGray),
        ),
    ];
    frame.render_widget(
        Paragraph::new(Line::from(status)).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_heatmap(frame: &mut ratatui::Frame<'_>, area: Rect, heatmap: &Heatmap) {
    let max_density = heatmap.max_density().max(1);
    let lines = (0..heatmap.height)
        .map(|row| {
            let spans = (0..heatmap.width)
                .map(|col| {
                    let density = heatmap.density_at(col, row);
                    Span::styled(
                        "█",
                        Style::default().fg(density_color(density, max_density)),
                    )
                })
                .collect::<Vec<_>>();
            Line::from(spans)
        })
        .collect::<Vec<_>>();

    frame.render_widget(
        Paragraph::new(lines).block(Block::default().title("Rink heatmap").borders(Borders::ALL)),
        area,
    );
}

fn render_bottom(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    shots: &[std::sync::Arc<Shot>],
    stats: &crate::app::TraitStats,
) {
    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    let rows = shots
        .iter()
        .take(4)
        .map(|shot| {
            Row::new([
                Cell::from(shot.id.to_string()),
                Cell::from(format!("P{}", shot.period)),
                Cell::from(shot.team.clone()),
                Cell::from(shot.shot_type.clone()),
                Cell::from(format!("{:.2}", shot.xg)),
                Cell::from(if shot.goal { "G" } else { "" }),
            ])
        })
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(4),
            Constraint::Length(5),
            Constraint::Min(10),
            Constraint::Length(5),
            Constraint::Length(3),
        ],
    )
    .header(Row::new(["id", "per", "team", "type", "xG", ""]))
    .block(
        Block::default()
            .title("Filtered shots")
            .borders(Borders::ALL),
    );
    frame.render_widget(table, bottom[0]);

    let stats_text = vec![
        Line::from(format!(
            "High danger: {}{}",
            stats.high_danger_count,
            sample(stats.high_danger_sample)
        )),
        Line::from(format!(
            "Rebounds:    {}{}",
            stats.rebound_count,
            sample(stats.rebound_sample)
        )),
        Line::from(format!(
            "One-timers:  {}{}",
            stats.one_timer_count,
            sample(stats.one_timer_sample)
        )),
        Line::from(format!(
            "Registry:    {} rows across {}",
            stats.showcase_row_count,
            stats.showcase_row_kinds.join("+")
        )),
        Line::from(format!(
            "Sample:      {}",
            stats.showcase_row_sample.as_deref().unwrap_or("none")
        )),
    ];
    frame.render_widget(
        Paragraph::new(stats_text).block(
            Block::default()
                .title("Sassi trait registry")
                .borders(Borders::ALL),
        ),
        bottom[1],
    );
}

fn flag(enabled: bool) -> &'static str {
    if enabled { "on" } else { "off" }
}

fn sample(id: Option<u64>) -> String {
    id.map(|id| format!("  sample #{id}")).unwrap_or_default()
}

fn density_color(density: u16, max_density: u16) -> Color {
    if density == 0 {
        return Color::DarkGray;
    }

    match density.saturating_mul(4) / max_density.max(1) {
        0 | 1 => Color::Blue,
        2 => Color::Green,
        3 => Color::Yellow,
        _ => Color::Red,
    }
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn enter() -> Result<Self, Box<dyn Error>> {
        enable_raw_mode()?;
        match enter_terminal_after_raw_mode() {
            Ok(terminal) => Ok(Self { terminal }),
            Err(err) => {
                let _ = disable_raw_mode();
                Err(err)
            }
        }
    }
}

fn enter_terminal_after_raw_mode() -> Result<Terminal<CrosstermBackend<Stdout>>, Box<dyn Error>> {
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    match Terminal::new(backend) {
        Ok(terminal) => Ok(terminal),
        Err(err) => {
            let mut stdout = io::stdout();
            let _ = execute!(stdout, LeaveAlternateScreen);
            Err(Box::new(err))
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}
