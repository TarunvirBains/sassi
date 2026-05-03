mod app;
mod data;
mod filters;
mod heatmap;
mod model;
mod ui;

use clap::Parser;
use filters::FilterOptions;
use std::error::Error;

#[derive(Debug, Parser)]
#[command(name = "bardownski")]
#[command(about = "Dependency-light sassi TUI over offline hockey shot data")]
struct Cli {
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=3))]
    period: Option<u8>,
    #[arg(long)]
    high_danger: bool,
    #[arg(long)]
    on_rebound: bool,
    #[arg(long)]
    summary: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let filters = FilterOptions {
        period: cli.period,
        high_danger: cli.high_danger,
        on_rebound: cli.on_rebound,
    };
    let app = app::Showcase::from_shots(data::load_sample_shots()?, filters)?;

    if cli.summary {
        println!("{}", app.summary_line());
        return Ok(());
    }

    ui::run(app)
}
