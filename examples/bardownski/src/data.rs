use crate::model::Shot;
use std::error::Error;
use std::io::Read;

pub const SAMPLE_CSV: &str = include_str!("../data/sample.csv");

pub fn parse_shots_from_reader<R: Read>(reader: R) -> Result<Vec<Shot>, Box<dyn Error>> {
    let mut csv = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(reader);
    let mut shots = Vec::new();
    for row in csv.deserialize() {
        shots.push(row?);
    }
    Ok(shots)
}

pub fn load_sample_shots() -> Result<Vec<Shot>, Box<dyn Error>> {
    parse_shots_from_reader(SAMPLE_CSV.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::parse_shots_from_reader;

    #[test]
    fn parse_shots_from_reader_should_parse_money_puck_style_rows() {
        let csv = "\
id,period,x,y,xg,shot_type,on_rebound,team,goal
1,2,12,-4,0.21,Wrist Shot,true,CGY,false
";

        let shots = parse_shots_from_reader(csv.as_bytes()).expect("csv should parse");

        assert_eq!(shots.len(), 1);
    }
}
