use crate::dogstatsdreader::DogStatsDReader;

pub enum Kind {
    Count,
    Distribution,
    Gauge,
    Timer,
    Histogram,
    Set,
    ServiceCheck,
    Event,
}

pub struct DogStatsDMessageStats {
    pub name_length: u16,
    pub num_values: u16,
    pub num_tags: u16,
    pub num_ascii_tags: u16,
    pub num_unicode_tags: u16,
    pub kind: Option<Kind>,
}

pub fn analyze_msgs(
    mut reader: Box<dyn DogStatsDReader>,
) -> Result<Vec<DogStatsDMessageStats>, std::io::Error> {
    let mut msg_stats: Vec<DogStatsDMessageStats> = Vec::new();

    let mut line = String::new();
    while let Ok(num_read) = reader.read_msg(&mut line) {
        if num_read == 0 {
            // EOF
            break;
        }
        let parts: Vec<&str> = line.split('|').collect();
        let name_and_values: Vec<&str> = parts[0].split(':').collect();

        let name = name_and_values[0];

        let last_part = parts[parts.len() - 1];
        let mut num_tags = 0;
        let mut num_ascii_tags = 0;
        let mut num_unicode_tags = 0;
        if last_part.starts_with("#") {
            // these are tags
            let tags = last_part.split(',');
            for tag in tags {
                num_tags += 1;
                if tag.is_ascii() {
                    num_ascii_tags += 1;
                } else {
                    num_unicode_tags += 1;
                }
            }
        }

        let kind = match parts.get(1) {
            Some(s) => match *s {
                "d" => Some(Kind::Distribution),
                "ms" => Some(Kind::Timer),
                "g" => Some(Kind::Gauge),
                "c" => Some(Kind::Count),
                "s" => Some(Kind::Set),
                "h" => Some(Kind::Histogram),
                _ => {
                    if line.starts_with("_sc") {
                        Some(Kind::ServiceCheck)
                    } else if line.starts_with("_e") {
                        Some(Kind::Event)
                    } else {
                        println!("Found unknown msg type for dogstatsd msg: {}", line);
                        None
                    }
                }
            },
            _ => {
                println!("Found unusual dogstatsd msg: '{}'", line);
                None
            }
        };

        let ds = DogStatsDMessageStats {
            name_length: name.len() as u16,
            num_values: name_and_values.len() as u16 - 1,
            num_tags,
            num_ascii_tags,
            num_unicode_tags,
            kind,
        };

        msg_stats.push(ds);
        line.clear();
    }

    Ok(msg_stats)
}
