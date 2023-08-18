use std::env;
use std::io::Error;

use dogstatsd_utils::dogstatsdreplay::DogStatsDReplay;


fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <file_path>", args[0]);
        std::process::exit(1);
    }
    let file_path = &args[1];

    let mut replay = DogStatsDReplay::try_from(file_path.as_str())?;

    let destination_file_path = file_path.to_owned() + ".txt";

    replay.write_to(&destination_file_path)?;

    println!("Done! Result is in {}", destination_file_path);
    Ok(())
}
