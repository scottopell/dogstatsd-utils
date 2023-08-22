use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
};

pub trait DogStatsDReader {
    /// read_msg populates the given String with a dogstastd message
    fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize>;
}

pub struct BufDogStatsDReader {
    reader: Box<dyn BufRead>,
}

impl BufDogStatsDReader {
    pub fn new(reader: Box<dyn BufRead>) -> Self {
        BufDogStatsDReader { reader }
    }
}

// TODO refactor this into regular constructor
impl TryFrom<&Path> for BufDogStatsDReader {
    type Error = io::Error;

    fn try_from(p: &Path) -> Result<Self, Self::Error> {
        // Q: why do I not need to declare this file as mutable
        // and give a mutable reference to BufReader::new?
        // Is it because I'm transfering ownership?
        //
        // Related:
        // Why can I not do the same thing for
        //  DogStatsDReplay::TryFrom<File>
        // ?  I currently have
        //  DogStatsDReplay::TryFrom<&mut File>
        // but I don't like this. I read out some bytes from the file in TryFrom
        // so the file is in a unknown state after.
        // I'd rather transfer ownership to TryFrom, but I get an error saying
        // "File must be mutable"
        // Not sure what I'm missing here.

        let file = File::open(p)?;

        Ok(BufDogStatsDReader {
            reader: Box::new(BufReader::new(file)),
        })
    }
}

impl DogStatsDReader for BufDogStatsDReader {
    fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize> {
        match self.reader.read_line(s) {
            Ok(n) => {
                return if n == 0 {
                    // EOF
                    Ok(0)
                } else {
                    Ok(1)
                };
            }
            Err(e) => Err(e),
        }
    }
}
