pub trait DogStatsDReader {
    /// read_msg populates the given String with a dogstastd message
    fn read_msg(&mut self, s: &mut String) -> std::io::Result<usize>;
}
