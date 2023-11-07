#[derive(Default, Debug, Clone, Copy)]
pub struct Config {
    pub max_store_bytes: u64,
    pub max_index_bytes: u64,
    pub initial_offset: u64,
}
