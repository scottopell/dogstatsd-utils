use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

pub mod analysis;
pub mod dogstatsdmsg;
pub mod dogstatsdreader;
pub mod dogstatsdreplayreader;
pub mod rate;
pub mod replay;
pub mod utf8dogstatsdreader;
pub mod zstd;

pub fn init_logging() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();
}
