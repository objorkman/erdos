//! Functions and structures for building an ERDOS application.

// Crate-wide submodules.
pub(crate) mod graph;

// Public submodules
pub mod context;
pub mod deadlines;
pub mod message;
pub mod operator;
pub mod operators;
pub mod state;
pub mod stream;
pub mod time;

// Public exports
pub use deadlines::TimestampDeadline;
pub use graph::Graph;
pub use message::{Data, Message, TimestampedData};
pub use operator::OperatorConfig;
pub use state::{AppendableState, State};
pub use stream::{LoopStream, ReadStream, Stream, WriteStream};
pub use time::Timestamp;
