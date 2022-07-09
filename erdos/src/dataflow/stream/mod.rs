//! Streams are used to send data between [operators](crate::dataflow::operator).
//!
//! In the driver, connections between operators are created by passing
//! [`Stream`]s as arguments to the [`Graph`](crate::dataflow::graph::Graph)'s
//! connect functions.
//!
//! During execution, operators can broadcast data to all downstream operators
//! connected to a stream by invoking [`WriteStreamT::send`].
//! Likewise, operators can process data by implementing callbacks
//! in the [operator traits](crate::dataflow::operator),
//! or by calling [`ReadStream::read`] or [`ReadStream::try_read`] in an
//! operator's `run` method.
//!
//! The driver can interact with an application by sending messages on an
//! [`IngressStream`] or reading messages from an [`EgressStream`].
//!
//! Messages sent on a stream are broadcast to all connected operators,
//! using zero-copy communication for operators on the same node.
//! Messages sent across nodes are serialized using
//! [abomonation](https://github.com/TimelyDataflow/abomonation) if possible,
//! before falling back to [bincode](https://github.com/servo/bincode).
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::dataflow::{Data, Message};

// Private submodules
mod egress_stream;
mod ingress_stream;
mod loop_stream;
mod read_stream;
mod write_stream;

// Public submodules
pub mod errors;

// Private imports
use errors::SendError;

// Public exports
pub use egress_stream::EgressStream;
pub use ingress_stream::IngressStream;
#[doc(hidden)]
pub use loop_stream::LoopStream;
pub use read_stream::ReadStream;
use serde::Deserialize;
pub use write_stream::WriteStream;

pub(crate) use private::InternalStream;

use super::graph::InternalGraph;

pub type StreamId = crate::Uuid;

/// Write stream trait which allows specialized implementations of
/// [`send`](WriteStreamT::send) depending on the serialization library used.
pub trait WriteStreamT<D: Data> {
    /// Sends a messsage to a channel.
    fn send(&mut self, msg: Message<D>) -> Result<(), SendError>;
}

/// Defines edges in the [`Graph`](crate::dataflow::Graph).
pub trait Stream<D: Data>: InternalStream<D> {
    /// The name of the stream used in debugging.
    fn name(&self) -> String;
    /// The stream's unique ID.
    fn id(&self) -> StreamId;
}

impl<'a, D: Data> Stream<D> for &'a dyn Stream<D>
where
    &'a dyn Stream<D>: InternalStream<D>,
{
    fn name(&self) -> String {
        (**self).name()
    }
    fn id(&self) -> StreamId {
        (**self).id()
    }
}

mod private {
    use super::{Arc, Data, InternalGraph, Mutex};
    /// A sealed trait implemented for ERDOS Stream types.
    pub trait InternalStream<D: Data>: Send + Sync {
        /// Returns a reference to the internal graph. Used to support the LINQ API.
        fn internal_graph(&self) -> Arc<Mutex<InternalGraph>>;
    }

    impl<D: Data> InternalStream<D> for &dyn InternalStream<D> {
        fn internal_graph(&self) -> Arc<Mutex<InternalGraph>> {
            (**self).internal_graph()
        }
    }
}

#[derive(Clone)]
pub struct OperatorStream<D: Data> {
    /// The unique ID of the stream (automatically generated by the constructor)
    id: StreamId,
    name: String,
    phantom: PhantomData<D>,
    graph: Arc<Mutex<InternalGraph>>,
}

impl<D: Data> OperatorStream<D> {
    /// Creates a new stream.
    pub(crate) fn new(name: &str, graph: Arc<Mutex<InternalGraph>>) -> Self {
        Self {
            id: StreamId::new_deterministic(),
            name: name.to_string(),
            phantom: PhantomData,
            graph,
        }
    }

    /// Adds an [`EgressStream`] to the graph.
    /// [`EgressStream`]s are automatically named based on the input stream.
    pub fn to_egress(&self) -> EgressStream<D>
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let egress_stream = EgressStream::new(self);
        self.internal_graph()
            .lock()
            .unwrap()
            .add_egress_stream(&egress_stream);

        egress_stream
    }
}

impl<D: Data> Stream<D> for OperatorStream<D> {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn id(&self) -> StreamId {
        self.id
    }
}

impl<D: Data> InternalStream<D> for OperatorStream<D> {
    fn internal_graph(&self) -> Arc<Mutex<InternalGraph>> {
        Arc::clone(&self.graph)
    }
}
