use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use serde::Deserialize;

use crate::{
    dataflow::{
        graph::{default_graph, AbstractGraph},
        Data, Message,
    },
    scheduler::channel_manager::ChannelManager,
};

use super::{errors::SendError, Stream, StreamId, WriteStream, WriteStreamT};

/// An [`IngestStream`] enables drivers to inject data into a running ERDOS application.
///
/// Similar to a [`WriteStream`], an [`IngestStream`] exposes a [`send`](IngestStream::send)
/// function to allow drivers to send data to the operators of the constructed graph.
///
/// # Example
/// The below example shows how to use an [`IngestStream`] to send data to a
/// [`FlatMapOperator`](crate::dataflow::operators::FlatMapOperator),
/// and retrieve the processed values through an
/// [`ExtractStream`](crate::dataflow::stream::ExtractStream).
/// ```no_run
/// # use erdos::dataflow::{
/// #    stream::{IngestStream, ExtractStream, Stream},
/// #    operators::FlatMapOperator,
/// #    OperatorConfig, Message, Timestamp
/// # };
/// # use erdos::*;
/// # use erdos::node::Node;
/// #
/// let args = erdos::new_app("ERDOS").get_matches();
/// let mut node = Node::new(Configuration::from_args(&args));
///
/// // Create an IngestStream.
/// let mut ingest_stream = IngestStream::new();
///
/// // Create an ExtractStream from the ReadStream of the FlatMapOperator.
/// let output_stream = erdos::connect_one_in_one_out(
///     || FlatMapOperator::new(|x: &usize| { std::iter::once(2 * x) }),
///     || {},
///     OperatorConfig::new().name("MapOperator"),
///     &ingest_stream,
/// );
/// let mut extract_stream = ExtractStream::new(&output_stream);
///
/// node.run_async();
///
/// // Send data on the IngestStream.
/// for i in 1..10 {
///     ingest_stream.send(Message::new_message(Timestamp::Time(vec![i as u64]), i)).unwrap();
/// }
///
/// // Retrieve mapped values using an ExtractStream.
/// for i in 1..10 {
///     let message = extract_stream.read().unwrap();
///     assert_eq!(*message.data().unwrap(), 2 * i);
/// }
/// ```
pub struct IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    // The unique ID of the stream (automatically generated by the constructor)
    id: StreamId,
    // Use a std mutex because the driver doesn't run on the tokio runtime.
    write_stream_option: Arc<Mutex<Option<WriteStream<D>>>>,
}

impl<D> IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Returns a new instance of the [`IngestStream`].
    pub fn new() -> Self {
        tracing::debug!("Initializing an IngestStream");
        let id = StreamId::new_deterministic();
        let ingest_stream = Self {
            id,
            write_stream_option: Arc::new(Mutex::new(None)),
        };

        // A hook to initialize the ingest stream's connections to downstream operators.
        let write_stream_option_copy = Arc::clone(&ingest_stream.write_stream_option);

        let setup_hook = move |graph: &AbstractGraph, channel_manager: &mut ChannelManager| {
            match channel_manager.get_send_endpoints(id) {
                Ok(send_endpoints) => {
                    let write_stream =
                        WriteStream::new(id, &graph.get_stream_name(&id), send_endpoints);
                    write_stream_option_copy
                        .lock()
                        .unwrap()
                        .replace(write_stream);
                }
                Err(msg) => panic!("Unable to set up IngestStream {}: {}", id, msg),
            }
        };

        default_graph::add_ingest_stream(&ingest_stream, setup_hook);
        default_graph::set_stream_name(&id, &format!("ingest_stream_{}", id));

        ingest_stream
    }

    /// Returns `true` if a top watermark message was received or the [`IngestStream`] failed to
    /// set up.
    pub fn is_closed(&self) -> bool {
        self.write_stream_option
            .lock()
            .unwrap()
            .as_ref()
            .map(WriteStream::is_closed)
            .unwrap_or(true)
    }

    /// Sends data on the stream.
    ///
    /// # Arguments
    /// * `msg` - The message to be sent on the stream.
    pub fn send(&mut self, msg: Message<D>) -> Result<(), SendError> {
        if !self.is_closed() {
            loop {
                {
                    if let Some(write_stream) = self.write_stream_option.lock().unwrap().as_mut() {
                        let res = write_stream.send(msg);
                        return res;
                    }
                }
                thread::sleep(Duration::from_millis(100));
            }
        } else {
            tracing::warn!(
                "Trying to send messages on a closed IngestStream {} (ID: {})",
                default_graph::get_stream_name(&self.id()),
                self.id(),
            );
            Err(SendError::Closed)
        }
    }
}

impl<D> Default for IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<D> Stream<D> for IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn id(&self) -> StreamId {
        self.id
    }
}

impl<D> WriteStreamT<D> for IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Blocks until write stream is available
    fn send(&mut self, msg: Message<D>) -> Result<(), SendError> {
        self.send(msg)
    }
}