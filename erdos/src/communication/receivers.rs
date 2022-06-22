use std::collections::HashMap;

use futures::{future, stream::SplitStream, FutureExt};
use futures_util::stream::StreamExt;
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        CommunicationError, ControlMessage, ControlMessageHandler, InterProcessMessage,
        MessageCodec, PusherT,
    },
    dataflow::stream::StreamId,
    node::NodeId,
};

/// Listens on a TCP stream, and pushes messages it receives to operator executors.
#[allow(dead_code)]
pub(crate) struct DataReceiver {
    /// The id of the node the TCP stream is receiving data from.
    node_id: NodeId,
    /// Framed TCP read stream.
    stream: SplitStream<Framed<TcpStream, MessageCodec>>,
    /// Channel receiver on which new pusher updates are received.
    rx: UnboundedReceiver<(StreamId, Box<dyn PusherT>)>,
    /// Mapping between stream id to [`PusherT`] trait objects.
    /// [`PusherT`] trait objects are used to deserialize and send
    /// messages to operators.
    stream_id_to_pusher: HashMap<StreamId, Box<dyn PusherT>>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

impl DataReceiver {
    pub(crate) async fn new(
        node_id: NodeId,
        stream: SplitStream<Framed<TcpStream, MessageCodec>>,
        receiver_control_rx: UnboundedReceiver<(StreamId, Box<dyn PusherT>)>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_data_receiver(node_id, control_tx);
        Self {
            node_id,
            stream,
            rx: receiver_control_rx,
            stream_id_to_pusher: HashMap::new(),
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify `ControlMessageHandler` that receiver is initialized.
        self.control_tx
            .send(ControlMessage::DataReceiverInitialized(self.node_id))
            .map_err(CommunicationError::from)?;
        while let Some(res) = self.stream.next().await {
            match res {
                // Push the message to the listening operator executors.
                Ok(msg) => {
                    // Update pushers before we send the message.
                    // Note: we may want to update the pushers less frequently.
                    self.update_pushers().await;
                    // Send the message.
                    let (metadata, bytes) = match msg {
                        InterProcessMessage::Serialized { metadata, bytes } => (metadata, bytes),
                        InterProcessMessage::Deserialized {
                            metadata: _,
                            data: _,
                        } => unreachable!(),
                    };
                    match self.stream_id_to_pusher.get_mut(&metadata.stream_id) {
                        Some(pusher) => {
                            if let Err(e) = pusher.send_from_bytes(bytes) {
                                return Err(e);
                            }
                        }
                        None => panic!(
                            "Receiver does not have any pushers. \
                             Race condition during data-flow reconfiguration."
                        ),
                    }
                }
                Err(e) => return Err(CommunicationError::from(e)),
            }
        }
        Ok(())
    }

    // TODO: update this method.
    async fn update_pushers(&mut self) {
        // Execute while we still have pusher updates.
        while let Some(Some((stream_id, pusher))) = self.rx.recv().now_or_never() {
            self.stream_id_to_pusher.insert(stream_id, pusher);
        }
    }
}

/// Receives TCP messages, and pushes them to operators endpoints.
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
pub(crate) async fn run_receivers(
    mut receivers: Vec<DataReceiver>,
) -> Result<(), CommunicationError> {
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run())).await;
    Ok(())
}
