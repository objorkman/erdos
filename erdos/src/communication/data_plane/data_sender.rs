use futures::stream::SplitSink;
use futures_util::sink::SinkExt;

use tokio::{
    self,
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{errors::CommunicationError, InterWorkerMessage},
    node::WorkerId,
};

use super::{codec::MessageCodec, notifications::DataPlaneNotification};

/// The [`DataSender`] pulls messages from a FIFO inter-thread channel.
/// The [`DataSender`] services all operators sending messages to a particular
/// Worker which may result in congestion.
pub(crate) struct DataSender {
    /// The ID of the [`Worker`] that the TCP stream is sending data to.
    worker_id: WorkerId,
    /// The sender of the Framed TCP stream for the Worker connection.
    tcp_stream: SplitSink<Framed<TcpStream, MessageCodec>, InterWorkerMessage>,
    /// MPSC channel to receive data messages from operators that are to
    /// be forwarded on the underlying TCP stream.
    data_message_rx: UnboundedReceiver<InterWorkerMessage>,
    /// MPSC channel to communicate messages to the [`DataPlane`] handler.
    data_plane_notification_tx: UnboundedSender<DataPlaneNotification>,
}

impl DataSender {
    pub(crate) fn new(
        worker_id: WorkerId,
        tcp_stream: SplitSink<Framed<TcpStream, MessageCodec>, InterWorkerMessage>,
        data_message_rx: UnboundedReceiver<InterWorkerMessage>,
        data_plane_notification_tx: UnboundedSender<DataPlaneNotification>,
    ) -> Self {
        Self {
            worker_id,
            tcp_stream,
            data_message_rx,
            data_plane_notification_tx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify the Worker that the DataSender is initialized.
        self.data_plane_notification_tx
            .send(DataPlaneNotification::SenderInitialized(self.worker_id))
            .map_err(CommunicationError::from)?;

        tracing::debug!(
            "[DataSender for Worker {}] Initialized DataSender.",
            self.worker_id
        );

        // Listen for messages from different operators that must be forwarded on the TCP stream.
        loop {
            match self.data_message_rx.recv().await {
                Some(msg) => {
                    if let Err(e) = self
                        .tcp_stream
                        .send(msg)
                        .await
                        .map_err(CommunicationError::from)
                    {
                        return Err(e);
                    }
                }
                None => return Err(CommunicationError::Disconnected),
            }
        }
    }
}