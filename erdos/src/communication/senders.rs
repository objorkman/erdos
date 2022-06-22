use futures::{future, stream::SplitSink};
use futures_util::sink::SinkExt;

use tokio::{
    self,
    net::TcpStream,
    sync::mpsc::{self, UnboundedReceiver},
};
use tokio_util::codec::Framed;

use crate::communication::{CommunicationError, ControlMessage, InterProcessMessage, MessageCodec};

#[allow(dead_code)]
/// The [`DataSender`] pulls messages from a FIFO inter-thread channel.
/// The [`DataSender`] services all operators sending messages to a particular
/// node which may result in congestion.
pub(crate) struct DataSender {
    /// The ID of the [`Worker`] that the TCP stream is sending data to.
    worker_id: usize,
    /// The sender of the Framed TCP stream for the Worker connection.
    tcp_stream: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
    /// MPSC channel to receive data messages from operators that are to
    /// be forwarded on the underlying TCP stream.
    data_message_rx: UnboundedReceiver<InterProcessMessage>,
    /// MPSC channel to communicate messages to the Worker.
    channel_to_worker: mpsc::Sender<ControlMessage>,
}

impl DataSender {
    pub(crate) async fn new(
        worker_id: usize,
        tcp_stream: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
        data_message_rx: UnboundedReceiver<InterProcessMessage>,
        channel_to_worker: mpsc::Sender<ControlMessage>,
    ) -> Self {
        Self {
            worker_id,
            tcp_stream,
            data_message_rx,
            channel_to_worker,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify the Worker that the DataSender is initialized.
        self.channel_to_worker
            .send(ControlMessage::DataSenderInitialized(self.worker_id))
            .await
            .map_err(CommunicationError::from)?;

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

/// Sends messages received from operator executors to other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `InterProcessMessages` messages, which it
/// forwards on the TCP stream.
pub(crate) async fn run_senders(senders: Vec<DataSender>) -> Result<(), CommunicationError> {
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(
        senders
            .into_iter()
            .map(|mut sender| tokio::spawn(async move { sender.run().await })),
    )
    .await;
    Ok(())
}
