// TODO(Sukrit): Rename this to worker.rs once the merge is complete.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::BytesMut;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        control_plane::{
            notifications::{DriverNotification, LeaderNotification, WorkerNotification},
            ControlPlaneCodec,
        },
        CommunicationError, EhloMetadata, InterProcessMessage, MessageCodec,
    },
    dataflow::{graph::JobGraph, stream::StreamId},
    node::Resources,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkerState {
    id: usize,
    address: SocketAddr,
    resources: Resources,
}

impl WorkerState {
    fn new(id: usize, address: SocketAddr, resources: Resources) -> Self {
        Self {
            id,
            address,
            resources,
        }
    }

    pub(crate) fn get_address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn get_id(&self) -> usize {
        self.id
    }
}

pub(crate) struct WorkerNode {
    worker_id: usize,
    leader_address: SocketAddr,
    resources: Resources,
    driver_notification_rx: Receiver<DriverNotification>,
    job_graphs: HashMap<String, JobGraph>,
}

impl WorkerNode {
    pub fn new(
        worker_id: usize,
        leader_address: SocketAddr,
        resources: Resources,
        driver_notification_rx: Receiver<DriverNotification>,
    ) -> Self {
        Self {
            worker_id,
            leader_address,
            resources,
            driver_notification_rx,
            job_graphs: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        // Connect to the Leader node.
        tracing::debug!("[Worker {}] Initialized Worker.", self.worker_id);
        let leader_connection = TcpStream::connect(self.leader_address).await?;
        let (mut leader_tx, mut leader_rx) = Framed::new(
            leader_connection,
            ControlPlaneCodec::<WorkerNotification, LeaderNotification>::default(),
        )
        .split();

        // Initialize the Data layer on a randomly-assigned port.
        // The data layer is used to retrieve the dataflow messages from other operators.
        let worker_data_listener = TcpListener::bind("0.0.0.0:0").await?;

        // Communicate the ID and data address of the Worker to the Leader.
        leader_tx
            .send(WorkerNotification::Initialized(WorkerState::new(
                self.worker_id,
                worker_data_listener.local_addr().unwrap(),
                self.resources.clone(),
            )))
            .await?;
        loop {
            tokio::select! {
                // Handle connections for data messages from other Workers.
                worker_connection = worker_data_listener.accept() => {
                    match worker_connection {
                        Ok((worker_stream, worker_address)) => {
                            tracing::debug!(
                                "[Worker {}] Received connection from address: {}",
                                self.worker_id,
                                worker_address
                            );
                            self.handle_worker_connections(worker_stream, worker_address).await;
                        }
                        Err(error) => {
                            tracing::error!(
                                "[Worker {}] Received an error when handling \
                                                    a Worker connection: {}",
                                self.worker_id,
                                error
                            );
                        }
                    }
                }

                // Handle messages received from the Leader.
                Some(msg_from_leader) = leader_rx.next() => {
                    match msg_from_leader {
                        Ok(msg_from_leader) => {
                            match msg_from_leader {
                                LeaderNotification::Shutdown => {
                                    tracing::debug!(
                                        "[Worker {}] Shutting down.",
                                        self.worker_id
                                    );
                                    return Ok(());
                                }
                                _ => {
                                    self.handle_leader_messages(
                                        msg_from_leader,
                                        &mut leader_tx
                                    ).await;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                "[Worker {}] Received error when retrieving messages \
                                                            from the Leader: {:?}",
                                self.worker_id,
                                error
                            );
                        },
                    }
                }

                // Handle messages received from the Driver.
                Some(driver_notification) = self.driver_notification_rx.recv() => {
                    match driver_notification {
                        DriverNotification::Shutdown => {
                            tracing::info!("[Worker {}] Shutting down.", self.worker_id);
                            if let Err(error) = leader_tx.send(WorkerNotification::Shutdown).await {
                                tracing::error!(
                                    "[Worker {}] Received an error when sending Shutdown message \
                                                                            to Leader: {:?}",
                                    self.worker_id,
                                    error
                                );
                            }
                            return Ok(());
                        }
                        _ => self.handle_driver_messages(driver_notification, &mut leader_tx).await,
                    }
                }
            }
        }
    }

    async fn handle_leader_messages(
        &mut self,
        msg_from_leader: LeaderNotification,
        leader_tx: &mut SplitSink<
            Framed<TcpStream, ControlPlaneCodec<WorkerNotification, LeaderNotification>>,
            WorkerNotification,
        >,
    ) {
        match msg_from_leader {
            LeaderNotification::ScheduleOperator(
                job_name,
                operator_id,
                source_worker_addresses,
            ) => {
                if let Some(job_graph) = self.job_graphs.get(&job_name) {
                    if let Some(operator) = job_graph.get_operator(&operator_id) {
                        tracing::debug!(
                            "[Worker {}] Received request to schedule {} with ID: {:?}.",
                            self.worker_id,
                            operator
                                .config
                                .name
                                .unwrap_or("UnnamedOperator".to_string()),
                            operator_id
                        );

                        // Connect to the addresses that send data to this Operator.
                        for (worker_id, worker_address) in source_worker_addresses {
                            // TODO (Sukrit): Check if we already have a connection.
                            match TcpStream::connect(worker_address).await {
                                Ok(worker_connection) => {
                                    tracing::debug!(
                                        "[Worker {}] Successfully connected to Worker {} at \
                                                                            address {}.",
                                        self.worker_id,
                                        worker_id,
                                        worker_address,
                                    );

                                    let (mut other_worker_tx, other_worker_rx) =
                                        Framed::new(worker_connection, MessageCodec::new()).split();
                                    let _ = other_worker_tx
                                        .send(InterProcessMessage::Ehlo {
                                            metadata: EhloMetadata {
                                                worker_id: self.worker_id,
                                            },
                                        })
                                        .await;
                                }
                                Err(error) => {
                                    tracing::error!(
                                        "[Worker {}] Received an error when connecting to Worker \
                                                                    {} at address {}: {:?}",
                                        self.worker_id,
                                        worker_id,
                                        worker_address,
                                        error,
                                    )
                                }
                            }
                        }

                        // TODO: Handle Operator
                        if let Err(error) = leader_tx
                            .send(WorkerNotification::OperatorReady(
                                job_name.clone(),
                                operator_id,
                            ))
                            .await
                        {
                            tracing::error!(
                                "[Worker {}] Could not communicate the Ready status of Operator \
                                        {} from the Job {} to the Leader. Received error {:?}",
                                self.worker_id,
                                operator_id,
                                job_name,
                                error,
                            )
                        }
                    } else {
                        tracing::error!(
                            "[Worker {}] Operator with ID: {} was not found in JobGraph {}",
                            self.worker_id,
                            operator_id,
                            job_name
                        );
                    }
                } else {
                    tracing::error!(
                        "[Worker {}] JobGraph {} was not registered on this Worker.",
                        self.worker_id,
                        job_name
                    );
                }
            }
            LeaderNotification::ExecuteGraph(job_name) => {
                tracing::debug!(
                    "[Worker {}] Executing JobGraph {}.",
                    self.worker_id,
                    job_name,
                );
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            LeaderNotification::Shutdown => unreachable!(),
        }
    }

    async fn handle_driver_messages(
        &mut self,
        driver_notification: DriverNotification,
        leader_tx: &mut SplitSink<
            Framed<TcpStream, ControlPlaneCodec<WorkerNotification, LeaderNotification>>,
            WorkerNotification,
        >,
    ) {
        match driver_notification {
            DriverNotification::RegisterGraph(job_graph) => {
                // Save the JobGraph.
                let job_graph_name = job_graph.get_name().clone().to_string();
                tracing::debug!(
                    "[Worker {}] Registered the JobGraph {}.",
                    self.worker_id,
                    job_graph_name
                );
                self.job_graphs.insert(job_graph_name, job_graph);
            }
            DriverNotification::SubmitGraph(job_graph_name) => {
                // Retrieve the JobGraph and communicate an Abstract version
                // of the graph to the Leader.
                if let Some(job_graph) = self.job_graphs.get(&job_graph_name) {
                    let internal_graph = job_graph.clone().into();
                    if let Err(error) = leader_tx
                        .send(WorkerNotification::SubmitGraph(
                            job_graph_name.clone(),
                            internal_graph,
                        ))
                        .await
                    {
                        tracing::error!(
                            "[Worker {}] Received an error when sending Abstract \
                                                Graph message to Leader: {:?}",
                            self.worker_id,
                            error
                        );
                    };
                } else {
                    tracing::error!(
                        "[Worker {}] Found no JobGraph with name {}.",
                        self.worker_id,
                        job_graph_name,
                    )
                }
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            DriverNotification::Shutdown => unreachable!(),
        }
    }

    async fn handle_worker_connections(
        &mut self,
        tcp_stream: TcpStream,
        worker_address: SocketAddr,
    ) {
        // Split the TCP stream into a Sink and a Stream, and receive the first message from the
        // Worker that contains the ID of the Worker.
        let (worker_sink, mut worker_stream) =
            Framed::new(tcp_stream, MessageCodec::default()).split();
        if let Some(result) = worker_stream.next().await {
            match result {
                Ok(message) => {
                    if let InterProcessMessage::Ehlo { metadata } = message {
                        let other_worker_id = metadata.worker_id;
                        tracing::debug!(
                        "[Worker {}] Received an incoming connection from Worker {} from address {}.",
                        self.worker_id,
                        other_worker_id,
                        worker_address,
                        );
                    } else {
                        tracing::debug!("The EHLO procedure went wrong!");
                    }
                }
                Err(_) => todo!(),
            }
        }
    }

    pub(crate) fn get_id(&self) -> usize {
        self.worker_id.clone()
    }
}
