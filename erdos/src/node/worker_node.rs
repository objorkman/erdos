// TODO(Sukrit): Rename this to worker.rs once the merge is complete.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures::{stream::SplitSink, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, UnboundedSender},
};
use tokio_util::codec::Framed;

use crate::{
    communication::{
        control_plane::{
            notifications::{DriverNotification, LeaderNotification, WorkerNotification},
            ControlPlaneCodec,
        },
        data_plane::{
            data_plane::DataPlane,
            notifications::{DataPlaneNotification, StreamType},
            StreamManager,
        },
        CommunicationError,
    },
    dataflow::{
        graph::{Job, JobGraph, JobGraphId},
        stream::StreamId,
    },
    node::{worker::Worker, Resources},
};

use super::WorkerId;

/// An alias for the type of the connection between the [`Leader`] and the [`Worker`].
type ConnectionToLeader = SplitSink<
    Framed<TcpStream, ControlPlaneCodec<WorkerNotification, LeaderNotification>>,
    WorkerNotification,
>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkerState {
    id: WorkerId,
    address: SocketAddr,
    resources: Resources,
}

impl WorkerState {
    fn new(id: WorkerId, address: SocketAddr, resources: Resources) -> Self {
        Self {
            id,
            address,
            resources,
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn id(&self) -> WorkerId {
        self.id
    }
}

#[derive(Debug)]
enum JobState {
    Scheduled,
    Ready,
    Executing,
    Shutdown,
}

pub(crate) struct WorkerNode {
    /// The ID of the [`Worker`].
    id: WorkerId,
    /// The address of the [`Leader`] that the [`Worker`] will connect to.
    leader_address: SocketAddr,
    /// The address of the [`DataPlane`] where the [`Worker`] will listen
    /// for incoming connections from other [`Worker`]s.
    data_plane_address: SocketAddr,
    /// The set of [`Resources`] that the [`Worker`] owns.
    resources: Resources,
    /// A channel where the [`Worker`] receives notifications from the [`Driver`].
    driver_notification_rx: Receiver<DriverNotification>,
    /// A mapping of the [`JobGraph`]s that have been submitted to the [`Worker`].
    job_graphs: HashMap<JobGraphId, JobGraph>,
    /// A memo of the stream connections that are remaining to be setup for
    /// each [`Job`] before it can be marked Ready to the [`Leader`].
    pending_stream_setups: HashMap<Job, (JobGraphId, HashSet<StreamId>)>,
    /// A mapping of the `JobGraph` to the state of each scheduled `Job`.
    job_graph_to_job_state: HashMap<JobGraphId, HashMap<Job, JobState>>,
    /// A handle to the [`StreamManager`] instance shared with the [`DataPlane`].
    /// The [`DataPlane`] populates the channels on the shared instance upon request,
    /// which are then retrieved for consumption by each [`Job`].
    stream_manager: Arc<Mutex<StreamManager>>,
}

impl WorkerNode {
    /// Initializes a new [`Worker`] with the given ID and available [`Resources`].
    pub fn new(
        id: WorkerId,
        leader_address: SocketAddr,
        data_plane_address: SocketAddr,
        resources: Resources,
        driver_notification_rx: Receiver<DriverNotification>,
    ) -> Self {
        Self {
            id,
            leader_address,
            data_plane_address,
            resources,
            driver_notification_rx,
            job_graphs: HashMap::new(),
            pending_stream_setups: HashMap::new(),
            job_graph_to_job_state: HashMap::new(),
            stream_manager: Arc::new(Mutex::new(StreamManager::new(id))),
        }
    }

    /// Runs the main loop of the [`Worker`].
    /// A [`Worker`] connects to the [`Leader`], initiates a [`DataPlane`] for other [`Worker`]s
    /// to be able to connect to it, and then responds to notifications from the [`Leader`], the
    /// driver and other workers via the [`DataPlane`].
    pub async fn run(&mut self) -> Result<(), CommunicationError> {
        // Connect to the Leader.
        tracing::trace!(
            "[Worker {}] Initializing Worker and connecting to Leader at address {}.",
            self.id,
            self.leader_address
        );
        let leader_connection = TcpStream::connect(self.leader_address).await?;
        let (mut leader_tx, mut leader_rx) = Framed::new(
            leader_connection,
            ControlPlaneCodec::<WorkerNotification, LeaderNotification>::default(),
        )
        .split();

        // Initialize the DataPlane on the specified address.
        tracing::trace!(
            "[Worker {}] Initiating a DataPlane for Worker at address {}.",
            self.id,
            self.data_plane_address
        );
        let (mut channel_to_data_plane_tx, channel_to_data_plane_rx) = mpsc::unbounded_channel();
        let (channel_from_data_plane_tx, mut channel_from_data_plane_rx) =
            mpsc::unbounded_channel();
        let mut data_plane = DataPlane::new(
            self.id,
            self.data_plane_address,
            Arc::clone(&self.stream_manager),
            channel_to_data_plane_rx,
            channel_from_data_plane_tx,
        )
        .await?;
        // The DataPlane might be required to bind to a randomly-assigned port,
        // so we retrieve the actual address and communicate it to the Leader.
        let data_plane_address = data_plane.address();
        let data_plane_handle = tokio::spawn(async move { data_plane.run().await });

        // Communicate the ID and DataPlane address of the Worker to the Leader.
        leader_tx
            .send(WorkerNotification::Initialized(WorkerState::new(
                self.id,
                data_plane_address,
                self.resources.clone(),
            )))
            .await?;
        tracing::debug!(
            "[Worker {}] Successfully Initialized Worker with the DataPlane address {}.",
            self.id,
            data_plane_address
        );

        // Respond to notifications from the Leader, the Driver and other Workers.
        loop {
            tokio::select! {
                // Handle messages received from the Leader.
                Some(msg_from_leader) = leader_rx.next() => {
                    match msg_from_leader {
                        Ok(msg_from_leader) => {
                            match msg_from_leader {
                                LeaderNotification::Shutdown => {
                                    tracing::info!(
                                        "[Worker {}] Shutting down upon request from the Leader.",
                                        self.id
                                    );
                                    return Ok(());
                                }
                                _ => {
                                    self.handle_leader_messages(
                                        msg_from_leader,
                                        &mut channel_to_data_plane_tx,
                                    ).await;
                                }
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                "[Worker {}] Received error when retrieving messages \
                                                            from the Leader: {:?}",
                                self.id,
                                error
                            );
                        },
                    }
                }

                // Handle messages received from the Driver.
                Some(driver_notification) = self.driver_notification_rx.recv() => {
                    match driver_notification {
                        DriverNotification::Shutdown => {
                            tracing::info!(
                                "[Worker {}] Shutting down upon request from the Driver.",
                                self.id
                            );
                            if let Err(error) = leader_tx.send(WorkerNotification::Shutdown).await {
                                tracing::error!(
                                    "[Worker {}] Received an error when sending Shutdown message \
                                                                            to Leader: {:?}",
                                    self.id,
                                    error
                                );
                            }
                            tokio::join!(data_plane_handle);
                            return Ok(());
                        }
                        _ => self.handle_driver_messages(driver_notification, &mut leader_tx).await,
                    }
                }

                // Handle messages received from the DataPlane.
                Some(data_plane_notification) = channel_from_data_plane_rx.recv() => {
                    self.handle_data_plane_messages(data_plane_notification, &mut leader_tx).await;
                }
            }
        }
    }

    /// Responds to notifications received from the [`DataPlane`].
    async fn handle_data_plane_messages(
        &mut self,
        notification: DataPlaneNotification,
        leader_tx: &mut ConnectionToLeader,
    ) {
        match notification {
            DataPlaneNotification::StreamReady(job, stream_id) => {
                tracing::trace!(
                    "[Worker {}] Received StreamReady notification for Stream {} for Job {:?}.",
                    self.id,
                    stream_id,
                    job
                );

                // Remove the stream from the memo of streams left to finish setting
                // up for the given Job.
                match self.pending_stream_setups.get_mut(&job) {
                    Some((job_graph_id, pending_streams)) => {
                        match pending_streams.remove(&stream_id) {
                            true => {
                                // If the set is empty, notify the Leader of the
                                // successful initialization of the Job.
                                if pending_streams.is_empty() {
                                    if let Err(error) = leader_tx
                                        .send(WorkerNotification::JobReady(
                                            job_graph_id.clone(),
                                            job,
                                        ))
                                        .await
                                    {
                                        tracing::error!(
                                            "[Worker {}] Could not communicate the Ready status \
                                            of Job {:?} from the JobGraph {:?} to the Leader. \
                                                                    Received error {:?}",
                                            self.id,
                                            job,
                                            job_graph_id,
                                            error,
                                        )
                                    }

                                    // Change the state of the Job in the JobGraph.
                                    match self.job_graph_to_job_state.get_mut(job_graph_id) {
                                        Some(job_state) => match job_state.get_mut(&job) {
                                            Some(job_state) => {
                                                *job_state = JobState::Ready;
                                            }
                                            None => {
                                                tracing::warn!(
                                                    "[Worker {}] Could not find the state of \
                                                            the Job {:?} that was supposed to be \
                                                            scheduled for the JobGraph {:?}.",
                                                    self.id,
                                                    job,
                                                    job_graph_id,
                                                )
                                            }
                                        },
                                        None => {
                                            tracing::warn!(
                                                "[Worker {}] Inconsistency between the state of \
                                                the pending streams for Job {:?} and the state \
                                                    of the JobGraph {:?} to which it belongs.",
                                                self.id,
                                                job,
                                                job_graph_id
                                            );
                                        }
                                    }

                                    // Remove the mapping from the pending setups.
                                    self.pending_stream_setups.remove(&job);
                                }
                            }
                            false => {
                                tracing::warn!(
                                    "[Worker {}] Could not find pending Stream {:?} for \
                                                Job {:?} from the JobGraph {:?}.",
                                    self.id,
                                    stream_id,
                                    job,
                                    job_graph_id,
                                );
                            }
                        }
                    }
                    None => {
                        tracing::warn!(
                            "[Worker {}] Inconsistency between the state of \
                                the pending Stream setups for Job {:?}.",
                            self.id,
                            job,
                        );
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    /// Responds to notifications received from the [`Leader`].
    async fn handle_leader_messages(
        &mut self,
        msg_from_leader: LeaderNotification,
        channel_to_data_plane: &mut UnboundedSender<DataPlaneNotification>,
    ) {
        match msg_from_leader {
            LeaderNotification::ScheduleJob(job_graph_id, job, worker_addresses) => {
                if let Some(job_graph) = self.job_graphs.get(&job_graph_id) {
                    if let Some(operator) = job_graph.get_job(&job) {
                        let operator_name = match &operator.config.name {
                            Some(name) => name.clone(),
                            None => "UnnamedOperator".to_string(),
                        };
                        tracing::debug!(
                            "[Worker {}] Received request to schedule Job {} (ID={:?}) \
                                                            from JobGraph {:?}.",
                            self.id,
                            operator_name,
                            operator.id,
                            job_graph_id,
                        );

                        // Construct a representation of the Streams for this Operator, along with
                        // the addresses of the Workers executing the sources or destinations.
                        let mut streams = Vec::new();
                        for read_stream_id in operator.read_streams {
                            let read_stream = job_graph.get_stream(&read_stream_id).unwrap();
                            let source_address = worker_addresses
                                .get(&read_stream.get_source())
                                .expect(&format!(
                                "Could not find the address for the Worker executing the Job {:?}.",
                                read_stream.get_source()
                            ));
                            streams
                                .push(StreamType::ReadStream(read_stream, source_address.clone()));
                        }

                        for write_stream_id in operator.write_streams {
                            let write_stream = job_graph.get_stream(&write_stream_id).unwrap();
                            let destination_addresses = write_stream
                                .get_destinations()
                                .iter()
                                .map(|job| {
                                    (
                                        job.clone(),
                                        worker_addresses
                                            .get(job)
                                            .expect(&format!(
                                                "Could not find address for the Worker executing \
                                                    the Job {:?}.",
                                                job
                                            ))
                                            .clone(),
                                    )
                                })
                                .collect();
                            streams
                                .push(StreamType::WriteStream(write_stream, destination_addresses));
                        }

                        // Cache the streams that need to be initialized to call this Operator ready.
                        let pending_setups = streams.iter().map(|stream| stream.id()).collect();
                        tracing::trace!(
                            "[Worker {}] The Job {:?} is pending setup of {:?} streams.",
                            self.id,
                            job,
                            pending_setups
                        );
                        self.pending_stream_setups
                            .insert(job, (job_graph_id.clone(), pending_setups));

                        // Add the Job to the set of scheduled Jobs for this JobGraph.
                        let job_state =
                            self.job_graph_to_job_state.entry(job_graph_id).or_default();
                        job_state.insert(job, JobState::Scheduled);

                        if let Err(error) = channel_to_data_plane
                            .send(DataPlaneNotification::SetupStreams(job, streams))
                        {
                            tracing::warn!(
                                "[Worker {}] Received error when requesting the setup of \
                                                    streams for {} with ID {:?}: {:?}",
                                self.id,
                                operator_name,
                                operator.id,
                                error
                            );
                        }
                    } else {
                        tracing::error!(
                            "[Worker {}] The Job {:?} was not found in JobGraph {:?}.",
                            self.id,
                            job,
                            job_graph_id,
                        );
                    }
                } else {
                    tracing::error!(
                        "[Worker {}] JobGraph {:?} was not registered on this Worker.",
                        self.id,
                        job_graph_id,
                    );
                }
            }
            LeaderNotification::ExecuteGraph(job_graph_id) => {
                tracing::debug!(
                    "[Worker {}] Executing JobGraph {:?}.",
                    self.id,
                    job_graph_id
                );
                tracing::info!(
                    "[Worker {}] The state of the JobGraph is {:?}.",
                    self.id,
                    self.job_graph_to_job_state
                );

                // TODO (Sukrit): Fix this code.
                let mut worker = Worker::new(2);
                let mut job_executors = Vec::new();
                for (job, _) in self.job_graph_to_job_state.get(&job_graph_id).unwrap() {
                    let job_graph = self.job_graphs.get(&job_graph_id).unwrap();
                    let operator = job_graph.get_job(job).unwrap();
                    let channel_manager_copy = Arc::clone(&self.stream_manager);
                    if let Some(operator_runner) = job_graph.get_operator_runner(&operator.id) {
                        let operator_executor = (operator_runner)(channel_manager_copy);
                        job_executors.push(operator_executor);
                    }
                }
                worker.spawn_tasks(job_executors).await;
                std::thread::sleep_ms(1000);
                worker.execute().await;
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            LeaderNotification::Shutdown => unreachable!(),
        }
    }

    /// Responds to the notifications received from the Driver.
    async fn handle_driver_messages(
        &mut self,
        driver_notification: DriverNotification,
        leader_tx: &mut ConnectionToLeader,
    ) {
        match driver_notification {
            DriverNotification::RegisterGraph(job_graph) => {
                // Save the JobGraph.
                let job_graph_id = job_graph.id();
                tracing::debug!(
                    "[Worker {}] Registered the JobGraph {:?}.",
                    self.id,
                    job_graph_id
                );
                self.job_graphs.insert(job_graph_id, job_graph);
            }
            DriverNotification::SubmitGraph(job_graph_id) => {
                // Retrieve the JobGraph and communicate an Abstract version
                // of the graph to the Leader.
                if let Some(job_graph) = self.job_graphs.get(&job_graph_id) {
                    let internal_graph = job_graph.clone().into();
                    if let Err(error) = leader_tx
                        .send(WorkerNotification::SubmitGraph(
                            job_graph_id,
                            internal_graph,
                        ))
                        .await
                    {
                        tracing::error!(
                            "[Worker {}] Received an error when sending Abstract \
                                                Graph message to Leader: {:?}",
                            self.id,
                            error
                        );
                    };
                } else {
                    tracing::error!(
                        "[Worker {}] Found no JobGraph with ID {:?}.",
                        self.id,
                        job_graph_id,
                    )
                }
            }
            // The shutdown arm is unreachable, because it should be handled in the main loop.
            DriverNotification::Shutdown => unreachable!(),
        }
    }

    pub(crate) fn id(&self) -> WorkerId {
        self.id.clone()
    }
}
