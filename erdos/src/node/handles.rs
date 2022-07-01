use std::{error::Error, fmt, net::SocketAddr};

use tokio::{runtime::Builder, sync::mpsc, task::JoinHandle};
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::{
    communication::{control_plane::notifications::DriverNotification, CommunicationError},
    dataflow::{
        graph::{GraphCompilationError, JobGraphId},
        Graph,
    },
    node::{Leader, Resources, WorkerNode},
    Configuration,
};

use super::WorkerId;

/// The error raised by the handles when executing commands from the drivers.
#[derive(Debug)]
pub enum HandleError {
    GraphCompilationError(GraphCompilationError),
    CommunicationError(String),
}

impl Error for HandleError {}

impl fmt::Display for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandleError::GraphCompilationError(err) => write!(f, "{}", err),
            HandleError::CommunicationError(err) => write!(f, "{}", err),
        }
    }
}

/// A [`LeaderHandle`] is used by driver applications to interact
/// with the Leader node running on their local instance.
pub struct LeaderHandle {
    /// A handle to communicate notifications to the underlying Leader.
    leader_handle: mpsc::Sender<DriverNotification>,
    /// An ID for the LeaderHandle that mirrors the ID of the underlying Leader.
    handle_id: usize,
    /// A handle for the asynchronously running Leader task.
    leader_task: JoinHandle<Result<(), CommunicationError>>,
    /// A handle for the Logging subsystem that flushes the logs when dropped.
    logger_guard: Option<WorkerGuard>,
}

impl LeaderHandle {
    pub fn new(leader_address: SocketAddr, logging_level: Option<tracing::Level>) -> Self {
        // Initialize the logger.
        let logger_guard = if let Some(logging_level) = logging_level {
            let display_thread_ids = logging_level >= Level::TRACE;
            let display_target = logging_level >= Level::TRACE;

            let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());
            let subscriber = tracing_subscriber::fmt()
                .with_writer(non_blocking)
                .with_thread_ids(display_thread_ids)
                .with_span_events(FmtSpan::FULL)
                .with_target(display_target)
                .with_max_level(logging_level);
            subscriber.init();

            Some(guard)
        } else {
            None
        };

        // Initialize a channel between the Handle and the Leader.
        // This channel is used by the Handle to submit requests to the Leader.
        let (leader_tx, leader_rx) = mpsc::channel(100);

        // Initialize a Leader.
        let mut leader_node = Leader::new(leader_address, leader_rx);
        let leader_task = tokio::spawn(async move { leader_node.run().await });
        Self {
            leader_handle: leader_tx,
            handle_id: 0,
            leader_task,
            logger_guard,
        }
    }

    pub async fn shutdown(&self) -> Result<(), HandleError> {
        // Send a shutdown message to the Leader.
        self.leader_handle
            .send(DriverNotification::Shutdown)
            .await
            .map_err(|_| {
                HandleError::CommunicationError(String::from(
                    "Error submitting Shutdown message to Leader.",
                ))
            })?;
        Ok(())
    }
}

/// A [`WorkerHandle`] is used by driver applications to submit ERDOS applications
/// to the ERDOS Leader, and query their execution progres.
pub struct WorkerHandle {
    /// A handle to communicate notifications to the underlying Worker.
    worker_handle: mpsc::Sender<DriverNotification>,
    /// An ID for the WorkerHandle that mirrors the ID of the underlying Worker.
    handle_id: WorkerId,
    /// A handle for the asynchronously running Worker task.
    worker_task: JoinHandle<Result<(), CommunicationError>>,
    /// A handle to the Tokio runtime spawned for this Worker.
    worker_runtime: tokio::runtime::Runtime,
    /// A handle for the Logging subsystem that flushes the logs when dropped.
    logger_guard: Option<WorkerGuard>,
}

impl WorkerHandle {
    pub fn new(config: Configuration) -> Self {
        // Initialize the logger.
        let logger_guard = if let Some(logging_level) = config.logging_level {
            let display_thread_ids = logging_level >= Level::TRACE;
            let display_target = logging_level >= Level::TRACE;

            let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());
            let subscriber = tracing_subscriber::fmt()
                .with_writer(non_blocking)
                .with_thread_ids(display_thread_ids)
                .with_span_events(FmtSpan::FULL)
                .with_target(display_target)
                .with_max_level(logging_level);
            subscriber.init();

            Some(guard)
        } else {
            None
        };

        // Build a Tokio runtime.
        let worker_runtime = Builder::new_multi_thread()
            .worker_threads(config.num_threads)
            .thread_name(format!("Worker-{}", config.id))
            .enable_all()
            .build()
            .unwrap();

        // Initialize a channel between the Handle and the Worker.
        // This channel is used by the Handle to submit requests to the Worker.
        let (worker_tx, worker_rx) = mpsc::channel(100);

        // Initialize a Worker with the given index, and an empty set of Resources.
        // TODO (Sukrit): In the future, the index of the Worker should be generated
        // at runtime, and be of a type alias WorkerId for Uuid.
        let worker_resources = Resources::empty();
        let mut worker = WorkerNode::new(
            config.id,
            config.leader_address,
            config.data_plane_address,
            worker_resources,
            worker_rx,
        );
        let worker_task = worker_runtime.spawn(async move { worker.run().await });
        Self {
            handle_id: config.id,
            worker_handle: worker_tx,
            worker_task,
            worker_runtime,
            logger_guard,
        }
    }

    // TODO (Sukrit): This function is kept different from the `submit` method
    // because all Workers need a copy of the JobGraph code, but only one of
    // them needs to submit it to the Leader. This should later be removed if
    // we choose to dynamically link the user applications into the Worker's
    // memory space.
    /// Registers the [`Graph`] for execution with the [`Worker`]s.
    pub fn register(&self, graph: Graph) -> Result<JobGraphId, HandleError> {
        // Compile the JobGraph and register it with the Worker.
        let job_graph = graph
            .compile()
            .map_err(|err| HandleError::GraphCompilationError(err))?;
        let job_graph_id = job_graph.id();
        tracing::trace!(
            "WorkerHandle {} received a notification from the Driver \
                            to register JobGraph {} (ID={:?}).",
            self.handle_id,
            job_graph.name(),
            job_graph_id,
        );
        self.worker_handle
            .blocking_send(DriverNotification::RegisterGraph(job_graph))
            .map_err(|_| {
                HandleError::CommunicationError(String::from(
                    "Error registering the Graph with the Leader.",
                ))
            })?;

        Ok(job_graph_id)
    }

    /// Submits the [`Graph`] to the `Leader` for execution.
    /// 
    /// This method automatically invokes the [`register`] method.
    pub fn submit(&self, graph: Graph) -> Result<JobGraphId, HandleError> {
        // Compile the JobGraph and register it with the Worker.
        let job_graph_id = self.register(graph)?;

        // Submit the JobGraph to the Leader.
        self.worker_handle
            .blocking_send(DriverNotification::SubmitGraph(job_graph_id.clone()))
            .map_err(|_| {
                HandleError::CommunicationError(String::from(
                    "Error submitting the Graph to the Leader.",
                ))
            })?;

        Ok(job_graph_id)
    }

    /// Retrieve the ID of the Worker underlying this handle.
    pub fn id(&self) -> WorkerId {
        self.handle_id
    }
}