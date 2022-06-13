use serde::{Deserialize, Serialize};

use crate::node::NodeId;

/// Notifications sent to the head node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeadNotification {
    /// Notifies the head that the specified node has finished setting up.
    Ready(NodeId),
}

/// Notifications sent to worker nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerNotification {
    /// Notifies the worker to shut down.
    Shutdown,
}
