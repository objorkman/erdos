use serde::{Deserialize, Serialize};

use crate::node::{leader_node::WorkerId};

/// [`ControlPlaneNotifcation`] defines the type of notifications communicated between the leader and the workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlPlaneNotification {
    Ready(WorkerId),
    Testing,
}
