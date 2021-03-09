use std::{
    cell::RefCell,
    collections::HashMap,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures::future;
use tokio::{
    self,
    stream::{Stream, StreamExt},
    sync::{mpsc, watch},
};

use crate::{
    communication::{ControlMessage, RecvEndpoint},
    dataflow::{
        operator::{Operator, OperatorConfig, Source},
        stream::{InternalReadStream, StreamId},
        Data, EventMakerT, Message, ReadStream, State, WriteStream,
    },
    node::lattice::ExecutionLattice,
    node::operator_event::OperatorEvent,
};

#[derive(Clone, Debug, PartialEq)]
enum EventRunnerMessage {
    AddedEvents,
    DestroyOperator,
}

pub(crate) struct OperatorExecutor<O, S, T, U, V, W>
where
    S: State,
    T: Data,
    U: Data,
    V: Data,
    W: Data,
{
    operator: O,
    state: S,
    left_read_stream: Option<ReadStream<T>>,
    right_read_stream: Option<ReadStream<U>>,
    left_write_stream: Option<WriteStream<V>>,
    right_write_stream: Option<WriteStream<W>>,
}

impl<O, S, T, U, V, W> OperatorExecutor<O, S, T, U, V, W>
where
    O: Operator<S, T, U, V, W>,
    S: State,
    T: Data,
    U: Data,
    V: Data,
    W: Data,
{
    pub fn new() -> Self {
        // Retrieve streams, set up state.
        unimplemented!()
    }

    pub async fn execute(&mut self) {
        // Wait for execute signal.
        // Spawn tasks.
        // Call operator.run()
        // Await messages.
        // Insert messages into lattice.
        unimplemented!()
    }
}
