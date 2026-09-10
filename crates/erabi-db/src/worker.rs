use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

type DbCallResult<T, E> = Result<Result<T, E>, WorkerFailure>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerFailure {
    ShuttingDown,
    Panicked,
}

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("worker capacity {capacity} is outside 1..={max}")]
    InvalidCapacity { capacity: usize, max: usize },
    #[error("failed to spawn database worker thread: {0}")]
    ThreadSpawn(#[source] std::io::Error),
    #[error("failed to open worker SQLite connection: {0}")]
    ConnectionOpen(#[source] rusqlite::Error),
    #[error("worker initializer failed: {0}")]
    Initializer(#[source] rusqlite::Error),
    #[error("worker initializer panicked")]
    InitializerPanicked,
    #[error("worker shared state was poisoned during startup")]
    SharedStatePoisoned,
    #[error("worker exited before publishing readiness")]
    ReadinessClosed,
}

#[derive(Debug, thiserror::Error)]
enum ShutdownError {
    #[error("worker completion was not observable")]
    CompletionLost,
    #[error("worker join failed")]
    JoinFailed,
    #[error("worker entered the terminal panic state")]
    Panicked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Lifecycle {
    Starting,
    Running,
    Closing,
    Closed,
    Panicked,
}

struct SharedState {
    lifecycle: Lifecycle,
    queue: VecDeque<Box<dyn ErasedJob>>,
}

struct WorkerShared {
    state: Mutex<SharedState>,
    wake: Condvar,
    // A permit is held from admission through execution, so this bounds
    // accepted outstanding work rather than only the FIFO queue length.
    capacity: Arc<Semaphore>,
    #[cfg(test)]
    test_hooks: Mutex<Option<Arc<TestHooks>>>,
}

impl WorkerShared {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(SharedState {
                lifecycle: Lifecycle::Starting,
                queue: VecDeque::new(),
            }),
            wake: Condvar::new(),
            capacity: Arc::new(Semaphore::new(capacity)),
            #[cfg(test)]
            test_hooks: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn install_test_hooks(&self, hooks: TestHooks) {
        let mut installed = recover_lock(&self.test_hooks);
        *installed = Some(Arc::new(hooks));
    }

    #[cfg(test)]
    fn test_hooks(&self) -> Option<Arc<TestHooks>> {
        recover_lock(&self.test_hooks).clone()
    }
}

trait ErasedJob: Send {
    fn execute(&mut self, connection: &mut rusqlite::Connection) -> JobExecution;

    fn fail(&mut self, failure: WorkerFailure);
}

enum JobExecution {
    Completed,
    Panicked,
}

struct JobEnvelope<T, E, F> {
    operation: Option<F>,
    responder: Option<oneshot::Sender<DbCallResult<T, E>>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<T, E, F> ErasedJob for JobEnvelope<T, E, F>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnOnce(&mut rusqlite::Connection) -> Result<T, E> + Send + 'static,
{
    fn execute(&mut self, connection: &mut rusqlite::Connection) -> JobExecution {
        let Some(operation) = self.operation.take() else {
            return JobExecution::Panicked;
        };

        let result = catch_unwind(AssertUnwindSafe(|| operation(connection)));
        match result {
            Ok(result) => {
                if let Some(responder) = self.responder.take() {
                    let _ = responder.send(Ok(result));
                }
                self.permit.take();
                JobExecution::Completed
            }
            Err(_) => JobExecution::Panicked,
        }
    }

    fn fail(&mut self, failure: WorkerFailure) {
        self.operation.take();
        self.permit.take();
        if let Some(responder) = self.responder.take() {
            let _ = responder.send(Err(failure));
        }
    }
}

#[derive(Clone)]
struct DbWorkerHandle {
    shared: Arc<WorkerShared>,
}

impl DbWorkerHandle {
    async fn call<T, E, F>(&self, operation: F) -> DbCallResult<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> Result<T, E> + Send + 'static,
    {
        let (responder, response) = oneshot::channel();

        #[cfg(test)]
        if let Some(hooks) = self.shared.test_hooks() {
            hooks.before_acquire();
        }

        let Ok(permit) = Arc::clone(&self.shared.capacity).acquire_owned().await else {
            return Err(observed_failure(&self.shared));
        };

        // There is intentionally no await between acquiring this permit and
        // the final admission attempt below. A permit alone is not acceptance.
        #[cfg(test)]
        if let Some(hooks) = self.shared.test_hooks() {
            hooks.after_permit();
        }

        {
            let mut state = match self.shared.state.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    let queued = terminalize_locked(&self.shared, poisoned.into_inner());
                    fail_jobs(queued, WorkerFailure::Panicked);
                    drop(permit);
                    return Err(WorkerFailure::Panicked);
                }
            };

            if state.lifecycle != Lifecycle::Running {
                let failure = failure_for(state.lifecycle);
                drop(state);
                drop(permit);
                return Err(failure);
            }

            state.queue.push_back(Box::new(JobEnvelope {
                operation: Some(operation),
                responder: Some(responder),
                permit: Some(permit),
            }));
            // This queue push while lifecycle == Running is the admission
            // linearization point protected by the same mutex as lifecycle.
            #[cfg(test)]
            if let Some(hooks) = self.shared.test_hooks() {
                hooks.accepted();
            }
            self.shared.wake.notify_one();
        }

        match response.await {
            Ok(result) => result,
            Err(_) => Err(WorkerFailure::Panicked),
        }
    }

    fn begin_shutdown(&self) {
        begin_shutdown(&self.shared);
    }
}

struct DbWorker {
    handle: DbWorkerHandle,
    join: Option<thread::JoinHandle<()>>,
    completion: Option<oneshot::Receiver<Lifecycle>>,
    shutdown_completed: bool,
}

impl DbWorker {
    fn start<F>(capacity: usize, initializer: F) -> Result<Self, StartupError>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<(), rusqlite::Error> + Send + 'static,
    {
        if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
            return Err(StartupError::InvalidCapacity {
                capacity,
                max: Semaphore::MAX_PERMITS,
            });
        }

        let shared = Arc::new(WorkerShared::new(capacity));
        let worker_shared = Arc::clone(&shared);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let (completion_sender, completion_receiver) = oneshot::channel();

        let join = thread::Builder::new()
            .name("erabi-db-worker".to_owned())
            .spawn(move || {
                worker_entry(
                    &worker_shared,
                    initializer,
                    &ready_sender,
                    completion_sender,
                );
            })
            .map_err(StartupError::ThreadSpawn)?;

        match ready_receiver.recv() {
            Ok(Ok(())) => Ok(Self {
                handle: DbWorkerHandle { shared },
                join: Some(join),
                completion: Some(completion_receiver),
                shutdown_completed: false,
            }),
            Ok(Err(error)) => {
                let _ = join.join();
                Err(error)
            }
            Err(_) => {
                let _ = join.join();
                Err(StartupError::ReadinessClosed)
            }
        }
    }

    fn handle(&self) -> DbWorkerHandle {
        self.handle.clone()
    }

    #[cfg(test)]
    fn install_test_hooks(&self, hooks: TestHooks) {
        self.handle.shared.install_test_hooks(hooks);
    }

    async fn shutdown(mut self) -> Result<(), ShutdownError> {
        self.handle.begin_shutdown();

        let Some(completion) = self.completion.take() else {
            return Err(ShutdownError::CompletionLost);
        };
        let lifecycle = completion
            .await
            .map_err(|_| ShutdownError::CompletionLost)?;

        let Some(join) = self.join.take() else {
            return Err(ShutdownError::JoinFailed);
        };

        let joined = tokio::task::spawn_blocking(move || join.join()).await;
        self.shutdown_completed = true;

        match joined {
            Ok(Ok(())) => match lifecycle {
                Lifecycle::Closed => Ok(()),
                Lifecycle::Panicked => Err(ShutdownError::Panicked),
                Lifecycle::Starting | Lifecycle::Running | Lifecycle::Closing => {
                    Err(ShutdownError::JoinFailed)
                }
            },
            Ok(Err(_)) | Err(_) => Err(ShutdownError::JoinFailed),
        }
    }
}

impl Drop for DbWorker {
    fn drop(&mut self) {
        if !self.shutdown_completed {
            // The owner deliberately detaches an unfinished OS thread. Closing
            // admission first guarantees an idle worker is woken and that the
            // worker naturally terminates after accepted work drains.
            self.handle.begin_shutdown();
        }
    }
}

fn worker_entry<F>(
    shared: &WorkerShared,
    initializer: F,
    ready_sender: &std::sync::mpsc::SyncSender<Result<(), StartupError>>,
    completion_sender: oneshot::Sender<Lifecycle>,
) where
    F: FnOnce(&mut rusqlite::Connection) -> Result<(), rusqlite::Error> + Send + 'static,
{
    let mut connection = match rusqlite::Connection::open_in_memory() {
        Ok(connection) => connection,
        Err(error) => {
            let _ = ready_sender.send(Err(StartupError::ConnectionOpen(error)));
            return;
        }
    };

    let initialization_result = catch_unwind(AssertUnwindSafe(|| initializer(&mut connection)));
    match initialization_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = ready_sender.send(Err(StartupError::Initializer(error)));
            return;
        }
        Err(_) => {
            let _ = ready_sender.send(Err(StartupError::InitializerPanicked));
            return;
        }
    }

    if mark_running(shared).is_err() {
        let _ = ready_sender.send(Err(StartupError::SharedStatePoisoned));
        return;
    }
    if ready_sender.send(Ok(())).is_err() {
        begin_shutdown(shared);
    }

    let mut current_job: Option<Box<dyn ErasedJob>> = None;
    let worker_result = catch_unwind(AssertUnwindSafe(|| {
        worker_loop(shared, &mut connection, &mut current_job)
    }));
    let lifecycle = if let Ok(lifecycle) = worker_result {
        lifecycle
    } else {
        let queued = transition_to_panicked(shared);
        if let Some(job) = current_job.as_mut() {
            job.fail(WorkerFailure::Panicked);
        }
        let current = current_job.take();
        drop(current);
        fail_jobs(queued, WorkerFailure::Panicked);
        Lifecycle::Panicked
    };

    drop(connection);
    // Completion means the terminal lifecycle is set and the connection is
    // gone; the OS thread is still owned and joined separately by DbWorker.
    let _ = completion_sender.send(lifecycle);

    #[cfg(test)]
    if let Some(hooks) = shared.test_hooks() {
        hooks.after_completion();
    }
}

fn mark_running(shared: &WorkerShared) -> Result<(), StartupError> {
    let mut state = shared
        .state
        .lock()
        .map_err(|_| StartupError::SharedStatePoisoned)?;
    state.lifecycle = Lifecycle::Running;
    Ok(())
}

fn worker_loop(
    shared: &WorkerShared,
    connection: &mut rusqlite::Connection,
    current_job: &mut Option<Box<dyn ErasedJob>>,
) -> Lifecycle {
    loop {
        let mut state = match shared.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let queued = terminalize_locked(shared, poisoned.into_inner());
                fail_jobs(queued, WorkerFailure::Panicked);
                return Lifecycle::Panicked;
            }
        };

        loop {
            #[cfg(test)]
            if !state.queue.is_empty() {
                let gate = shared.test_hooks().and_then(|hooks| hooks.before_dequeue());
                if let Some(gate) = gate {
                    drop(state);
                    let blocked = gate.block_worker_once();
                    state = match shared.state.lock() {
                        Ok(state) => state,
                        Err(poisoned) => {
                            let queued = terminalize_locked(shared, poisoned.into_inner());
                            fail_jobs(queued, WorkerFailure::Panicked);
                            return Lifecycle::Panicked;
                        }
                    };
                    if blocked {
                        continue;
                    }
                }
            }

            if let Some(job) = state.queue.pop_front() {
                *current_job = Some(job);
                break;
            }

            match state.lifecycle {
                Lifecycle::Running | Lifecycle::Starting => {
                    state = match shared.wake.wait(state) {
                        Ok(state) => state,
                        Err(poisoned) => {
                            let queued = terminalize_locked(shared, poisoned.into_inner());
                            fail_jobs(queued, WorkerFailure::Panicked);
                            return Lifecycle::Panicked;
                        }
                    };
                }
                Lifecycle::Closing => {
                    // Only lifecycle plus accepted queue state controls
                    // termination; unaccepted semaphore permits are ignored.
                    state.lifecycle = Lifecycle::Closed;
                    shared.wake.notify_all();
                    drop(state);
                    return Lifecycle::Closed;
                }
                Lifecycle::Closed => {
                    drop(state);
                    return Lifecycle::Closed;
                }
                Lifecycle::Panicked => {
                    let queued = std::mem::take(&mut state.queue);
                    drop(state);
                    fail_jobs(queued, WorkerFailure::Panicked);
                    return Lifecycle::Panicked;
                }
            }
        }
        drop(state);

        #[cfg(test)]
        if let Some(hooks) = shared.test_hooks() {
            hooks.after_dequeue();
        }

        let execution = current_job.as_mut().map(|job| job.execute(connection));
        match execution {
            Some(JobExecution::Completed) => {
                let completed = current_job.take();
                drop(completed);
            }
            Some(JobExecution::Panicked) => {
                let queued = transition_to_panicked(shared);
                if let Some(job) = current_job.as_mut() {
                    job.fail(WorkerFailure::Panicked);
                }
                let current = current_job.take();
                drop(current);
                fail_jobs(queued, WorkerFailure::Panicked);
                return Lifecycle::Panicked;
            }
            None => return Lifecycle::Panicked,
        }
    }
}

fn begin_shutdown(shared: &WorkerShared) {
    let queued = match shared.state.lock() {
        Ok(mut state) => {
            if state.lifecycle == Lifecycle::Running {
                state.lifecycle = Lifecycle::Closing;
            }
            shared.capacity.close();
            shared.wake.notify_all();
            None
        }
        Err(poisoned) => Some(terminalize_locked(shared, poisoned.into_inner())),
    };

    if let Some(queued) = queued {
        fail_jobs(queued, WorkerFailure::Panicked);
    }
}

fn transition_to_panicked(shared: &WorkerShared) -> VecDeque<Box<dyn ErasedJob>> {
    let state = match shared.state.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    };
    terminalize_locked(shared, state)
}

fn terminalize_locked(
    shared: &WorkerShared,
    mut state: MutexGuard<'_, SharedState>,
) -> VecDeque<Box<dyn ErasedJob>> {
    state.lifecycle = Lifecycle::Panicked;
    shared.capacity.close();
    let queued = std::mem::take(&mut state.queue);
    shared.wake.notify_all();
    drop(state);
    queued
}

fn fail_jobs<I>(jobs: I, failure: WorkerFailure)
where
    I: IntoIterator<Item = Box<dyn ErasedJob>>,
{
    for mut job in jobs {
        job.fail(failure);
    }
}

fn observed_failure(shared: &WorkerShared) -> WorkerFailure {
    match shared.state.lock() {
        Ok(state) => failure_for(state.lifecycle),
        Err(poisoned) => {
            let queued = terminalize_locked(shared, poisoned.into_inner());
            fail_jobs(queued, WorkerFailure::Panicked);
            WorkerFailure::Panicked
        }
    }
}

const fn failure_for(lifecycle: Lifecycle) -> WorkerFailure {
    match lifecycle {
        Lifecycle::Panicked => WorkerFailure::Panicked,
        Lifecycle::Starting | Lifecycle::Running | Lifecycle::Closing | Lifecycle::Closed => {
            WorkerFailure::ShuttingDown
        }
    }
}

#[cfg(test)]
fn recover_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
struct TestHooks {
    before_acquire: Option<std::sync::mpsc::Sender<()>>,
    accepted: Option<Arc<TestAdmission>>,
    after_permit: Option<Arc<TestGate>>,
    before_dequeue: Option<Arc<TestGate>>,
    after_dequeue: Option<Arc<TestGate>>,
    panic_after_dequeue: std::sync::atomic::AtomicBool,
    after_completion: Option<Arc<TestGate>>,
}

#[cfg(test)]
impl TestHooks {
    fn new(
        before_acquire: Option<std::sync::mpsc::Sender<()>>,
        after_permit: Option<Arc<TestGate>>,
        after_dequeue: Option<Arc<TestGate>>,
        panic_after_dequeue: bool,
        after_completion: Option<Arc<TestGate>>,
    ) -> Self {
        Self {
            before_acquire,
            accepted: None,
            after_permit,
            before_dequeue: None,
            after_dequeue,
            panic_after_dequeue: std::sync::atomic::AtomicBool::new(panic_after_dequeue),
            after_completion,
        }
    }

    fn before_acquire(&self) {
        if let Some(sender) = &self.before_acquire {
            let _ = sender.send(());
        }
    }

    fn accepted(&self) {
        if let Some(admission) = &self.accepted {
            admission.record();
        }
    }

    fn before_dequeue(&self) -> Option<Arc<TestGate>> {
        self.before_dequeue.clone()
    }

    fn after_permit(&self) {
        if let Some(gate) = &self.after_permit {
            gate.block_worker();
        }
    }

    fn after_dequeue(&self) {
        if let Some(gate) = &self.after_dequeue {
            gate.block_worker();
        }
        if self
            .panic_after_dequeue
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("injected worker infrastructure panic");
        }
    }

    fn after_completion(&self) {
        if let Some(gate) = &self.after_completion {
            gate.block_worker();
        }
    }
}

#[cfg(test)]
struct TestAdmission {
    state: Mutex<usize>,
    wake: Condvar,
}

#[cfg(test)]
impl TestAdmission {
    fn new() -> Self {
        Self {
            state: Mutex::new(0),
            wake: Condvar::new(),
        }
    }

    fn record(&self) {
        let mut count = recover_lock(&self.state);
        *count += 1;
        self.wake.notify_all();
    }

    fn wait_for(&self, expected: usize) {
        let mut count = recover_lock(&self.state);
        while *count < expected {
            count = match self.wake.wait(count) {
                Ok(count) => count,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }
}

#[cfg(test)]
struct TestGate {
    state: Mutex<TestGateState>,
    wake: Condvar,
}

#[cfg(test)]
struct TestGateState {
    reached: bool,
    released: bool,
}

#[cfg(test)]
impl TestGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(TestGateState {
                reached: false,
                released: false,
            }),
            wake: Condvar::new(),
        }
    }

    fn block_worker(&self) {
        let mut state = recover_lock(&self.state);
        state.reached = true;
        self.wake.notify_all();
        while !state.released {
            state = match self.wake.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    fn block_worker_once(&self) -> bool {
        let mut state = recover_lock(&self.state);
        if state.released || state.reached {
            return false;
        }
        state.reached = true;
        self.wake.notify_all();
        while !state.released {
            state = match self.wake.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        true
    }

    fn wait_until_reached(&self) {
        let mut state = recover_lock(&self.state);
        while !state.reached {
            state = match self.wake.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    fn release(&self) {
        let mut state = recover_lock(&self.state);
        state.released = true;
        self.wake.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, mpsc},
        thread,
    };

    use tokio::sync::Semaphore;

    use super::{
        DbCallResult, DbWorker, Lifecycle, StartupError, TestAdmission, TestGate, TestHooks,
        WorkerFailure,
    };

    fn worker(capacity: usize) -> DbWorker {
        match DbWorker::start(capacity, |_| Ok(())) {
            Ok(worker) => worker,
            Err(error) => panic!("worker should start: {error:?}"),
        }
    }

    fn install_hooks(worker: &DbWorker, hooks: TestHooks) {
        worker.install_test_hooks(hooks);
    }

    async fn successful_call(
        handle: &super::DbWorkerHandle,
        value: i64,
    ) -> DbCallResult<i64, rusqlite::Error> {
        handle.call(move |_| Ok(value)).await
    }

    async fn assert_shutdown(worker: DbWorker) {
        assert!(worker.shutdown().await.is_ok());
    }

    async fn wait_for_gate(gate: Arc<TestGate>) {
        let result = tokio::task::spawn_blocking(move || gate.wait_until_reached()).await;
        assert!(result.is_ok());
    }

    async fn wait_for_admission(admission: Arc<TestAdmission>, expected: usize) {
        let result = tokio::task::spawn_blocking(move || admission.wait_for(expected)).await;
        assert!(result.is_ok());
    }

    #[test]
    fn zero_capacity_is_rejected_before_semaphore_construction() {
        let result = DbWorker::start(0, |_| Ok(()));

        assert!(matches!(
            result,
            Err(StartupError::InvalidCapacity { capacity: 0, .. })
        ));
    }

    #[test]
    fn capacity_above_semaphore_limit_is_rejected_without_panic() {
        let result = std::panic::catch_unwind(|| {
            DbWorker::start(Semaphore::MAX_PERMITS.saturating_add(1), |_| Ok(()))
        });

        assert!(matches!(
            result,
            Ok(Err(StartupError::InvalidCapacity { .. }))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_one_applies_backpressure_to_second_unaccepted_call() {
        let (acquire_sender, acquire_receiver) = mpsc::channel();
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(
                Some(acquire_sender),
                None,
                Some(Arc::clone(&gate)),
                false,
                None,
            ),
        );
        let handle = worker.handle();
        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.call(|_| Ok::<_, rusqlite::Error>(11_i64)).await }
        });
        assert!(acquire_receiver.recv().is_ok());
        wait_for_gate(Arc::clone(&gate)).await;

        let second = tokio::spawn({
            let handle = handle.clone();
            async move { successful_call(&handle, 22).await }
        });
        assert!(acquire_receiver.recv().is_ok());
        assert!(!second.is_finished());

        gate.release();
        assert!(matches!(first.await, Ok(Ok(Ok(11)))));
        assert!(matches!(second.await, Ok(Ok(Ok(22)))));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn begin_shutdown_closes_semaphore_and_wakes_capacity_waiter() {
        let (started_sender, started_receiver) = mpsc::channel();
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(
                Some(started_sender),
                None,
                Some(Arc::clone(&gate)),
                false,
                None,
            ),
        );
        let handle = worker.handle();
        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.call(|_| Ok::<_, rusqlite::Error>(1_i64)).await }
        });
        assert!(started_receiver.recv().is_ok());
        wait_for_gate(Arc::clone(&gate)).await;
        let second = tokio::spawn({
            let handle = handle.clone();
            async move { successful_call(&handle, 2).await }
        });
        assert!(started_receiver.recv().is_ok());

        handle.begin_shutdown();
        assert!(matches!(second.await, Ok(Err(WorkerFailure::ShuttingDown))));
        gate.release();
        assert!(matches!(first.await, Ok(Ok(Ok(1)))));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_waiter_returns_shutting_down_on_normal_closing() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, Some(Arc::clone(&gate)), None, false, None),
        );
        let handle = worker.handle();
        let handle_for_thread = handle.clone();
        let caller = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .map(|runtime| runtime.block_on(successful_call(&handle_for_thread, 1)))
        });
        wait_for_gate(Arc::clone(&gate)).await;
        handle.begin_shutdown();
        gate.release();
        let result = match caller.join() {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => panic!("caller runtime failed: {error}"),
            Err(error) => panic!("caller thread panicked: {error:?}"),
        };
        assert!(matches!(result, Err(WorkerFailure::ShuttingDown)));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unaccepted_permit_does_not_prevent_worker_termination() {
        let gate = Arc::new(TestGate::new());
        let mut worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, Some(Arc::clone(&gate)), None, false, None),
        );
        let handle = worker.handle();
        let handle_for_thread = handle.clone();
        let caller = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .map(|runtime| runtime.block_on(successful_call(&handle_for_thread, 1)))
        });
        wait_for_gate(Arc::clone(&gate)).await;
        handle.begin_shutdown();
        let completion = match worker.completion.as_mut() {
            Some(receiver) => receiver.await,
            None => panic!("completion receiver missing"),
        };
        assert!(matches!(completion, Ok(Lifecycle::Closed)));
        gate.release();
        let result = match caller.join() {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => panic!("caller runtime failed: {error}"),
            Err(error) => panic!("caller thread panicked: {error:?}"),
        };
        assert!(matches!(result, Err(WorkerFailure::ShuttingDown)));
        drop(worker);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn permit_acquired_before_closing_cannot_enqueue_after_closing_wins_lock() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, Some(Arc::clone(&gate)), None, false, None),
        );
        let handle = worker.handle();
        let handle_for_thread = handle.clone();
        let caller = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .map(|runtime| runtime.block_on(successful_call(&handle_for_thread, 9)))
        });
        wait_for_gate(Arc::clone(&gate)).await;
        handle.begin_shutdown();
        gate.release();
        let result = match caller.join() {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => panic!("caller runtime failed: {error}"),
            Err(error) => panic!("caller thread panicked: {error:?}"),
        };
        assert!(matches!(result, Err(WorkerFailure::ShuttingDown)));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enqueue_before_closing_linearization_is_accepted_and_drains() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, None, Some(Arc::clone(&gate)), false, None),
        );
        let handle = worker.handle();
        let call = tokio::spawn({
            let handle = handle.clone();
            async move { successful_call(&handle, 17).await }
        });
        wait_for_gate(Arc::clone(&gate)).await;
        handle.begin_shutdown();
        gate.release();
        assert!(matches!(call.await, Ok(Ok(Ok(17)))));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepted_jobs_execute_fifo() {
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gate = Arc::new(TestGate::new());
        let admission = Arc::new(TestAdmission::new());
        let worker = worker(2);
        let mut hooks = TestHooks::new(None, None, None, false, None);
        hooks.accepted = Some(Arc::clone(&admission));
        hooks.before_dequeue = Some(Arc::clone(&gate));
        worker.install_test_hooks(hooks);
        let handle = worker.handle();
        let second = {
            let order = Arc::clone(&order);
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .call(move |_| {
                        match order.lock() {
                            Ok(mut order) => order.push(2),
                            Err(poisoned) => poisoned.into_inner().push(2),
                        }
                        Ok::<_, rusqlite::Error>(2_i64)
                    })
                    .await
            })
        };
        wait_for_admission(Arc::clone(&admission), 1).await;
        wait_for_gate(Arc::clone(&gate)).await;

        let third = {
            let order = Arc::clone(&order);
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .call(move |_| {
                        match order.lock() {
                            Ok(mut order) => order.push(3),
                            Err(poisoned) => poisoned.into_inner().push(3),
                        }
                        Ok::<_, rusqlite::Error>(3_i64)
                    })
                    .await
            })
        };
        wait_for_admission(Arc::clone(&admission), 2).await;
        gate.release();
        assert!(matches!(second.await, Ok(Ok(Ok(2)))));
        assert!(matches!(third.await, Ok(Ok(Ok(3)))));
        let observed = match order.lock() {
            Ok(order) => order.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        assert_eq!(observed, vec![2, 3]);
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ordinary_operation_error_does_not_poison_worker() {
        let worker = worker(1);
        let handle = worker.handle();
        let error = handle
            .call(|_| Err::<i64, _>("operation failed".to_owned()))
            .await;
        assert!(matches!(error, Ok(Err(message)) if message == "operation failed"));
        assert!(matches!(successful_call(&handle, 3).await, Ok(Ok(3))));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_operation_returns_nested_success() {
        let worker = worker(1);
        assert!(matches!(
            successful_call(&worker.handle(), 42).await,
            Ok(Ok(42))
        ));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_response_receiver_does_not_cancel_accepted_execution() {
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, None, Some(Arc::clone(&gate)), false, None),
        );
        let handle = worker.handle();
        let call = tokio::spawn({
            let completed = Arc::clone(&completed);
            async move {
                handle
                    .call(move |_| {
                        completed.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok::<_, rusqlite::Error>(())
                    })
                    .await
            }
        });
        wait_for_gate(Arc::clone(&gate)).await;
        call.abort();
        let _ = call.await;
        gate.release();
        let probe = worker.handle();
        assert!(matches!(successful_call(&probe, 1).await, Ok(Ok(1))));
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submitted_operation_panic_makes_worker_terminal() {
        let worker = worker(1);
        let _result = worker
            .handle()
            .call::<(), (), _>(|_| panic!("hidden panic"))
            .await;
        assert!(matches!(
            worker
                .handle()
                .call::<i64, rusqlite::Error, _>(|_| Ok(1))
                .await,
            Err(WorkerFailure::Panicked)
        ));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn current_panicking_caller_receives_exactly_panicked() {
        let worker = worker(1);
        let result = worker
            .handle()
            .call::<(), (), _>(|_| panic!("hidden panic"))
            .await;
        assert!(matches!(result, Err(WorkerFailure::Panicked)));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_callers_receive_panicked_after_current_operation_panic() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(2);
        install_hooks(
            &worker,
            TestHooks::new(None, None, Some(Arc::clone(&gate)), false, None),
        );
        let handle = worker.handle();
        let current = tokio::spawn({
            let handle = handle.clone();
            async move { handle.call::<(), (), _>(|_| panic!("hidden panic")).await }
        });
        wait_for_gate(Arc::clone(&gate)).await;
        let queued = tokio::spawn({
            let handle = handle.clone();
            async move { successful_call(&handle, 2).await }
        });
        gate.release();
        assert!(matches!(current.await, Ok(Err(WorkerFailure::Panicked))));
        assert!(matches!(queued.await, Ok(Err(WorkerFailure::Panicked))));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_waiters_wake_with_panicked_after_worker_panic() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, None, Some(Arc::clone(&gate)), false, None),
        );
        let handle = worker.handle();
        let current = tokio::spawn({
            let handle = handle.clone();
            async move { handle.call::<(), (), _>(|_| panic!("hidden panic")).await }
        });
        wait_for_gate(Arc::clone(&gate)).await;
        let waiting = tokio::spawn({
            let handle = handle.clone();
            async move { successful_call(&handle, 2).await }
        });
        gate.release();
        assert!(matches!(current.await, Ok(Err(WorkerFailure::Panicked))));
        assert!(matches!(waiting.await, Ok(Err(WorkerFailure::Panicked))));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn future_calls_receive_panicked_after_worker_panic() {
        let worker = worker(1);
        let current = worker
            .handle()
            .call::<(), (), _>(|_| panic!("hidden panic"))
            .await;
        assert!(matches!(current, Err(WorkerFailure::Panicked)));
        let future = worker
            .handle()
            .call::<i64, rusqlite::Error, _>(|_| Ok(3))
            .await;
        assert!(matches!(future, Err(WorkerFailure::Panicked)));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[test]
    fn initialization_succeeds_before_handle_becomes_usable() {
        let worker = match DbWorker::start(1, |connection| {
            connection.execute_batch("CREATE TABLE ready (value INTEGER NOT NULL)")
        }) {
            Ok(worker) => worker,
            Err(error) => panic!("initialization should succeed: {error:?}"),
        };
        let handle = worker.handle();
        let result = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .map(|runtime| {
                    runtime.block_on(handle.call(|connection| {
                        connection.query_row("SELECT COUNT(*) FROM ready", [], |row| row.get(0))
                    }))
                })
        });
        let result = match result.join() {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => panic!("runtime failed: {error}"),
            Err(error) => panic!("caller panicked: {error:?}"),
        };
        assert!(matches!(result, Ok(Ok(0))));
        drop(worker);
    }

    #[test]
    fn initialization_error_returns_without_usable_handle() {
        let result = DbWorker::start(1, |connection| connection.execute_batch("not valid SQL"));

        assert!(matches!(result, Err(StartupError::Initializer(_))));
    }

    #[test]
    fn initialization_panic_returns_without_stranding_os_thread() {
        let result = DbWorker::start(1, |_| -> Result<(), rusqlite::Error> {
            panic!("initializer panic")
        });

        assert!(matches!(result, Err(StartupError::InitializerPanicked)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outer_worker_panic_after_dequeue_fails_current_caller() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, None, Some(Arc::clone(&gate)), true, None),
        );
        let handle = worker.handle();
        let call = tokio::spawn(async move { successful_call(&handle, 1).await });
        wait_for_gate(Arc::clone(&gate)).await;
        gate.release();
        assert!(matches!(call.await, Ok(Err(WorkerFailure::Panicked))));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outer_worker_panic_fails_queued_callers() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(2);
        install_hooks(
            &worker,
            TestHooks::new(None, None, Some(Arc::clone(&gate)), true, None),
        );
        let handle = worker.handle();
        let current = tokio::spawn({
            let handle = handle.clone();
            async move { successful_call(&handle, 1).await }
        });
        wait_for_gate(Arc::clone(&gate)).await;
        let queued = tokio::spawn({
            let handle = handle.clone();
            async move { successful_call(&handle, 2).await }
        });
        gate.release();
        assert!(matches!(current.await, Ok(Err(WorkerFailure::Panicked))));
        assert!(matches!(queued.await, Ok(Err(WorkerFailure::Panicked))));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poisoned_shared_state_becomes_terminal_panicked() {
        let worker = worker(1);
        let state = Arc::clone(&worker.handle.shared);
        let poison = thread::spawn(move || {
            let _guard = match state.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            panic!("poison shared state");
        });
        assert!(poison.join().is_err());
        let result = successful_call(&worker.handle(), 1).await;
        assert!(matches!(result, Err(WorkerFailure::Panicked)));
        assert!(matches!(
            worker.shutdown().await,
            Err(super::ShutdownError::Panicked)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_begin_shutdown_is_idempotent() {
        let worker = worker(1);
        let handle = worker.handle();
        handle.begin_shutdown();
        handle.begin_shutdown();
        assert!(matches!(
            successful_call(&handle, 1).await,
            Err(WorkerFailure::ShuttingDown)
        ));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completion_is_distinguishable_from_actual_thread_join() {
        let gate = Arc::new(TestGate::new());
        let mut worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, None, None, false, Some(Arc::clone(&gate))),
        );
        let handle = worker.handle();
        let join_is_finished = worker.join.as_ref().map(thread::JoinHandle::is_finished);
        assert_eq!(join_is_finished, Some(false));
        handle.begin_shutdown();
        wait_for_gate(Arc::clone(&gate)).await;
        let completion = match worker.completion.as_mut() {
            Some(receiver) => receiver.await,
            None => panic!("completion receiver missing"),
        };
        assert!(completion.is_ok());
        assert_eq!(
            worker.join.as_ref().map(thread::JoinHandle::is_finished),
            Some(false)
        );
        gate.release();
        let Some(join) = worker.join.take() else {
            panic!("join handle missing");
        };
        let joined = tokio::task::spawn_blocking(move || join.join()).await;
        assert!(matches!(joined, Ok(Ok(()))));
        worker.shutdown_completed = true;
        drop(worker);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_shutdown_joins_without_blocking_tokio_worker() {
        let worker = worker(1);
        let marker = tokio::task::spawn_blocking(|| 7_i32).await;
        assert!(matches!(marker, Ok(7)));
        assert!(worker.shutdown().await.is_ok());
    }

    #[test]
    fn dropping_owner_wakes_idle_worker_without_joining_synchronously() {
        let gate = Arc::new(TestGate::new());
        let worker = worker(1);
        install_hooks(
            &worker,
            TestHooks::new(None, None, None, false, Some(Arc::clone(&gate))),
        );
        drop(worker);
        gate.wait_until_reached();
        gate.release();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cloned_handles_share_admission_and_lifecycle_state() {
        let worker = worker(1);
        let first = worker.handle();
        let second = first.clone();
        first.begin_shutdown();
        assert!(matches!(
            successful_call(&second, 1).await,
            Err(WorkerFailure::ShuttingDown)
        ));
        assert_shutdown(worker).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn database_worker_thread_has_no_tokio_runtime() {
        let worker = worker(1);
        let result = worker
            .handle()
            .call(|_| {
                assert!(tokio::runtime::Handle::try_current().is_err());
                Ok::<_, rusqlite::Error>(())
            })
            .await;
        assert!(matches!(result, Ok(Ok(()))));
        assert_shutdown(worker).await;
    }
}
