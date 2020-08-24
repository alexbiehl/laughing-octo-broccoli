use super::protocol;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub struct Metrics {
    total_queued_tickets: AtomicUsize,
    total_redeems: AtomicUsize,
    total_expired_tickets: AtomicUsize,
    total_successful_tasks: AtomicUsize,
    total_failed_tasks: AtomicUsize,
    total_found_empty_queue: AtomicUsize,
}

#[repr(transparent)]
#[derive(Clone)]
pub struct SampledMetrics(protocol::StatusUpdateResponse);

impl SampledMetrics {
    pub fn from_status_update_response(status_update: protocol::StatusUpdateResponse) -> Self {
        SampledMetrics(status_update)
    }

    pub fn new() -> Self {
        SampledMetrics(protocol::StatusUpdateResponse {
            total_queued_tickets: 0,
            total_redeems: 0,
            total_expired_tickets: 0,
            total_successful_tasks: 0,
            total_failed_tasks: 0,
            total_found_empty_queue: 0,
        })
    }
}

impl slog::Value for SampledMetrics {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str(key, &format!("total_queued: {} total_redeems: {} total_expired_tickets: {} total_successful_tasks: {} total_failed_tasks: {} total_found_empty_queue: {}", self.0.total_queued_tickets, self.0.total_redeems, self.0.total_expired_tickets, self.0.total_successful_tasks, self.0.total_failed_tasks, self.0.total_found_empty_queue))
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            total_queued_tickets: AtomicUsize::new(0),
            total_redeems: AtomicUsize::new(0),
            total_expired_tickets: AtomicUsize::new(0),
            total_successful_tasks: AtomicUsize::new(0),
            total_failed_tasks: AtomicUsize::new(0),
            total_found_empty_queue: AtomicUsize::new(0),
        }
    }

    pub fn inc_total_queued_tickets(&self) {
        self.total_queued_tickets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_total_redeems(&self) {
        self.total_redeems.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_total_expired_tickets(&self) {
        self.total_expired_tickets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_total_successful_tasks(&self) {
        self.total_successful_tasks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_total_failed_tasks(&self) {
        self.total_failed_tasks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_total_found_empty_queue(&self) {
        self.total_found_empty_queue.fetch_add(1, Ordering::Relaxed);
    }

    pub fn to_status_update_response(&self) -> protocol::StatusUpdateResponse {
        let metrics = self;
        protocol::StatusUpdateResponse {
            total_queued_tickets: metrics.total_queued_tickets.load(Ordering::Relaxed) as u64,
            total_redeems: metrics.total_redeems.load(Ordering::Relaxed) as u64,
            total_expired_tickets: metrics.total_expired_tickets.load(Ordering::Relaxed) as u64,
            total_successful_tasks: metrics.total_successful_tasks.load(Ordering::Relaxed) as u64,
            total_failed_tasks: metrics.total_failed_tasks.load(Ordering::Relaxed) as u64,
            total_found_empty_queue: metrics.total_found_empty_queue.load(Ordering::Relaxed) as u64,
        }
    }
}

impl std::ops::AddAssign for SampledMetrics {
    fn add_assign(&mut self, other: Self) {
        self.0.total_queued_tickets += other.0.total_queued_tickets;
        self.0.total_redeems += other.0.total_redeems;
        self.0.total_expired_tickets += other.0.total_expired_tickets;
        self.0.total_successful_tasks += other.0.total_successful_tasks;
        self.0.total_failed_tasks += other.0.total_failed_tasks;
        self.0.total_found_empty_queue += other.0.total_found_empty_queue;
    }
}

impl std::ops::SubAssign for SampledMetrics {
    fn sub_assign(&mut self, other: Self) {
        self.0.total_queued_tickets -= other.0.total_queued_tickets;
        self.0.total_redeems -= other.0.total_redeems;
        self.0.total_expired_tickets -= other.0.total_expired_tickets;
        self.0.total_successful_tasks -= other.0.total_successful_tasks;
        self.0.total_failed_tasks -= other.0.total_failed_tasks;
        self.0.total_found_empty_queue -= other.0.total_found_empty_queue;
    }
}
