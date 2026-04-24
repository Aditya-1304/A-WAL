use crate::{error::WalError, lsn::Lsn};

#[derive(Debug)]
pub struct SyncCoordinator {
    pub durable_lsn: Lsn,
    pub requested_lsn: Option<Lsn>,
    pub sync_in_flight: bool,
    pub waiting_sync_requests: usize,
    pub last_error: Option<String>,
}

impl SyncCoordinator {
    pub fn new(durable_lsn: Lsn) -> Self {
        Self {
            durable_lsn,
            requested_lsn: None,
            sync_in_flight: false,
            waiting_sync_requests: 0,
            last_error: None,
        }
    }

    pub fn durable_lsn(&self) -> Lsn {
        self.durable_lsn
    }

    pub fn publish_durable_lsn(&mut self, durable_lsn: Lsn) {
        if durable_lsn > self.durable_lsn {
            self.durable_lsn = durable_lsn;
            self.last_error = None;
        }
    }

    pub fn add_waiter(&mut self, lsn: Lsn) {
        self.waiting_sync_requests += 1;
        self.requested_lsn = Some(match self.requested_lsn {
            Some(existing) => existing.max(lsn),
            None => lsn,
        });
    }

    pub fn remove_waiter(&mut self) {
        self.waiting_sync_requests = self.waiting_sync_requests.saturating_sub(1);

        if self.waiting_sync_requests == 0 && !self.sync_in_flight {
            self.requested_lsn = None;
        }
    }

    pub fn begin_sync(&mut self) {
        self.sync_in_flight = true;
    }

    pub fn finish_sync_success(&mut self, durable_lsn: Lsn) {
        self.publish_durable_lsn(durable_lsn);
        self.sync_in_flight = false;
        self.last_error = None;

        if self
            .requested_lsn
            .is_some_and(|requested_lsn| requested_lsn <= self.durable_lsn)
        {
            self.requested_lsn = None;
        }
    }

    pub fn finish_sync_error(&mut self, error: &WalError) {
        self.sync_in_flight = false;
        self.last_error = Some(error.to_string());
    }

    pub fn finish_sync_without_error(&mut self) {
        self.sync_in_flight = false;
    }
}
