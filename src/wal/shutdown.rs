use crate::{
    error::WalError,
    io::control_file::{ControlFile, FsControlFileStore},
    lsn::Lsn,
    types::WalIdentity,
};

/// checkpoint metadata preserved while the clean-shutdown witness changes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CheckpointState {
    pub last_checkpoint_lsn: Option<Lsn>,
    pub checkpoint_no: u64,
}

impl CheckpointState {
    /// recover checkpoint metadata from the current control file
    pub fn from_control(control: Option<&ControlFile>) -> Self {
        match control {
            Some(control) => Self {
                last_checkpoint_lsn: control.last_checkpoint_lsn,
                checkpoint_no: control.checkpoint_no,
            },
            None => Self::default(),
        }
    }
}

/// publish a clean-shutdown witness without changing checkpoint metadata
///
/// recovery still validates every retained segment. The witness is diagnostic
/// state and must never bypass physical history validation
pub(crate) fn publish_clean_shutdown(
    control_store: &FsControlFileStore,
    identity: WalIdentity,
    checkpoint: CheckpointState,
) -> Result<(), WalError> {
    let control = ControlFile::new(
        identity,
        checkpoint.last_checkpoint_lsn,
        checkpoint.checkpoint_no,
        true,
    );

    control_store.publish(&control)
}

/// clear the clean-shutdown witness when a writable WAL is reopened
pub(crate) fn clear_clean_shutdown(
    control_store: &FsControlFileStore,
    identity: WalIdentity,
    checkpoint: CheckpointState,
) -> Result<(), WalError> {
    let control = ControlFile::new(
        identity,
        checkpoint.last_checkpoint_lsn,
        checkpoint.checkpoint_no,
        false,
    );

    control_store.publish(&control)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos();

            let path = std::env::temp_dir()
                .join(format!("wal-shutdown-{prefix}-{}-{nanos}", process::id()));

            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn sample_identity() -> WalIdentity {
        WalIdentity::new(11, 22, 1)
    }

    #[test]
    fn publish_and_clear_clean_shutdown_round_trip_control_flag() {
        let test_dir = TestDir::new("control");
        let store = FsControlFileStore::new(test_dir.path().to_path_buf());

        let checkpoint = CheckpointState {
            last_checkpoint_lsn: Some(Lsn::new(4096)),
            checkpoint_no: 7,
        };

        publish_clean_shutdown(&store, sample_identity(), checkpoint).unwrap();

        let control = store.read().unwrap().unwrap();
        assert!(control.clean_shutdown);
        assert_eq!(control.last_checkpoint_lsn, checkpoint.last_checkpoint_lsn);
        assert_eq!(control.checkpoint_no, checkpoint.checkpoint_no);

        clear_clean_shutdown(&store, sample_identity(), checkpoint).unwrap();

        let control = store.read().unwrap().unwrap();
        assert!(!control.clean_shutdown);
        assert_eq!(control.last_checkpoint_lsn, checkpoint.last_checkpoint_lsn);
        assert_eq!(control.checkpoint_no, checkpoint.checkpoint_no);
    }

    #[test]
    fn checkpoint_state_can_be_extracted_from_control_file() {
        let checkpoint = CheckpointState {
            last_checkpoint_lsn: Some(Lsn::new(1234)),
            checkpoint_no: 9,
        };
        let control = ControlFile::new(
            sample_identity(),
            checkpoint.last_checkpoint_lsn,
            checkpoint.checkpoint_no,
            true,
        );

        assert_eq!(CheckpointState::from_control(Some(&control)), checkpoint);
        assert_eq!(
            CheckpointState::from_control(None),
            CheckpointState::default()
        );
    }
}
