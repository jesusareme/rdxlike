use std::assert_matches;
use monilib::{LibClockSource, LibConfig, MoniErrorType, MoniLib, MoniLogLevel};
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use uuid::Uuid;
use monilib::LibErrorCause::Path;
use monilib::MoniErrorType::Lib;

fn config() -> LibConfig {
    LibConfig {
        log_level: MoniLogLevel::Debug,
        clock: LibClockSource::System,
    }
}

fn tmp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!("monilib-tests-{}", Uuid::new_v4()));
    fs::create_dir_all(&path).expect("temp dir should be accessible");
    path
}

fn wait() {
    thread::sleep(Duration::from_millis(500));
}

#[test]
fn unreadable_state_should_not_crash_library() {
    let path = tmp_dir();
    fs::write(path.join("state.json"), "{unreadable model}")
        .expect("state file should be writable");

    let lib = MoniLib::new(path.display().to_string(), config())
        .expect("corrupted state file should not error library");

    wait();

    assert!(!lib.has_finished());
}

#[test]
fn missing_state_should_start_from_new_model() {
    let path = tmp_dir();

    let lib = MoniLib::new(path.display().to_string(), config()).expect("lib should start");

    wait();

    assert!(!lib.has_finished());
    assert!(path.join("state.json").exists());
}

#[test]
fn wrong_path_should_fail_on_lib_init() {
    let wrong_path = tmp_dir().join("nowhere");

    let Err(error) = MoniLib::new(wrong_path.to_string_lossy().to_string(), config()) else {
        panic!("a path that does not exist should not be initialized");
    };

    assert_matches!(error.error_type, Lib(Path));
}