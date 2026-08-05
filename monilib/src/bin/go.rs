use monilib::{ExpenseCategory, LibClockSource, LibConfig, MoniExpense, MoniLib, MoniLogLevel};
use rand::{random, RngExt, random_range};
use std::{thread, time::Duration};
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let tmp =  std::env::temp_dir().to_str().unwrap().to_string();

    let config = LibConfig {
        log_level: MoniLogLevel::Debug,
        clock: LibClockSource::System,
    };

    let lib = MoniLib::new(tmp, config)?;
    // lib.save();

    lib.add_expense(MoniExpense {
        date: None,
        amount: random_range(-100_000_00..100_000_00),
        comment: Some("go.rs1".to_string()),
        category: ExpenseCategory::Essential,
    })?;

    thread::sleep(Duration::from_secs(2));

    lib.add_expense(MoniExpense {
        date: None,
        amount: random_range(-100_000_00..100_000_00),
        comment: Some("go.rs2".to_string()),
        category: ExpenseCategory::Essential,
    })?;

    thread::sleep(Duration::from_secs(6));

    Ok(())
}
