use monilib::{ExpenseCategory, LibClockSource, LibConfig, MoniExpense, MoniLib, MoniLogLevel};
use rand::random;
use std::{thread, time::Duration};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");
    let tmp = "/var/tmp";

    let config = LibConfig {
        log_level: MoniLogLevel::Debug,
        clock: LibClockSource::System,
    };

    let lib = MoniLib::new(tmp.to_string(), config)?;
    // lib.save();

    lib.add_expense(MoniExpense {
        date: None,
        amount: random(),
        comment: Some("go.rs1".to_string()),
        category: ExpenseCategory::Essential,
    })?;

    thread::sleep(Duration::from_secs(2));

    lib.add_expense(MoniExpense {
        date: None,
        amount: random(),
        comment: Some("go.rs2".to_string()),
        category: ExpenseCategory::Essential,
    })?;

    thread::sleep(Duration::from_secs(6));

    Ok(())
}
