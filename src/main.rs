use std::{
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
};

use clap::Parser;
use conserve::{open_transport, Archive, BandSelectionPolicy};
use conserve_fuse::fs::ConserveFilesystem;
use fuser::spawn_mount2;
use log::info;

#[derive(Debug, Parser)]
struct Args {
    /// Archive path or URL.
    archive_url: PathBuf,
    /// Directory where the filesystem will be mounted.
    mount_path: PathBuf,
    /// Band to open, defaults to latest closed.
    #[arg(short, long)]
    band: Option<String>,
}

impl Args {
    fn band(&self) -> conserve::Result<BandSelectionPolicy> {
        Ok(match &self.band {
            Some(s) => BandSelectionPolicy::Specified(s.parse()?),
            None => BandSelectionPolicy::LatestClosed,
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();
    let transport = open_transport(args.archive_url.to_string_lossy().as_ref())?;
    let archive = Archive::open(transport.clone())?;
    let tree = archive.open_stored_tree(args.band()?)?;

    let fs = ConserveFilesystem::new(tree);
    let session = spawn_mount2(fs, &args.mount_path, &[])?;

    info!(
        "archive mounted at {mountpoint:?}",
        mountpoint = session.mountpoint
    );

    let waiter = Arc::new((Condvar::new(), Mutex::new(false)));
    let signaler = waiter.clone();
    ctrlc::set_handler(move || {
        info!("signal received exiting...");
        let (cond, mx) = &*signaler;
        *mx.lock().unwrap() = true;
        cond.notify_one();
    })?;

    let (cond, mx) = &*waiter;
    let mut done = mx.lock().unwrap();
    while !*done {
        done = cond.wait(done).unwrap();
    }
    drop(session);

    Ok(())
}
