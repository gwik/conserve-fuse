use std::path::PathBuf;

use clap::Parser;
use conserve::{Archive, BandSelectionPolicy};
use conserve_fuse::fs::ConserveFilesystem;
use fuser::mount2;

#[derive(Debug, Parser)]
struct Args {
    archive_path: PathBuf,
    mount_path: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();
    let archive = Archive::open_path(&args.archive_path)?;
    let tree = archive.open_stored_tree(BandSelectionPolicy::LatestClosed)?;

    let fs = ConserveFilesystem::new(tree);
    mount2(fs, &args.mount_path, &[])?;

    Ok(())
}
