use std::path::PathBuf;

use clap::Parser;
use conserve::{open_transport, Archive, BandSelectionPolicy};
use conserve_fuse::fs::ConserveFilesystem;
use fuser::mount2;

#[derive(Debug, Parser)]
struct Args {
    /// Archive path or URL.
    archive_url: PathBuf,
    /// Directory where the filesystem will be mounted.
    mount_path: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();
    let transport = open_transport(args.archive_url.to_string_lossy().as_ref())?;
    let archive = Archive::open(transport.clone())?;
    let tree = archive.open_stored_tree(BandSelectionPolicy::LatestClosed)?;

    let fs = ConserveFilesystem::new(tree);
    mount2(fs, &args.mount_path, &[])?;

    Ok(())
}
