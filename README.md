# Conserve Userland Filesystem (FUSE)

This repository contains the Conserve backup archive FUSE implementation. It allows you to mount your Conserve
backup archives as a filesystem, enabling easy access and navigation of your backed-up files and directories.

**Status: Work in Progress** - Please refer to the Roadmap section for more information on current and upcoming
features.

## Installation

To install Conserve FUSE, you need to have Rust's package manager, Cargo, installed. Once you have Cargo, you can
install Conserve FUSE using the following command:

```bash
cargo install conserve-fuse
```

This will download and compile the Conserve FUSE from the source.

## Usage

After installing, you can mount a Conserve backup archive to a directory of your choice. For example:

```bash
conserve-mount /path/to/my/archive /mnt
```

This command mounts the backup archive located at `/path/to/my/archive` to the `/mnt` directory. Once mounted,
you can use standard file system commands to interact with the archive:

```bash
ls /mnt
```

## Roadmap

The development of Conserve FUSE is ongoing, and several features are in the pipeline:

- [x] Basic directory traversing
- [x] Read file
- [ ] UID/GID support
- [ ] Permissions support
- [ ] Advanced options for UID/GID remapping, etc.
