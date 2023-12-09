use std::{
    borrow::Cow,
    ffi::OsStr,
    io, iter,
    path::Path,
    rc::Rc,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, Bytes};
use conserve::{monitor::Monitor, Apath, Exclude, IndexEntry, Kind, ReadTree, StoredTree};
use fuser::{FileAttr, FileType, Filesystem};
use libc::{EINVAL, ENOENT};
use log::{debug, error};
use snafu::prelude::*;

mod tree;

use tree::FilesystemTree;

use self::tree::INodeEntry;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("INode {ino} does not exists"))]
    NoExists {
        ino: INode,
    },
    FilesystemTree {
        source: tree::Error,
    },
    Conserve {
        source: conserve::Error,
    },
    Transport {
        source: conserve::transport::Error,
    },
    Io {
        source: io::Error,
    },
}

type Result<T> = std::result::Result<T, Error>;

type EntryRef = Rc<Entry>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct INode(u64);

impl INode {
    pub const ROOT: Self = Self(1);
    pub const ROOT_PARENT: Self = Self(0);

    fn from_u64(ino: u64) -> Option<Self> {
        if ino == 0 {
            return None;
        }
        INode(ino).into()
    }

    fn is_root(&self) -> bool {
        self == &Self::ROOT
    }
}

impl std::fmt::Display for INode {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:x}/{}", self.0, self.0)
    }
}

impl From<INode> for u64 {
    fn from(value: INode) -> Self {
        value.0
    }
}

pub struct ConserveFilesystem {
    tree: StoredTree,
    fs: FilesystemTree,
}

impl ConserveFilesystem {
    pub fn new(tree: StoredTree) -> Self {
        let root_entry = tree
            .band()
            .index()
            .iter_hunks()
            .flatten()
            .take(1)
            .next()
            .expect("root");

        assert_eq!(root_entry.apath, "/");
        let fs = FilesystemTree::new(root_entry);
        Self { tree, fs }
    }

    fn lookup_child_by_name(&mut self, parent: INode, name: &OsStr) -> Result<Option<&EntryRef>> {
        if !self.fs.is_dir_loaded(parent) {
            let Some(parent_entry) = self
                .fs
                .lookup(parent)
                .map(|INodeEntry { parent: _, entry }| entry.clone())
            else {
                return Ok(None);
            };
            self.load_dir(parent, parent_entry.apath())?;
        }
        Ok(self.fs.lookup_child_by_name(parent, name))
    }

    fn open_dir(&mut self, ino: INode) -> Result<()> {
        let Some(dir_entry) = self
            .fs
            .lookup(ino)
            .map(|INodeEntry { parent: _, entry }| entry)
            .filter(|entry| entry.is_dir())
        else {
            return Err(Error::NoExists { ino });
        };
        if !self.fs.is_dir_loaded(dir_entry.ino) {
            self.load_dir(ino, dir_entry.clone().apath())?;
        }
        Ok(())
    }

    fn load_dir(&mut self, ino: INode, dir_path: &Apath) -> Result<()> {
        let iter = self
            .tree
            .iter_entries(dir_path.clone(), Exclude::nothing())
            .context(ConserveSnafu)?
            .skip(1)
            .take_while(|entry| {
                let path: &Path = entry.apath.as_ref();
                let parent_path: &Path = dir_path.as_ref();
                debug!("inspect entry path = {path:?} ?==? parent path = {parent_path:?}");
                path.parent() == Some(parent_path)
            });
        for entry in iter {
            debug!("insert entry parent = {ino}, entry = {entry:?}");
            self.fs
                .insert_entry(ino, entry)
                .context(FilesystemTreeSnafu)?;
        }

        Ok(())
    }

    fn read_file(&self, ino: INode, mut offset: u64) -> Result<Option<Bytes>> {
        let Some(entry) = self.fs.lookup_entry(ino).filter(|entry| entry.is_file()) else {
            return Ok(None);
        };

        let addr = entry
            .inner
            .addrs
            .iter()
            .find(|addr| {
                if offset > addr.len {
                    offset -= addr.len;
                    false
                } else {
                    true
                }
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "end of file reached before offset",
                )
            })
            .context(IoSnafu)?;

        let content = self
            .tree
            .block_dir()
            .read_address(addr, DiscardMonitor::arc())
            .context(ConserveSnafu)?;
        Ok(content.slice((offset as usize)..).into())
    }

    fn list_dir(&self, ino: INode) -> Option<impl Iterator<Item = ListEntry<'_>>> {
        let INodeEntry { parent, entry: dir } = self.fs.lookup(ino)?;
        let dir = ListEntry {
            ino: dir.ino,
            name: ".".into(),
            file_type: FileType::Directory,
        };
        let parent = ListEntry {
            ino: *parent,
            name: "..".into(),
            file_type: FileType::Directory,
        };

        Some(
            iter::once(dir)
                .chain(iter::once(parent))
                .chain(self.fs.children(ino).filter_map(|(name, entry)| {
                    ListEntry {
                        ino: entry.ino,
                        name: name.into(),
                        file_type: entry.file_type()?,
                    }
                    .into()
                })),
        )
    }

    fn get(&self, ino: INode) -> Option<&EntryRef> {
        let INodeEntry { parent: _, entry } = self.fs.lookup(ino)?;
        entry.into()
    }

    fn get_dir(&self, ino: INode) -> Option<&EntryRef> {
        self.get(ino)
            .filter(|entry| entry.file_type() == Some(FileType::Directory))
    }
}

#[derive(Debug, Clone)]
struct ListEntry<'s> {
    ino: INode,
    name: Cow<'s, str>,
    file_type: FileType,
}

const TTL: Duration = Duration::from_secs(1);

impl Filesystem for ConserveFilesystem {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        Ok(())
    }

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        debug!(
            "lookup parent = {} name = {}",
            parent,
            name.to_str().unwrap_or_default()
        );

        let Some(parent) = INode::from_u64(parent) else {
            reply.error(EINVAL);
            return;
        };

        let Some(parent_dir) = self.get_dir(parent) else {
            reply.error(ENOENT);
            return;
        };

        debug!("loopkup parent dir {parent_dir:?} {name:?}");
        match self.lookup_child_by_name(parent_dir.ino, name) {
            Err(err) => {
                error!("lookup child {name:?} in {parent} failed: {err}");
                reply.error(EINVAL);
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(entry)) => {
                let file_attr = entry.file_attr().unwrap();
                debug!("lookup match {name:?} {file_attr:?}");
                reply.entry(&TTL, &file_attr, 0);
            }
        }
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let Some(ino) = INode::from_u64(ino) else {
            reply.error(EINVAL);
            return;
        };

        if offset < 0 {
            reply.error(EINVAL);
            return;
        }

        debug!(
            "read {} {} {} {} {} {:?}",
            ino, fh, offset, size, flags, lock_owner
        );

        match self.read_file(ino, offset as u64) {
            Err(err) => {
                error!("read file error: {err}");
                reply.error(EINVAL);
            }
            Ok(None) => reply.error(ENOENT),
            Ok(Some(content)) => reply.data(content.chunk()),
        }
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        let Some(ino) = INode::from_u64(ino) else {
            reply.error(EINVAL);
            return;
        };

        match self.open_dir(ino) {
            Err(Error::NoExists { ino: _ }) => reply.error(ENOENT),
            Err(err) => {
                error!("failed to open dir {ino}: {err}");
                reply.error(EINVAL)
            }
            Ok(_) => reply.opened(0, 0),
        }
    }

    fn getattr(&mut self, _req: &fuser::Request, ino: u64, reply: fuser::ReplyAttr) {
        debug!("getattr ino = {}", ino);
        let Some(ino) = INode::from_u64(ino) else {
            reply.error(EINVAL);
            return;
        };
        let Some(file_attr) = self.get(ino).and_then(|entry| entry.file_attr()) else {
            reply.error(ENOENT);
            return;
        };

        reply.attr(&TTL, &file_attr);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        debug!("readdir ino = {}, fh = {}, offset = {}", ino, fh, offset);
        let Some(ino) = INode::from_u64(ino) else {
            reply.error(EINVAL);
            return;
        };

        let Some(entries) = self.list_dir(ino) else {
            reply.error(ENOENT);
            return;
        };

        for (i, entry) in entries.enumerate().skip(offset as usize) {
            debug!("readdir entry: {entry:?}");

            if reply.add(
                entry.ino.into(),
                (i + 1) as i64,
                entry.file_type,
                entry.name.as_ref(),
            ) {
                break;
            }
        }
        reply.ok();
    }
}

#[derive(Debug, Clone)]
struct Entry {
    ino: INode,
    inner: IndexEntry,
}

impl Entry {
    #[inline]
    fn file_type(&self) -> Option<FileType> {
        kind_to_filetype(self.inner.kind)
    }

    #[inline]
    fn is_dir(&self) -> bool {
        self.inner.kind == Kind::Dir
    }

    #[inline]
    fn is_file(&self) -> bool {
        self.inner.kind == Kind::File
    }

    #[inline]
    fn apath(&self) -> &Apath {
        &self.inner.apath
    }

    fn file_attr(&self) -> Option<FileAttr> {
        FileAttr {
            ino: self.ino.into(),
            size: self.inner.addrs.iter().map(|addr| addr.len).sum(),
            blocks: self.inner.addrs.len() as u64,
            atime: UNIX_EPOCH,
            mtime: into_time(self.inner.mtime, self.inner.mtime_nanos),
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: self.file_type()?,
            perm: 0o777,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: self
                .inner
                .addrs
                .first()
                .map(|addr| addr.len)
                .unwrap_or_default() as u32,
        }
        .into()
    }
}

fn into_time(secs: i64, subsec_nanos: u32) -> SystemTime {
    let t = if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs())
    };

    t + Duration::from_nanos(subsec_nanos as u64)
}

fn kind_to_filetype(kind: Kind) -> Option<FileType> {
    match kind {
        Kind::File => FileType::RegularFile.into(),
        Kind::Dir => FileType::Directory.into(),
        Kind::Symlink => FileType::Symlink.into(),
        Kind::Unknown => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct DiscardMonitor;

impl DiscardMonitor {
    fn arc() -> Arc<dyn Monitor> {
        Arc::new(Self)
    }
}

impl Monitor for DiscardMonitor {
    fn count(&self, _counter: conserve::counters::Counter, _increment: usize) {}

    fn set_counter(&self, _counter: conserve::counters::Counter, _value: usize) {}

    fn problem(&self, _problem: conserve::monitor::Problem) {}

    fn start_task(&self, name: String) -> conserve::monitor::task::Task {
        conserve::monitor::task::TaskList::default().start_task(name)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::{ffi::OsStr, path::Path};

    use conserve::{Archive, BlockHash};

    use super::{ConserveFilesystem, INode};

    fn load_fs(name: &str) -> ConserveFilesystem {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let base = Path::new(&manifest_dir);
        let path = base.join("fixtures").join(name);
        let archive = Archive::open_path(&path).expect("archive");

        let tree = archive
            .open_stored_tree(conserve::BandSelectionPolicy::LatestClosed)
            .expect("tree");

        ConserveFilesystem::new(tree)
    }

    #[test]
    fn open_root() {
        let mut filesystem = load_fs("backup-treeroot");
        let root_ino = INode::from_u64(1).unwrap();
        filesystem.open_dir(root_ino).expect("open root dir");
        let Some(dir_iter) = filesystem.list_dir(root_ino) else {
            panic!("root not loaded");
        };

        let content: Vec<_> = dir_iter
            .map(|ls_entry| (ls_entry.name.to_string(), ls_entry.ino))
            .collect();

        assert_eq!(
            vec![
                (String::from("."), INode(1)),
                (String::from(".."), INode(0)),
                (String::from("file0.txt"), INode(3)),
                (String::from("subdir0"), INode(4)),
                (String::from("subdir1"), INode(5)),
                (String::from("words"), INode(6)),
            ],
            content
        );
    }

    #[test]
    fn open_subdir() {
        let mut filesystem = load_fs("backup-treeroot");
        let root_ino = INode::from_u64(1).unwrap();
        filesystem.open_dir(root_ino).expect("open root dir");

        filesystem.open_dir(INode(4)).expect("open subdir0");
        let Some(dir_iter) = filesystem.list_dir(INode(4)) else {
            panic!("root not loaded");
        };

        let content: Vec<_> = dir_iter
            .map(|ls_entry| (ls_entry.name.to_string(), ls_entry.ino))
            .collect();

        assert_eq!(
            vec![
                (String::from("."), INode(4)),
                (String::from(".."), INode(1)),
                (String::from("subdir0_1"), INode(7)),
                (String::from("subdir0_2"), INode(8)),
            ],
            content
        );
    }

    #[test]
    fn test_read_small_file() {
        let mut filesystem = load_fs("backup-treeroot");

        filesystem.open_dir(INode::ROOT).expect("open root");
        let child = filesystem
            .lookup_child_by_name(INode::ROOT, OsStr::new("file0.txt"))
            .expect("child")
            .expect("child")
            .clone();
        let content = filesystem
            .read_file(child.ino, 0)
            .expect("read file")
            .expect("some content");
        let content = content.chunks(1024).take(1).next().expect("content");
        assert_eq!(String::from_utf8_lossy(content).as_ref(), "Hello FUSE!\n");
    }

    #[test]
    fn test_read_large_file() {
        let mut filesystem = load_fs("backup-treeroot");

        filesystem.open_dir(INode::ROOT).expect("open root");
        let child = filesystem
            .lookup_child_by_name(INode::ROOT, OsStr::new("words"))
            .expect("child")
            .expect("child")
            .clone();

        let size = child.file_attr().unwrap().size;
        let mut hasher = blake2_rfc::blake2b::Blake2b::new(64);
        let mut offset = 0;

        while offset < size {
            let buffer = filesystem
                .read_file(child.ino, offset)
                .expect("read file")
                .expect("some content");
            offset += buffer.len() as u64;
            for chunk in buffer.chunks(4096) {
                hasher.update(chunk);
            }
        }

        let hash_result = hasher.finalize();
        let hash = BlockHash::from(hash_result);
        assert_eq!(hash.to_string(), "53b246febde6a54d4f9995a3c7b68a38e1dd93159a196c642fabafa09e7eec113cc4061856d12997901dbc1ba95bd7bff517a312c6de3f01b1d380ea157bc122");
    }
}
