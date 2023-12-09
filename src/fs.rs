use std::{
    borrow::Cow,
    ffi::OsStr,
    iter,
    path::Path,
    rc::Rc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use conserve::{Apath, IndexEntry, Kind, StoredTree};
use fuser::{FileAttr, FileType, Filesystem};
use libc::{EINVAL, ENOENT, ENOSYS};
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
        let index = FilesystemTree::new(root_entry);

        Self { tree, fs: index }
    }

    #[inline]
    fn lookup_child_of(&self, parent: INode, name: &OsStr) -> Option<&EntryRef> {
        self.fs.lookup_child_of(parent, name)
    }

    fn open_dir(&mut self, ino: INode) -> Result<()> {
        let Some(INodeEntry { parent: _, entry }) = self.fs.lookup(ino) else {
            return Err(Error::NoExists { ino });
        };
        // TODO(gwik): load if needed
        self.load_dir(ino, &entry.clone().inner.apath)?;
        Ok(())
    }

    fn load_dir(&mut self, ino: INode, dir_path: &Apath) -> Result<()> {
        let iter = self
            .tree
            .band()
            .index()
            .iter_hunks()
            // we would want to advance to after "{dir}/" , unfortunately, that's not a valid Apath
            // .advance_to_after(dir_path)
            .flatten()
            // .take_while(|entry| dbg!(dir_path).is_prefix_of(dbg!(&entry.apath)))
            .filter(|entry| {
                let path: &Path = entry.apath.as_ref();
                let parent_path: &Path = dir_path.as_ref();
                debug!("path = {path:?} ?==? parent path = {parent_path:?}");
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
                .chain(self.fs.children(ino).filter_map(|((_, name), entry)| {
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

        debug!("loopkup parent dir {:?}", parent_dir);
        if let Some(entry) = self.lookup_child_of(parent_dir.ino, name).map(Rc::clone) {
            if entry.is_dir() {
                let _fixme = self.load_dir(entry.ino, &entry.inner.apath);
            }
            let file_attr = entry.file_attr().unwrap();
            debug!("lookup match {name:?} {file_attr:?}");
            reply.entry(&TTL, &file_attr, 0);
        } else {
            reply.error(ENOENT);
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
        debug!(
            "read {} {} {} {} {} {:?}",
            ino, fh, offset, size, flags, lock_owner
        );
        reply.error(ENOSYS);
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
    fn file_type(&self) -> Option<FileType> {
        kind_to_filetype(self.inner.kind)
    }

    fn is_dir(&self) -> bool {
        self.inner.kind == Kind::Dir
    }

    fn file_attr(&self) -> Option<FileAttr> {
        FileAttr {
            ino: self.ino.into(),
            size: 0,
            blocks: 0,
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
            blksize: 512,
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::path::Path;

    use conserve::{Archive, StoredTree};

    use super::{ConserveFilesystem, INode};

    fn load_tree() -> StoredTree {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let base = Path::new(&manifest_dir);
        let path = base.join("fixtures/backup-treeroot");
        let archive = Archive::open_path(&path).expect("archive");

        archive
            .open_stored_tree(conserve::BandSelectionPolicy::LatestClosed)
            .expect("tree")
    }

    #[test]
    fn open_root() {
        let tree = load_tree();
        let mut filesystem = ConserveFilesystem::new(tree);
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
}
