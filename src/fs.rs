use std::{
    borrow::Cow,
    ffi::OsStr,
    iter,
    num::NonZeroU64,
    path::Path,
    rc::Rc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use conserve::{Apath, IndexEntry, Kind, StoredTree};
use fuser::{FileAttr, FileType, Filesystem};
use libc::{EINVAL, ENOENT, ENOSYS};
use log::debug;

mod dirindex;

use dirindex::DirIndex;

type EntryRef = Rc<Entry>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct INode(u64);

impl INode {
    fn from_u64(ino: u64) -> Option<Self> {
        if ino == 0 {
            return None;
        }
        INode(ino).into()
    }

    fn is_root(&self) -> bool {
        self.0 == 1
    }
}

impl From<INode> for u64 {
    fn from(value: INode) -> Self {
        value.0
    }
}

pub struct ConserveFilesystem {
    tree: StoredTree,
    index: DirIndex,
}

impl ConserveFilesystem {
    pub fn new(tree: StoredTree) -> Self {
        Self {
            tree,
            index: DirIndex::default(),
        }
    }

    #[inline]
    fn lookup_child_of(&mut self, parent: INode, name: &OsStr) -> Option<&EntryRef> {
        self.index.lookup_child_of(parent, name)
    }

    fn open_dir(&mut self, ino: INode) -> bool {
        if !self.index.is_dir_loaded(ino) {
            if ino.is_root() {
                self.load_dir(ino, &Apath::root())
            } else {
                let Some((_, entry)) = self.index.lookup(ino) else {
                    return false;
                };
                self.load_dir(ino, &entry.clone().inner.apath);
            };
        }
        true
    }

    fn load_dir(&mut self, ino: INode, dir_path: &Apath) {
        self.tree
            .band()
            .index()
            .iter_hunks()
            .advance_to_after(dir_path)
            .flatten()
            .take_while(|entry| dir_path.is_prefix_of(&entry.apath))
            .filter(|entry| {
                let path: &Path = entry.apath.as_ref();
                let parent_path: &Path = dir_path.as_ref();
                dbg!(path.parent()) == Some(dbg!(parent_path))
            })
            .for_each(|entry| {
                self.index.insert_entry(ino, entry);
            });
    }

    fn list_dir(&mut self, ino: INode) -> Option<impl Iterator<Item = (&OsStr, &EntryRef)>> {
        let (parent, dir) = self.index.lookup(ino)?;
        let (_, parent) = self.index.lookup(*parent)?;

        Some(
            iter::once((OsStr::new("."), dir))
                .chain(iter::once((OsStr::new(".."), parent)))
                .chain(
                    self.index
                        .parent_and_children(dir.ino)
                        .skip(1)
                        .filter_map(|(_, entry)| (entry.file_name()?, entry).into()),
                ),
        )
    }

    fn get(&self, ino: INode) -> Option<&EntryRef> {
        let (_, entry) = self.index.lookup(ino)?;
        entry.into()
    }

    fn get_dir(&self, ino: INode) -> Option<&EntryRef> {
        self.get(ino)
            .filter(|entry| entry.file_type() == Some(FileType::Directory))
    }
}

const TTL: Duration = Duration::from_secs(1);

impl Filesystem for ConserveFilesystem {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        self.open_dir(INode(1));
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
        if let Some(file_attr) = self
            .lookup_child_of(parent_dir.ino, name)
            .and_then(|entry| entry.file_attr())
        {
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
        if self.open_dir(ino) {
            reply.error(ENOENT);
        } else {
            reply.opened(0, 0);
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

        for (i, (name, entry)) in entries.enumerate().skip(offset as usize) {
            debug!("readdir entry: {entry:?}");

            let Some(file_type) = entry.file_type() else {
                continue;
            };

            if reply.add(entry.ino.into(), (i + 1) as i64, file_type, name) {
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
    fn path(&self) -> &Path {
        self.inner.apath.as_ref()
    }

    fn file_name(&self) -> Option<&OsStr> {
        if AsRef::<str>::as_ref(&self.inner.apath).is_empty() {
            return Some(OsStr::new("."));
        }
        self.path().file_name()
    }

    fn file_type(&self) -> Option<FileType> {
        kind_to_filetype(self.inner.kind)
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
    use std::path::Path;

    use conserve::Archive;

    use super::ConserveFilesystem;

    #[test]
    fn open() {
        let path = Path::new("/tank/backup/etc");
        let archive = Archive::open_path(path).expect("archive");

        let tree = archive
            .open_stored_tree(conserve::BandSelectionPolicy::LatestClosed)
            .expect("tree");

        let filesystem = ConserveFilesystem::new(tree);
    }
}
