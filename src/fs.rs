use std::{
    ffi::OsStr,
    num::NonZeroU64,
    path::Path,
    rc::Rc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use conserve::{IndexEntry, Kind, StoredTree};
use fuser::{FileAttr, FileType, Filesystem};
use libc::{EINVAL, ENOENT, ENOSYS};
use log::debug;

mod dirindex;

use dirindex::{DirIndex, DirIndexKey};

type EntryRef = Rc<Entry>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct INode(NonZeroU64);

impl INode {
    fn from_u64(ino: u64) -> Option<Self> {
        INode(NonZeroU64::new(ino)?).into()
    }
}

impl From<INode> for u64 {
    fn from(value: INode) -> Self {
        value.0.get()
    }
}

pub struct ConserveFilesystem {
    _tree: StoredTree,
    entry_cache: Vec<EntryRef>,
    dir_index: DirIndex,
}

#[inline]
fn idx(ino: INode) -> usize {
    (ino.0.get() - 1) as usize
}

impl ConserveFilesystem {
    pub fn new(tree: StoredTree) -> Self {
        let entry_cache = Self::entries(&tree);
        Self {
            _tree: tree,
            entry_cache,
            dir_index: DirIndex::default(),
        }
    }

    fn entries(tree: &StoredTree) -> Vec<EntryRef> {
        let mut entries: Vec<Entry> = tree
            .band()
            .index()
            .iter_entries()
            .map(|inner| Entry {
                ino: INode(NonZeroU64::new(1).unwrap()),
                inner,
            })
            .collect();

        entries.sort_by(|a, b| a.path().cmp(b.path()));
        entries
            .into_iter()
            .enumerate()
            .map(|(i, mut entry)| {
                entry.ino = INode(NonZeroU64::new(i as u64 + 1).unwrap());
                entry.into()
            })
            .collect()
    }

    #[inline]
    fn lookup_(&mut self, parent: INode, name: &OsStr) -> Option<&EntryRef> {
        self.dir_index.lookup(parent, name)
    }

    fn load_dir(&mut self, ino: INode) -> impl Iterator<Item = (&DirIndexKey, &EntryRef)> {
        if !self.dir_index.contains_dir(ino) {
            let Some(parent_dir_entry) = self.entry_cache.get(idx(ino)) else {
                return self.dir_index.parent_and_children(ino);
            };

            // take the direct children of parent.
            let entries = self.entry_cache[idx(ino)..]
                .iter()
                .skip(1)
                .take_while(|entry| {
                    parent_dir_entry
                        .inner
                        .apath
                        .is_prefix_of(&entry.inner.apath)
                })
                .filter(|entry| dbg!(entry.path().parent()) == Some(dbg!(parent_dir_entry.path())))
                .collect::<Vec<_>>();

            debug!("load entries into index parent = {parent_dir_entry:?}, entries = {entries:?}");

            // insert parent as (ino, "")
            self.dir_index
                .insert_entry((ino, String::new()), parent_dir_entry.clone());
            for entry in entries {
                self.dir_index.insert_entry(
                    (
                        ino,
                        entry
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string(),
                    ),
                    entry.clone(),
                );
            }
        }
        self.dir_index.parent_and_children(ino)
    }

    fn get(&self, ino: INode) -> Option<&EntryRef> {
        self.entry_cache.get(idx(ino))
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
        debug!("init");
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

        let _ = self.load_dir(parent);
        let Some(parent_dir) = self.get_dir(parent) else {
            reply.error(ENOENT);
            return;
        };

        debug!("loopkup parent dir {:?}", parent_dir);
        if let Some(file_attr) = self
            .lookup_(parent_dir.ino, name)
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
        if self.get_dir(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        let _ = self.load_dir(ino);
        reply.opened(0, 0);
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

        let entries = self.load_dir(ino);

        for (i, ((_, ref name), entry)) in entries.enumerate().skip(offset as usize) {
            debug!("readdir entry: {entry:?}");

            let name = if name.is_empty() { "." } else { name.as_str() };

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
