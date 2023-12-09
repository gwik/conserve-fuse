use snafu::prelude::*;

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
};

#[derive(Debug, Snafu)]
pub(super) enum Error {
    #[snafu(display("parent node ({parent} path doesn't match"))]
    ParentPrefixNoMatch { parent: INode },
    #[snafu(display("parent node ({parent} not found"))]
    ParentNotFound { parent: INode },
    #[snafu(display("node is not a directory {inode}"))]
    NotADirectory { inode: INode },
}

use conserve::{Apath, IndexEntry};

use super::{Entry, EntryRef, INode};

#[derive(Debug, Clone)]
pub(super) struct INodeEntry {
    pub(super) parent: INode,
    pub(super) entry: EntryRef,
}

impl INodeEntry {
    fn new(parent: INode, entry: EntryRef) -> Self {
        Self { parent, entry }
    }
}

#[derive(Debug, Clone)]
pub(super) struct FilesystemTree {
    next_ino: u64,
    ino_index: HashMap<INode, INodeEntry>,
    parent_index: HashMap<INode, BTreeMap<String, EntryRef>>,
}

impl FilesystemTree {
    pub fn new(root_entry: IndexEntry) -> Self {
        let mut this = Self {
            next_ino: INode::ROOT.0 + 1,
            ino_index: HashMap::new(),
            parent_index: HashMap::new(),
        };

        let entry: EntryRef = Entry {
            ino: INode::ROOT,
            inner: root_entry,
        }
        .into();
        this.ino_index.insert(
            INode::ROOT,
            INodeEntry::new(INode::ROOT_PARENT, entry.clone()),
        );
        this.parent_index.insert(
            INode::ROOT_PARENT,
            [(String::from(""), entry)].into_iter().collect(),
        );

        this
    }

    fn alloc_inode(&mut self) -> INode {
        self.next_ino += 1;
        INode(self.next_ino)
    }

    pub fn children(&self, parent: INode) -> impl Iterator<Item = (&str, &EntryRef)> {
        self.parent_index
            .get(&parent)
            .into_iter()
            .flatten()
            .map(|(name, entry)| (name.as_str(), entry))
    }

    pub fn is_dir_loaded(&self, dir: INode) -> bool {
        self.parent_index.contains_key(&dir)
    }

    pub fn lookup(&self, ino: INode) -> Option<&INodeEntry> {
        self.ino_index.get(&ino)
    }

    pub fn lookup_child_by_name(&self, parent: INode, name: &OsStr) -> Option<&EntryRef> {
        self.parent_index
            .get(&parent)
            .and_then(|entries| entries.get(name.to_string_lossy().as_ref()))
    }

    pub fn insert_entry(&mut self, parent: INode, entry: IndexEntry) -> Result<(), Error> {
        let ino = self.alloc_inode();
        let entry: EntryRef = Entry { ino, inner: entry }.into();
        let parent_path: Cow<'_, Apath> = if parent.is_root() {
            Cow::Owned(Apath::root())
        } else {
            let INodeEntry {
                parent: _,
                entry: parent_entry,
            } = self
                .lookup(parent)
                .ok_or(Error::ParentNotFound { parent })?;
            if !parent_entry.is_dir() {
                return Err(Error::NotADirectory { inode: parent });
            }
            Cow::Borrowed(&parent_entry.inner.apath)
        };
        let name = entry
            .inner
            .apath
            .strip_prefix(AsRef::<str>::as_ref(parent_path.as_ref()))
            .ok_or(Error::ParentPrefixNoMatch { parent })?
            .trim_start_matches('/')
            .to_string();
        self.ino_index
            .insert(ino, INodeEntry::new(parent, entry.clone()));
        self.parent_index
            .entry(parent)
            .or_default()
            .insert(name, entry);

        Ok(())
    }
}
