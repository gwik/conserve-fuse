use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    num::NonZeroU64,
};

use conserve::IndexEntry;

use super::{Entry, EntryRef, INode};

type ParentIndexKey = (INode, String);

#[derive(Debug, Clone)]
pub(super) struct DirIndex {
    next_ino: u64,
    ino_index: HashMap<INode, (INode, EntryRef)>,
    parent_index: BTreeMap<ParentIndexKey, EntryRef>,
}

impl Default for DirIndex {
    fn default() -> Self {
        Self {
            next_ino: 1,
            ino_index: HashMap::new(),
            parent_index: BTreeMap::new(),
        }
    }
}

impl DirIndex {
    pub fn is_dir_loaded(&self, dir: INode) -> bool {
        self.parent_index.contains_key(&(dir, String::new()))
    }

    fn alloc_inode(&mut self) -> INode {
        self.next_ino += 1;
        INode(self.next_ino)
    }

    pub fn parent_and_children(
        &self,
        parent: INode,
    ) -> impl Iterator<Item = (&ParentIndexKey, &EntryRef)> {
        self.parent_index
            .range((parent, String::new())..(INode(parent.0 + 1u64), String::new()))
    }

    pub fn lookup(&self, ino: INode) -> Option<&(INode, EntryRef)> {
        self.ino_index.get(&ino)
    }

    pub fn lookup_child_of(&self, parent: INode, name: &OsStr) -> Option<&EntryRef> {
        let key = (parent, name.to_string_lossy().to_string());
        self.parent_index.get(&key)
    }

    pub fn insert_entry(&mut self, parent: INode, entry: IndexEntry) {
        let ino = self.alloc_inode();
        let entry: EntryRef = Entry { ino, inner: entry }.into();
        self.ino_index.insert(ino, (parent, entry.clone()));
        let key = (parent, entry.path().to_string_lossy().to_string());
        self.parent_index.insert(key, entry);
    }
}
