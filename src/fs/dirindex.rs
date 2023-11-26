use std::{collections::BTreeMap, ffi::OsStr, num::NonZeroU64};

use super::{EntryRef, INode};

pub(super) type DirIndexKey = (INode, String);

#[derive(Debug, Default, Clone)]
pub(super) struct DirIndex {
    inner: BTreeMap<DirIndexKey, EntryRef>,
}

impl DirIndex {
    pub fn contains_dir(&self, dir: INode) -> bool {
        self.inner.contains_key(&(dir, String::new()))
    }

    pub fn parent_and_children(
        &self,
        parent: INode,
    ) -> impl Iterator<Item = (&DirIndexKey, &EntryRef)> {
        self.inner.range(
            (parent, String::new())
                ..(
                    INode(NonZeroU64::new(parent.0.get() + 1u64).unwrap()),
                    String::new(),
                ),
        )
    }

    pub fn lookup(&self, parent: INode, name: &OsStr) -> Option<&EntryRef> {
        let key = (parent, name.to_string_lossy().to_string());
        self.inner.get(&key)
    }

    pub fn insert_entry(&mut self, key: DirIndexKey, entry: EntryRef) {
        self.inner.insert(key, entry);
    }
}
