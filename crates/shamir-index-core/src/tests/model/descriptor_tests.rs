use crate::descriptor::IndexDescriptor;
use crate::kind::IndexKind;
use smallvec::SmallVec;

#[test]
fn descriptor_round_trip() {
    let mut paths: SmallVec<[Vec<u64>; 2]> = SmallVec::new();
    paths.push(vec![1, 2, 3]);
    let d = IndexDescriptor::new(
        7,
        "users_email",
        42,
        paths,
        IndexKind::Btree { unique: true },
    );
    let bytes = bincode::serialize(&d).unwrap();
    let got: IndexDescriptor = bincode::deserialize(&bytes).unwrap();
    assert_eq!(got.id, 7);
    assert_eq!(got.name, "users_email");
    assert_eq!(got.name_interned, 42);
    assert_eq!(got.paths[0], vec![1, 2, 3]);
}
