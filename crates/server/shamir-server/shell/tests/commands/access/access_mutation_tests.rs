//! The typed mutation target of `chmod` / `chown` / `chgrp` must match the
//! addressed selector form.

use shamir_query_types::admin::ResourceRef;

use super::{mutation_resource, Selector};
use crate::shell::output::ShellError;

/// The typed mutation target follows the selector FORM: `db:root` is the
/// DATABASE `root`, while the bare `root` stays the synthetic root and is
/// refused. Mutant: treating `db:root` as Root.
#[test]
fn mutation_targets_follow_the_selector_form() {
    assert!(matches!(
        mutation_resource(&Selector::Root),
        Err(ShellError::RootMutation)
    ));
    assert!(matches!(
        mutation_resource(&Selector::Database(String::from("root"))),
        Ok(ResourceRef::Database { database }) if database == "root"
    ));
    assert!(matches!(
        mutation_resource(&Selector::Function(String::from("math/abs"))),
        Ok(ResourceRef::Function { function }) if function == "math/abs"
    ));
    assert!(matches!(
        mutation_resource(&Selector::Resource(vec![String::from("billing")])),
        Ok(ResourceRef::Database { database }) if database == "billing"
    ));
    assert!(matches!(
        mutation_resource(&Selector::Resource(vec![
            String::from("billing"),
            String::from("main"),
        ])),
        Ok(ResourceRef::Store { store }) if store[0] == "billing" && store[1] == "main"
    ));
    assert!(matches!(
        mutation_resource(&Selector::Resource(vec![
            String::from("billing"),
            String::from("main"),
            String::from("invoices"),
        ])),
        Ok(ResourceRef::Table { table })
            if table[0] == "billing" && table[1] == "main" && table[2] == "invoices"
    ));
}
