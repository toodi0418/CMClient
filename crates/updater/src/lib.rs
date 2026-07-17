//! Signed update and rollback support owned by the Rust Agent.

/// Stable workspace identity for the updater boundary.
pub const COMPONENT: &str = "updater";

#[cfg(test)]
mod tests {
    use super::COMPONENT;

    #[test]
    fn exposes_its_component_identity() {
        assert_eq!(COMPONENT, "updater");
    }
}
