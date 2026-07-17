//! Rust types shared by the Agent control API boundary.

/// Stable workspace identity for the control API boundary.
pub const COMPONENT: &str = "control-api";

#[cfg(test)]
mod tests {
    use super::COMPONENT;

    #[test]
    fn exposes_its_component_identity() {
        assert_eq!(COMPONENT, "control-api");
    }
}
