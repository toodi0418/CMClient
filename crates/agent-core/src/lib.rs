//! Shared Rust foundations for the CMClient Agent.

/// Stable workspace identity for the Agent core boundary.
pub const COMPONENT: &str = "agent-core";

#[cfg(test)]
mod tests {
    use super::COMPONENT;

    #[test]
    fn exposes_its_component_identity() {
        assert_eq!(COMPONENT, "agent-core");
    }
}
