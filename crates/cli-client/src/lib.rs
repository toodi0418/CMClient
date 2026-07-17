//! Rust client support for communication with the local Agent.

/// Stable workspace identity for the CLI client boundary.
pub const COMPONENT: &str = "cli-client";

#[cfg(test)]
mod tests {
    use super::COMPONENT;

    #[test]
    fn exposes_its_component_identity() {
        assert_eq!(COMPONENT, "cli-client");
    }
}
