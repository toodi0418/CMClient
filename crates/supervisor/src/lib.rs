//! Process supervision primitives owned by the Rust Agent.

/// Stable workspace identity for the supervisor boundary.
pub const COMPONENT: &str = "supervisor";

#[cfg(test)]
mod tests {
    use super::COMPONENT;

    #[test]
    fn exposes_its_component_identity() {
        assert_eq!(COMPONENT, "supervisor");
    }
}
