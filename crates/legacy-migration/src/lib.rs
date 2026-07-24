//! Product-specific, resumable migration into the unified CMClient state root.

mod transaction;

pub use transaction::{
    ChildGatewayMaintenanceRunner, GatewayMaintenanceReport, GatewayMaintenanceRequest,
    GatewayMaintenanceRunner, MaintenanceSchemaHistory, MigrationError, MigrationOutcome,
    MigrationPhase, ProductMigrationRequest, ProductMigrationSourceSet, migrate_detected_product,
    migrate_detected_product_source_sets, pending_migration_source,
    run_or_resume_product_migration, run_or_resume_product_migration_source_set,
    run_or_resume_product_migration_with_phase_hook, source_contains_known_state,
};

/// Stable workspace identity for the offline migration boundary.
pub const COMPONENT: &str = "legacy-migration";
