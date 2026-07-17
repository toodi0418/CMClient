use cmclient_agent_core::COMPONENT as AGENT_CORE;
use cmclient_control_api::COMPONENT as CONTROL_API;
use cmclient_supervisor::COMPONENT as SUPERVISOR;
use cmclient_updater::COMPONENT as UPDATER;

fn main() {
    println!("cmclient-agent workspace: {AGENT_CORE}, {CONTROL_API}, {SUPERVISOR}, {UPDATER}");
}
