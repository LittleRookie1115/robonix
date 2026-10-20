// SPDX-License-Identifier: MulanPSL-2.0

pub mod health_dependencies;
pub mod pb;
pub mod soma_body;

impl health_dependencies::HealthSnapshot for pb::vitals::VitalsSnapshot {
    fn components(&self) -> impl ExactSizeIterator<Item = (&str, u32)> {
        self.components
            .iter()
            .map(|component| (component.name.as_str(), component.health))
    }

    fn has_bodies(&self) -> bool {
        !self.bodies.is_empty()
    }

    fn body_alert(&self) -> bool {
        self.bodies
            .iter()
            .any(|body| body.state != 0 || !body.message.is_empty())
    }
}
