//! # UI – Zed UI Primitives & Components
//!
//! This crate provides a set of UI primitives and components that are used to build all of the elements in Zed's UI.
//!
//! ## Related Crates:
//!
//! - [`ui_macros`] - proc_macros support for this crate
//! - `ui_input` - the single line input component

pub mod component_prelude;
mod components;
mod gearbox_text;
pub mod prelude;
mod styles;
mod traits;
pub mod utils;

pub use components::*;
pub use gearbox_text::{
    translate as gearbox_translate_text,
    translate_setting_description as gearbox_translate_setting_description,
};
pub use prelude::*;
pub use styles::*;
pub use traits::animation_ext::*;
