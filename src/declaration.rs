//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48).

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the server,
/// `host:6379`: where a Location connects.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "stream",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The stream key a Receive Location reads and a Send Location appends to \
                      when a target names no stream.",
            applies: Applies::Both,
        },
        Setting {
            name: "field",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The entry field that carries the Stream; `body` when left out.",
            applies: Applies::Both,
        },
        Setting {
            name: "after",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The entry id a Receive Location starts reading after; the stream's \
                      beginning when left out.",
            applies: Applies::Receive,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a server that stops mid-reply is waited on; unbounded when \
             left out.",
        ),
    ],
};

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["redis"];
