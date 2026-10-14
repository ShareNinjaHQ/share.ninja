//! Device-profile display helpers, shared across the app orchestrator and the
//! terminal UI so a nearby peer renders the same label everywhere.

use crate::models;
use crate::utils::device_models::apple_model_name;

/// Human model name: Apple identifiers ("iPhone16,1", "Mac16,5") map to their
/// marketing name ("iPhone 15 Pro", "iPad Pro"); anything unknown
/// passes through raw. Mirrors `formatModel` in sharon-lib's `utils/device.ts`.
pub fn format_model(model: &str) -> &str {
    apple_model_name(model).unwrap_or(model)
}

/// Marketing browser name; empty for `Other`/unknown.
fn browser_name(browser: Option<models::ws_device_profile::Browser>) -> &'static str {
    use models::ws_device_profile::Browser;
    match browser {
        Some(Browser::Chrome) => "Chrome",
        Some(Browser::Firefox) => "Firefox",
        Some(Browser::Safari) => "Safari",
        Some(Browser::Edge) => "Edge",
        Some(Browser::Other) | None => "",
    }
}

/// Human device string for a nearby peer, derived from the backend's structured
/// [`WsDeviceProfile`] (the flat `description` field it replaced). Joins the
/// browser and model with " · "; the OS is omitted since it's conveyed by an
/// icon. Empty when nothing is known (bonjour/BLE feeds leave
/// `DiscoveredPeer::description` empty the same way).
pub fn nearby_device_label(device: &models::WsDeviceProfile) -> String {
    let mut parts: Vec<String> = Vec::new();
    let browser = browser_name(device.browser);
    if !browser.is_empty() {
        parts.push(browser.to_string());
    }
    if let Some(model) = device.model.as_deref()
        && !model.is_empty()
    {
        parts.push(format_model(model).to_string());
    }
    parts.join(" · ")
}

/// The product id the CLI and the commander register with — one id on every
/// platform, the OS rides in the device profile.
pub const PRODUCT_ID_CLI: &str = "net.shareninja.cli";

/// The product id the web app registers with.
pub const PRODUCT_ID_WEB: &str = "net.shareninja.webui";

/// What a product id names, for display ("Share.Ninja for Mac", "Share.Ninja
/// web app"): the label around it is the UI's to word and translate, the
/// platform noun is a proper name and stays as it is in every language.
/// Mirrors `productName` in the web app's `sharing/domain/productName.ts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Product {
    Web,
    Cli,
    /// A native app, by the platform it is built for.
    Platform(&'static str),
    /// An id outside the table, shown as it is.
    Unknown(String),
}

/// Every shell has its own id: the Tauri desktop per OS, the SwiftUI Mac
/// app, the electron builds, iOS, Android, the web app, the CLI.
pub fn product(product_id: &str) -> Product {
    match product_id {
        PRODUCT_ID_WEB => Product::Web,
        PRODUCT_ID_CLI => Product::Cli,
        "net.shareninja.mac" | "net.shareninja.macsw" | "net.shareninja.elmac" => Product::Platform("Mac"),
        "net.shareninja.win" | "net.shareninja.elwin" => Product::Platform("Windows"),
        "net.shareninja.linux" => Product::Platform("Linux"),
        "net.shareninja.ios" => Product::Platform("iPhone"),
        "net.shareninja.droid" => Product::Platform("Android"),
        other => Product::Unknown(other.to_string()),
    }
}

/// Marketing platform name of a profile's `os`; empty for `Other`.
pub fn os_name(os: models::ws_device_profile::Os) -> &'static str {
    use models::ws_device_profile::Os;
    match os {
        Os::Mac => "macOS",
        Os::Win => "Windows",
        Os::Android => "Android",
        Os::Ios => "iOS",
        Os::Harmony => "HarmonyOS",
        Os::Other => "",
    }
}

/// Human device line for a device card / join request / nearby row: a native
/// app agent is its model ("MacBook Pro", same mapping as [`format_model`]);
/// a browser agent is always "<Browser> on <OS>" ("Safari on iOS", "Chrome on
/// Windows") — the UA's device (`iPhone`) is deliberately not used so every
/// platform reads the same way; a CLI agent (`product_id` ==
/// [`PRODUCT_ID_CLI`]) is "CLI on <OS>" the same way, since its glyph is the
/// terminal, not the platform. Empty when nothing is known.
pub fn device_card_label(device: &models::WsDeviceProfile, product_id: Option<&str>) -> String {
    let browser = browser_name(device.browser);
    let app = if !browser.is_empty() {
        browser
    } else if product_id == Some(PRODUCT_ID_CLI) {
        "CLI"
    } else {
        ""
    };
    if app.is_empty() {
        return device
            .model
            .as_deref()
            .filter(|m| !m.is_empty())
            .map(format_model)
            .unwrap_or_default()
            .to_string();
    }
    let os = os_name(device.os);
    if os.is_empty() {
        app.to_string()
    } else {
        format!("{app} on {os}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_model_maps_apple_identifiers() {
        assert_eq!(format_model("iPhone16,1"), "iPhone 15 Pro");
        assert_eq!(format_model("Mac16,5"), "MacBook Pro");
        assert_eq!(format_model("Mac13,1"), "Mac Studio");
    }

    #[test]
    fn format_model_passes_unknown_through() {
        assert_eq!(format_model("Latitude 7440"), "Latitude 7440");
        assert_eq!(format_model(""), "");
    }

    fn profile(
        os: models::ws_device_profile::Os,
        browser: Option<models::ws_device_profile::Browser>,
        model: Option<&str>,
    ) -> models::WsDeviceProfile {
        models::WsDeviceProfile {
            os,
            browser,
            model: model.map(str::to_string),
            form_factor: None,
            location: None,
        }
    }

    #[test]
    fn card_label_browser_is_browser_on_os() {
        use models::ws_device_profile::{Browser, Os};
        assert_eq!(
            device_card_label(&profile(Os::Ios, Some(Browser::Safari), Some("iPhone")), None),
            "Safari on iOS"
        );
        assert_eq!(
            device_card_label(&profile(Os::Win, Some(Browser::Chrome), None), None),
            "Chrome on Windows"
        );
        assert_eq!(
            device_card_label(&profile(Os::Other, Some(Browser::Edge), None), None),
            "Edge"
        );
        assert_eq!(
            device_card_label(&profile(Os::Mac, None, Some("Mac16,5")), None),
            "MacBook Pro"
        );
        assert_eq!(device_card_label(&profile(Os::Ios, None, None), None), "");
    }

    #[test]
    fn card_label_cli_is_cli_on_os() {
        use models::ws_device_profile::Os;
        assert_eq!(
            device_card_label(&profile(Os::Mac, None, None), Some(PRODUCT_ID_CLI)),
            "CLI on macOS"
        );
        assert_eq!(
            device_card_label(&profile(Os::Other, None, None), Some(PRODUCT_ID_CLI)),
            "CLI"
        );
        // A native app's product id changes nothing: the model is the line.
        assert_eq!(
            device_card_label(&profile(Os::Mac, None, Some("Mac16,5")), Some("net.shareninja.mac")),
            "MacBook Pro"
        );
    }
}
