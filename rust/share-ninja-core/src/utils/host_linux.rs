//! Linux host identity for agent registration — the counterpart of the macOS
//! Computer Name / `hw.model` and the Windows WMI reads in the shells. Plain
//! file reads, all world-readable: no root, no D-Bus, no subprocess. Shared by
//! the desktop shell (`commands::os_info`) and the CLI (`share-ninja-client`).
//!
//! The parsers take the file contents so they are testable on any host.

use std::fs;

/// The name other people see: the pretty hostname set in Settings → About
/// (`hostnamectl set-hostname --pretty`, stored as `PRETTY_HOSTNAME` in
/// `/etc/machine-info`), else the static hostname — the counterpart of the
/// macOS Computer Name and Android's Device name.
pub fn machine_name() -> Option<String> {
    fs::read_to_string("/etc/machine-info")
        .ok()
        .and_then(|s| env_value(&s, "PRETTY_HOSTNAME"))
        .or_else(|| fs::read_to_string("/proc/sys/kernel/hostname").ok().and_then(|s| non_empty(&s)))
}

/// Distribution and release, e.g. "Debian GNU/Linux 13", "Ubuntu 24.04",
/// "Fedora Linux 42", from os-release(5). A bare version number says nothing
/// on Linux, so the distribution name always leads; a rolling release without
/// `VERSION_ID` is just its name ("Arch Linux").
pub fn os_version() -> Option<String> {
    ["/etc/os-release", "/usr/lib/os-release"]
        .iter()
        .find_map(|p| fs::read_to_string(p).ok())
        .and_then(|s| parse_os_release(&s))
}

/// Hardware line under the name, vendor and model as the firmware spells them
/// ("Dell Latitude 7440", "LENOVO ThinkPad X1 Carbon Gen 11", "Raspberry Pi 5
/// Model B Rev 1.0"): DMI on PCs, the device tree on ARM boards. `None` when
/// the firmware only carries placeholders — the core then omits
/// `device_model`, per the contract ("absent when unknown").
pub fn device_model() -> Option<String> {
    let dmi = |f: &str| fs::read_to_string(format!("/sys/class/dmi/id/{f}")).ok();
    dmi_model(
        dmi("sys_vendor").as_deref(),
        dmi("product_name").as_deref(),
        dmi("product_version").as_deref(),
    )
    .or_else(|| {
        fs::read("/sys/firmware/devicetree/base/model")
            .ok()
            .and_then(|b| clean_firmware_value(&String::from_utf8_lossy(&b)))
    })
}

/// Firmware placeholders that OEMs ship instead of leaving the field blank.
/// They look like real data and are identical across every board of that
/// vendor, so they never become a model. Compared case-insensitively.
const OEM_PLACEHOLDERS: &[&str] = &[
    "default string",
    "to be filled by o.e.m.",
    "to be filled by oem",
    "system serial number",
    "system manufacturer",
    "system product name",
    "system version",
    "not applicable",
    "not specified",
    "none",
    "o.e.m.",
    "unknown",
    "0",
];

/// Trimmed (NULs too — device-tree strings are NUL-terminated), `None` for
/// blanks and [`OEM_PLACEHOLDERS`].
fn clean_firmware_value(v: &str) -> Option<String> {
    let s = v.trim_matches(|c: char| c.is_whitespace() || c == '\0');
    if s.is_empty() || OEM_PLACEHOLDERS.contains(&s.to_lowercase().as_str()) {
        return None;
    }
    Some(s.to_string())
}

fn non_empty(v: &str) -> Option<String> {
    let s = v.trim();
    (!s.is_empty()).then(|| s.to_string())
}

fn dmi_model(vendor: Option<&str>, name: Option<&str>, version: Option<&str>) -> Option<String> {
    let vendor = vendor.and_then(clean_firmware_value);
    let mut product = name.and_then(clean_firmware_value);
    // Lenovo puts the machine-type code in product_name ("21HMCTO1WW") and the
    // marketing name in product_version ("ThinkPad X1 Carbon Gen 11").
    if vendor.as_deref().is_some_and(|v| v.eq_ignore_ascii_case("lenovo"))
        && let Some(v) = version.and_then(clean_firmware_value)
    {
        product = Some(v);
    }
    let product = product?;
    // The vendor's short name: "Dell Inc." → "Dell", "ASUSTeK COMPUTER INC."
    // → "ASUSTeK"; dropped when the product already names it ("HP EliteBook
    // 840 G10", "VMware Virtual Platform").
    let brand = vendor
        .as_deref()
        .and_then(|v| v.split_whitespace().next())
        .map(|w| w.trim_end_matches(','))
        .filter(|w| !w.is_empty());
    match brand {
        Some(b) if !product.to_lowercase().contains(&b.to_lowercase()) => Some(format!("{b} {product}")),
        _ => Some(product),
    }
}

fn parse_os_release(s: &str) -> Option<String> {
    let name = env_value(s, "NAME");
    let version = env_value(s, "VERSION_ID");
    match (name, version) {
        (Some(n), Some(v)) => Some(format!("{n} {v}")),
        (Some(n), None) => Some(n),
        (None, _) => env_value(s, "PRETTY_NAME"),
    }
}

/// `KEY=value` from an env-style file (os-release, machine-info): values may
/// be single- or double-quoted, `#` lines are comments; empty counts as unset.
fn env_value(s: &str, key: &str) -> Option<String> {
    s.lines().find_map(|line| {
        let (k, v) = line.trim().split_once('=')?;
        if k.trim() != key {
            return None;
        }
        let v = v.trim();
        let v = v
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .or_else(|| v.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(v);
        non_empty(&v.replace("\\\"", "\"").replace("\\\\", "\\"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_release_name_and_version() {
        let debian = "PRETTY_NAME=\"Debian GNU/Linux 13 (trixie)\"\nNAME=\"Debian GNU/Linux\"\nVERSION_ID=\"13\"\n";
        assert_eq!(parse_os_release(debian).as_deref(), Some("Debian GNU/Linux 13"));
        let arch = "NAME=\"Arch Linux\"\nPRETTY_NAME=\"Arch Linux\"\nBUILD_ID=rolling\n";
        assert_eq!(parse_os_release(arch).as_deref(), Some("Arch Linux"));
        assert_eq!(parse_os_release("PRETTY_NAME='Custom OS'\n").as_deref(), Some("Custom OS"));
        assert_eq!(parse_os_release("# nothing\n"), None);
    }

    #[test]
    fn machine_info_pretty_hostname() {
        let s = "# comment\nPRETTY_HOSTNAME=\"Philip's Laptop\"\nICON_NAME=computer-laptop\n";
        assert_eq!(env_value(s, "PRETTY_HOSTNAME").as_deref(), Some("Philip's Laptop"));
        assert_eq!(env_value("PRETTY_HOSTNAME=\"\"\n", "PRETTY_HOSTNAME"), None);
    }

    #[test]
    fn dmi_vendor_and_product() {
        assert_eq!(dmi_model(Some("Dell Inc.\n"), Some("Latitude 7440\n"), None).as_deref(), Some("Dell Latitude 7440"));
        assert_eq!(dmi_model(Some("HP"), Some("HP EliteBook 840 G10"), None).as_deref(), Some("HP EliteBook 840 G10"));
        assert_eq!(
            dmi_model(Some("VMware, Inc."), Some("VMware Virtual Platform"), Some("None")).as_deref(),
            Some("VMware Virtual Platform")
        );
        assert_eq!(
            dmi_model(Some("LENOVO"), Some("21HMCTO1WW"), Some("ThinkPad X1 Carbon Gen 11")).as_deref(),
            Some("LENOVO ThinkPad X1 Carbon Gen 11")
        );
        assert_eq!(dmi_model(Some("LENOVO"), Some("21HMCTO1WW"), Some("")).as_deref(), Some("LENOVO 21HMCTO1WW"));
        assert_eq!(dmi_model(Some("To be filled by O.E.M."), Some("Default string"), None), None);
        assert_eq!(dmi_model(Some("System manufacturer"), Some("B550 AORUS"), None).as_deref(), Some("B550 AORUS"));
    }

    #[test]
    fn device_tree_model_is_nul_terminated() {
        assert_eq!(clean_firmware_value("Raspberry Pi 5 Model B Rev 1.0\0").as_deref(), Some("Raspberry Pi 5 Model B Rev 1.0"));
    }
}
