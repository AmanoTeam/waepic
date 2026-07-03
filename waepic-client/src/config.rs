//! Client configuration: device properties, reconnect behavior, WebSocket URL.

use waproto::whatsapp::device_props::PlatformType;
use waepic_connection::ConnectionConfig;

/// Configuration for a WhatsApp client.
#[derive(Clone, Debug, Default)]
pub struct ClientConfiguration {
    /// Device properties sent during the WhatsApp handshake.
    pub device: DeviceProps,
    /// Connection-layer configuration (WebSocket URL, reconnect, keepalive).
    pub connection: ConnectionConfig,
}

/// Device properties sent during WhatsApp handshake.
#[derive(Clone, Debug)]
pub struct DeviceProps {
    /// Operating system name (e.g. "Linux", "macOS", "Windows").
    pub os: String,
    /// WhatsApp application version.
    pub version: AppVersion,
    /// Platform type reported to the server (Desktop, Mobile, etc.).
    pub platform_type: PlatformType,
}

impl DeviceProps {
    /// Default device properties for a desktop client.
    pub fn default_desktop() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            os: "Linux".to_string(),
            #[cfg(target_os = "macos")]
            os: "MacOS".to_string(),
            #[cfg(target_os = "windows")]
            os: "Windows".to_string(),
            version: AppVersion::default(),
            platform_type: PlatformType::Desktop,
        }
    }
}

impl Default for DeviceProps {
    fn default() -> Self {
        Self::default_desktop()
    }
}

/// WhatsApp application version.
#[derive(Clone, Debug)]
pub struct AppVersion {
    /// Major version number.
    pub primary: u16,
    /// Minor version number.
    pub secondary: u16,
    /// Patch version number.
    pub tertiary: u16,
}

impl Default for AppVersion {
    fn default() -> Self {
        Self {
            primary: 2,
            secondary: 25,
            tertiary: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use waproto::whatsapp::device_props::PlatformType;

    use super::*;

    #[test]
    fn default_config() {
        let config = ClientConfiguration::default();

        assert!(config.connection.auto_reconnect);
        assert_eq!(config.connection.keepalive_interval, Duration::from_secs(20));
        assert_eq!(config.connection.max_reconnect_attempts, 10);
    }

    #[test]
    fn default_device_props_desktop() {
        let props = DeviceProps::default_desktop();

        #[cfg(target_os = "linux")]
        assert_eq!(props.os, "Linux");
        #[cfg(target_os = "macos")]
        assert_eq!(props.os, "MacOS");
        #[cfg(target_os = "windows")]
        assert_eq!(props.os, "Windows");

        assert_eq!(props.version.primary, 2);
        assert_eq!(props.version.secondary, 25);
        assert_eq!(props.version.tertiary, 1);
        assert!(matches!(props.platform_type, PlatformType::Desktop));
    }

    #[test]
    fn default_app_version() {
        let version = AppVersion::default();

        assert_eq!(version.primary, 2);
        assert_eq!(version.secondary, 25);
        assert_eq!(version.tertiary, 1);
    }
}
