/// A builder for constructing messages to send.
///
/// # Examples
///
/// ```
/// use waepic_client::message::input_message::InputMessage;
///
/// let msg = InputMessage::text("hello")
///     .silent(true);
/// ```
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct InputMessage {
    text: Option<String>,
    reply_to: Option<String>,
    silent: bool,
    link_preview: bool,
}

impl InputMessage {
    /// Create a new empty `InputMessage`.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates a new `InputMessage` with a text.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            ..Default::default()
        }
    }

    /// Set the message ID this message should reply to, if any.
    pub fn reply_to(mut self, reply_to: Option<impl Into<String>>) -> Self {
        self.reply_to = reply_to.map(Into::into);
        self
    }

    /// Whether the message should be sent silently (no notification).
    pub fn silent(mut self, silent: bool) -> Self {
        self.silent = silent;
        self
    }

    /// Whether a link preview should be generated for URLs in the text.
    pub fn link_preview(mut self, link_preview: bool) -> Self {
        self.link_preview = link_preview;
        self
    }
}

impl Default for InputMessage {
    fn default() -> Self {
        Self {
            text: None,
            reply_to: None,
            silent: false,
            link_preview: true,
        }
    }
}

impl From<String> for InputMessage {
    fn from(text: String) -> Self {
        Self::text(text)
    }
}

impl From<&str> for InputMessage {
    fn from(text: &str) -> Self {
        Self::text(text.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_sets_text() {
        let msg = InputMessage::text("hello");
        assert_eq!(msg.text, Some("hello".to_string()));
    }

    #[test]
    fn builder_defaults() {
        let msg = InputMessage::empty();
        assert_eq!(msg.text, None);
        assert_eq!(msg.reply_to, None);
        assert!(!msg.silent);
        assert!(msg.link_preview);
    }

    #[test]
    fn builder_silent_and_no_link_preview() {
        let msg = InputMessage::text("check this out https://example.com")
            .silent(true)
            .link_preview(false);
        assert!(msg.silent);
        assert!(!msg.link_preview);
    }

    #[test]
    fn builder_reply_to() {
        let msg = InputMessage::text("reply").reply_to(Some("abc123"));
        assert_eq!(msg.reply_to, Some("abc123".to_string()));
    }

    #[test]
    fn builder_reply_to_none() {
        let msg = InputMessage::text("no reply").reply_to(None::<String>);
        assert_eq!(msg.reply_to, None);
    }

    #[test]
    fn from_string_sets_text() {
        let msg = InputMessage::from("test".to_string());
        assert_eq!(msg.text, Some("test".to_string()));
    }

    #[test]
    fn from_str_sets_text() {
        let msg = InputMessage::from("test");
        assert_eq!(msg.text, Some("test".to_string()));
    }
}
