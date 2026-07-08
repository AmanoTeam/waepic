use crate::InputMedia;

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
pub struct InputMessage {
    pub(crate) text: Option<String>,
    pub(crate) reply_to: Option<String>,
    pub(crate) silent: bool,
    pub(crate) link_preview: bool,
    pub(crate) media: Option<InputMedia>,
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

    /// Set the media content for this message.
    #[must_use]
    pub fn media(mut self, media: InputMedia) -> Self {
        self.media = Some(media);
        self
    }

    /// Set the message ID this message should reply to, if any.
    #[must_use]
    pub fn reply_to(mut self, reply_to: Option<impl Into<String>>) -> Self {
        self.reply_to = reply_to.map(Into::into);
        self
    }

    /// Whether the message should be sent silently (no notification).
    #[must_use]
    pub fn silent(mut self, silent: bool) -> Self {
        self.silent = silent;
        self
    }

    /// Whether a link preview should be generated for URLs in the text.
    #[must_use]
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
            media: None,
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
        assert!(msg.media.is_none());
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

    #[test]
    fn builder_with_media() {
        let media = InputMedia::image(vec![0u8; 100]);
        let msg = InputMessage::text("caption").media(media);
        assert!(msg.media.is_some());
        assert_eq!(msg.text, Some("caption".to_string()));
    }

    #[test]
    fn input_media_defaults() {
        let img = InputMedia::image(vec![1, 2, 3]);
        assert_eq!(img.data().as_ref(), &[1, 2, 3]);
        assert_eq!(img.to_string(), "image");
    }

    #[test]
    fn input_media_with_caption() {
        let vid = InputMedia::video(vec![0u8; 10]).caption("cool video");
        match vid {
            InputMedia::Video { caption, .. } => {
                assert_eq!(caption.as_deref(), Some("cool video"));
            }
            _ => panic!("expected Video"),
        }
    }

    #[test]
    fn input_media_with_filename() {
        let doc = InputMedia::document(vec![0u8; 5]).filename("readme.md");
        match doc {
            InputMedia::Document { filename, .. } => {
                assert_eq!(filename.as_deref(), Some("readme.md"));
            }
            _ => panic!("expected Document"),
        }
    }

    #[test]
    fn input_media_ptt() {
        let audio = InputMedia::audio(vec![0u8; 10]).ptt(true);
        assert_eq!(audio.to_string(), "voice note");
        let audio2 = InputMedia::audio(vec![0u8; 10]);
        assert_eq!(audio2.to_string(), "audio");
    }

    #[test]
    fn input_media_display() {
        assert_eq!(InputMedia::image(vec![]).to_string(), "image");
        assert_eq!(InputMedia::video(vec![]).to_string(), "video");
        assert_eq!(InputMedia::document(vec![]).to_string(), "document");
        assert_eq!(InputMedia::sticker(vec![]).to_string(), "sticker");
    }
}
