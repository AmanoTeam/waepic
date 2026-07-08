use bytes::Bytes;
use std::fmt;

/// Media content to send with a message.
///
/// Each variant holds the raw file data. Upload, encryption, and
/// MIME-type detection happen internally when the message is sent.
#[derive(Clone, Debug)]
pub enum InputMedia {
    Image {
        caption: Option<String>,
        data: Bytes,
        mime_type: Option<String>,
    },
    Video {
        caption: Option<String>,
        data: Bytes,
        mime_type: Option<String>,
    },
    Audio {
        data: Bytes,
        mime_type: Option<String>,
        ptt: bool,
    },
    Document {
        data: Bytes,
        filename: Option<String>,
        mime_type: Option<String>,
    },
    Sticker {
        data: Bytes,
        mime_type: Option<String>,
    },
}

impl InputMedia {
    pub fn image(data: impl Into<Bytes>) -> Self {
        Self::Image {
            caption: None,
            data: data.into(),
            mime_type: None,
        }
    }

    pub fn video(data: impl Into<Bytes>) -> Self {
        Self::Video {
            caption: None,
            data: data.into(),
            mime_type: None,
        }
    }

    pub fn audio(data: impl Into<Bytes>) -> Self {
        Self::Audio {
            data: data.into(),
            mime_type: None,
            ptt: false,
        }
    }

    pub fn document(data: impl Into<Bytes>) -> Self {
        Self::Document {
            data: data.into(),
            filename: None,
            mime_type: None,
        }
    }

    pub fn sticker(data: impl Into<Bytes>) -> Self {
        Self::Sticker {
            data: data.into(),
            mime_type: None,
        }
    }

    #[must_use]
    pub fn with_caption(mut self, caption: impl Into<String>) -> Self {
        match &mut self {
            Self::Image { caption: c, .. } | Self::Video { caption: c, .. } => {
                *c = Some(caption.into());
            }
            _ => {}
        }
        self
    }

    #[must_use]
    pub fn with_mime_type(mut self, mime_type: impl Into<String>) -> Self {
        match &mut self {
            Self::Image { mime_type: m, .. }
            | Self::Video { mime_type: m, .. }
            | Self::Audio { mime_type: m, .. }
            | Self::Document { mime_type: m, .. }
            | Self::Sticker { mime_type: m, .. } => {
                *m = Some(mime_type.into());
            }
        }
        self
    }

    #[must_use]
    pub fn with_filename(mut self, filename: impl Into<String>) -> Self {
        if let Self::Document { filename: f, .. } = &mut self {
            *f = Some(filename.into());
        }
        self
    }

    #[must_use]
    pub fn with_ptt(mut self, ptt: bool) -> Self {
        if let Self::Audio { ptt: p, .. } = &mut self {
            *p = ptt;
        }
        self
    }

    pub fn data(&self) -> &Bytes {
        match self {
            Self::Image { data, .. }
            | Self::Video { data, .. }
            | Self::Audio { data, .. }
            | Self::Document { data, .. }
            | Self::Sticker { data, .. } => data,
        }
    }

    pub fn mime_type(&self) -> Option<&str> {
        match self {
            Self::Image { mime_type, .. }
            | Self::Video { mime_type, .. }
            | Self::Audio { mime_type, .. }
            | Self::Document { mime_type, .. }
            | Self::Sticker { mime_type, .. } => mime_type.as_deref(),
        }
    }
}

impl fmt::Display for InputMedia {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Image { .. } => write!(f, "image"),
            Self::Video { .. } => write!(f, "video"),
            Self::Audio { ptt, .. } => {
                if *ptt {
                    write!(f, "voice note")
                } else {
                    write!(f, "audio")
                }
            }
            Self::Document { .. } => write!(f, "document"),
            Self::Sticker { .. } => write!(f, "sticker"),
        }
    }
}

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
        assert!(img.mime_type().is_none());
        assert_eq!(img.data().as_ref(), &[1, 2, 3]);
        assert_eq!(img.to_string(), "image");
    }

    #[test]
    fn input_media_with_caption() {
        let vid = InputMedia::video(vec![0u8; 10]).with_caption("cool video");
        match vid {
            InputMedia::Video { caption, .. } => {
                assert_eq!(caption.as_deref(), Some("cool video"));
            }
            _ => panic!("expected Video"),
        }
    }

    #[test]
    fn input_media_with_mime() {
        let doc = InputMedia::document(vec![0u8; 5]).with_mime_type("application/pdf");
        assert_eq!(doc.mime_type(), Some("application/pdf"));
    }

    #[test]
    fn input_media_with_filename() {
        let doc = InputMedia::document(vec![0u8; 5]).with_filename("readme.md");
        match doc {
            InputMedia::Document { filename, .. } => {
                assert_eq!(filename.as_deref(), Some("readme.md"));
            }
            _ => panic!("expected Document"),
        }
    }

    #[test]
    fn input_media_ptt() {
        let audio = InputMedia::audio(vec![0u8; 10]).with_ptt(true);
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
