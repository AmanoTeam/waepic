use std::fmt;

use bytes::Bytes;

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
    pub fn caption(mut self, caption: impl Into<String>) -> Self {
        match &mut self {
            Self::Image { caption: c, .. } | Self::Video { caption: c, .. } => {
                *c = Some(caption.into());
            }
            _ => {}
        }
        self
    }

    #[must_use]
    pub fn mime_type(mut self, mime_type: impl Into<String>) -> Self {
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
    pub fn filename(mut self, filename: impl Into<String>) -> Self {
        if let Self::Document { filename: f, .. } = &mut self {
            *f = Some(filename.into());
        }
        self
    }

    #[must_use]
    pub fn ptt(mut self, ptt: bool) -> Self {
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
