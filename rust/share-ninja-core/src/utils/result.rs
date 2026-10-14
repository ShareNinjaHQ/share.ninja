//! Result codes for operations

/// Result codes matching the TypeScript implementation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Result {
    OK = 0,
    Fail = -1,
    EOF = -2,
    InvalidHandle = -3,
    NoPermission = -4,
    BadPath = -5,
    FileExists = -8,
    NoSuchFile = -9,
    QuotaExceeded = -10,
    NoFreeSpace = -11,
    SharingViolation = -13,
    ImpersonationFailed = -15,
    Incomplete = -16,
    TooManyOpenFiles = -17,
    NotImpl = -18,
    Timeout = -19,
    Cancelled = -20,

    // WebRTC errors
    WebRTCFailParseSDPError = -100,
    WebRTCFailConnectionClose = -101,
    WebRTCFailConnectionFail = -102,
    WebRTCFailDisconnected = -103,
    WebRTCFailOfferCreateFail = -104,
    WebRTCFailSerializeSDPError = -105,
    WebRTCFailSetSession = -106,
    WebRTCFailAddTrack = -107,
    WebRTCFailSetLocalSession = -108,
    WebRTCFailAddIceCandidate = -109,
    WebRTCFailParseIceCandidate = -110,
    WebRTCFailParseJson = -111,
    WebRTCFailServiceError = -112,

    // Non-WebRTC generic error
    WebRTCFailGenericError = -150,

    // Sharon-specific errors
    SHGenericFail = -200,
    PartyOffline = -201,
    PartyNoShare = -202,
    SignatureFail = -203,
    PartyNotFound = -204,
    BadFileName = -210,
}

impl Result {
    pub fn is_ok(&self) -> bool {
        matches!(self, Result::OK)
    }

    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }
}

impl std::fmt::Display for Result {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
