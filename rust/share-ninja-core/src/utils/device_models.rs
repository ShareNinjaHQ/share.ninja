//! Apple device model identifier -> marketing name. Generated from the AppleDB
//! open dataset (https://api.appledb.dev/device/main.json), which matches the
//! identifier lists on Apple support pages (e.g. support.apple.com/en-us/108052).
//! Plain product names: connectivity variants and every parenthetical
//! (size/chip/date) are trimmed off.
//!
//! Twin of the TS table in `packages/sharon-lib/src/utils/deviceModels.ts` —
//! regenerate BOTH together when new hardware ships. Unknown identifiers fall
//! back to the raw string in `format_model`, so a stale table degrades rather
//! than misreports.

/// Marketing name for an Apple model identifier, or `None` if unknown.
pub fn apple_model_name(model: &str) -> Option<&'static str> {
    Some(match model {
        "Mac13,1" | "Mac13,2" => "Mac Studio",
        "Mac14,2" => "MacBook Air",
        "Mac14,3" => "Mac mini",
        "Mac14,5" | "Mac14,6" | "Mac14,7" => "MacBook Pro",
        "Mac14,8" => "Mac Pro",
        "Mac14,9" | "Mac14,10" => "MacBook Pro",
        "Mac14,12" => "Mac mini",
        "Mac14,13" | "Mac14,14" => "Mac Studio",
        "Mac14,15" | "Mac15,1" | "Mac15,2" => "MacBook Air",
        "Mac15,3" => "MacBook Pro",
        "Mac15,4" | "Mac15,5" => "iMac",
        "Mac15,6" | "Mac15,7" | "Mac15,8" | "Mac15,9" | "Mac15,10" | "Mac15,11" => "MacBook Pro",
        "Mac15,12" | "Mac15,13" => "MacBook Air",
        "Mac15,14" => "Mac Studio",
        "Mac16,1" => "MacBook Pro",
        "Mac16,2" | "Mac16,3" => "iMac",
        "Mac16,5" | "Mac16,6" | "Mac16,7" | "Mac16,8" => "MacBook Pro",
        "Mac16,9" => "Mac Studio",
        "Mac16,10" | "Mac16,11" => "Mac mini",
        "Mac16,12" | "Mac16,13" => "MacBook Air",
        "Mac17,2" => "MacBook Pro",
        "Mac17,3" | "Mac17,4" => "MacBook Air",
        "Mac17,5" => "MacBook Neo",
        "Mac17,6" | "Mac17,7" | "Mac17,8" | "Mac17,9" => "MacBook Pro",
        "MacBook1,1" | "MacBook2,1" | "MacBook3,1" | "MacBook4,1" | "MacBook5,1" | "MacBook5,2" | "MacBook6,1"
        | "MacBook7,1" | "MacBook8,1" | "MacBook9,1" | "MacBook10,1" => "MacBook",
        "MacBookAir1,1" | "MacBookAir2,1" | "MacBookAir3,1" | "MacBookAir3,2" | "MacBookAir4,1" | "MacBookAir4,2"
        | "MacBookAir5,1" | "MacBookAir5,2" | "MacBookAir6,1" | "MacBookAir6,2" | "MacBookAir7,1" | "MacBookAir7,2"
        | "MacBookAir8,1" | "MacBookAir8,2" | "MacBookAir9,1" | "MacBookAir10,1" => "MacBook Air",
        "MacBookPro1,1" | "MacBookPro1,2" | "MacBookPro2,1" | "MacBookPro2,2" | "MacBookPro3,1" | "MacBookPro4,1"
        | "MacBookPro5,1" | "MacBookPro5,2" | "MacBookPro5,3" | "MacBookPro5,4" | "MacBookPro5,5" | "MacBookPro6,1"
        | "MacBookPro6,2" | "MacBookPro7,1" | "MacBookPro8,1" | "MacBookPro8,2" | "MacBookPro8,3" | "MacBookPro9,1"
        | "MacBookPro9,2" | "MacBookPro10,1" | "MacBookPro10,2" | "MacBookPro11,1" | "MacBookPro11,2"
        | "MacBookPro11,3" | "MacBookPro11,4" | "MacBookPro11,5" | "MacBookPro12,1" | "MacBookPro13,1"
        | "MacBookPro13,2" | "MacBookPro13,3" | "MacBookPro14,1" | "MacBookPro14,2" | "MacBookPro14,3"
        | "MacBookPro15,1" | "MacBookPro15,2" | "MacBookPro15,3" | "MacBookPro15,4" | "MacBookPro16,1"
        | "MacBookPro16,2" | "MacBookPro16,3" | "MacBookPro16,4" | "MacBookPro17,1" | "MacBookPro18,1"
        | "MacBookPro18,2" | "MacBookPro18,3" | "MacBookPro18,4" => "MacBook Pro",
        "MacPro1,1" | "MacPro2,1" | "MacPro3,1" | "MacPro4,1" | "MacPro5,1" | "MacPro6,1" | "MacPro7,1" => "Mac Pro",
        "Macmini1,1" | "Macmini2,1" | "Macmini3,1" | "Macmini4,1" | "Macmini5,1" | "Macmini5,2" | "Macmini5,3"
        | "Macmini6,1" | "Macmini6,2" | "Macmini6,3" | "Macmini7,1" | "Macmini7,2" | "Macmini7,3" | "Macmini8,1"
        | "Macmini8,2" | "Macmini9,1" | "Macmini10,1" | "Macmini10,2" => "Mac mini",
        "iMac,1" | "iMac4,1" | "iMac4,2" | "iMac5,1" | "iMac5,2" | "iMac6,1" | "iMac7,1" | "iMac8,1" | "iMac9,1"
        | "iMac10,1" | "iMac11,1" | "iMac11,2" | "iMac11,3" | "iMac12,1" | "iMac12,2" | "iMac13,1" | "iMac13,2"
        | "iMac13,3" | "iMac14,1" | "iMac14,2" | "iMac14,3" | "iMac14,4" | "iMac15,1" | "iMac16,1" | "iMac16,2"
        | "iMac17,1" | "iMac18,1" | "iMac18,2" | "iMac18,3" | "iMac19,1" | "iMac19,2" | "iMac20,1" | "iMac20,2"
        | "iMac21,1" | "iMac21,2" => "iMac",
        "iPad1,1" => "iPad",
        "iPad2,1" => "iPad 2",
        "iPad2,2" | "iPad2,3" => "iPad 2 + 3G",
        "iPad2,4" => "iPad 2",
        "iPad2,5" | "iPad2,6" | "iPad2,7" => "iPad mini",
        "iPad3,1" | "iPad3,2" | "iPad3,3" | "iPad3,4" | "iPad3,5" | "iPad3,6" => "iPad",
        "iPad4,1" | "iPad4,2" | "iPad4,3" => "iPad Air",
        "iPad4,4" | "iPad4,5" | "iPad4,6" => "iPad mini 2",
        "iPad4,7" | "iPad4,8" | "iPad4,9" => "iPad mini 3",
        "iPad5,1" | "iPad5,2" => "iPad mini 4",
        "iPad5,3" | "iPad5,4" => "iPad Air 2",
        "iPad5,5" | "iPad5,6" => "iPad Pro",
        "iPad6,1" | "iPad6,2" => "iPad mini",
        "iPad6,3" | "iPad6,4" | "iPad6,7" | "iPad6,8" => "iPad Pro",
        "iPad6,11" | "iPad6,12" => "iPad",
        "iPad7,1" | "iPad7,2" | "iPad7,3" | "iPad7,4" => "iPad Pro",
        "iPad7,5" | "iPad7,6" | "iPad7,11" | "iPad7,12" => "iPad",
        "iPad8,1" | "iPad8,2" | "iPad8,3" | "iPad8,4" | "iPad8,5" | "iPad8,6" | "iPad8,7" | "iPad8,8" | "iPad8,9"
        | "iPad8,10" | "iPad8,11" | "iPad8,12" => "iPad Pro",
        "iPad11,1" | "iPad11,2" => "iPad mini",
        "iPad11,3" | "iPad11,4" => "iPad Air",
        "iPad11,6" | "iPad11,7" | "iPad12,1" | "iPad12,2" => "iPad",
        "iPad13,1" | "iPad13,2" => "iPad Air",
        "iPad13,4" | "iPad13,5" | "iPad13,6" | "iPad13,7" | "iPad13,8" | "iPad13,9" | "iPad13,10" | "iPad13,11" => {
            "iPad Pro"
        }
        "iPad13,16" | "iPad13,17" => "iPad Air",
        "iPad13,18" | "iPad13,19" | "iPad13,20" | "iPad13,21" => "iPad",
        "iPad14,1" | "iPad14,2" => "iPad mini",
        "iPad14,3" | "iPad14,4" | "iPad14,5" | "iPad14,6" => "iPad Pro",
        "iPad14,8" | "iPad14,9" | "iPad14,10" | "iPad14,11" | "iPad15,3" | "iPad15,4" | "iPad15,5" | "iPad15,6" => {
            "iPad Air"
        }
        "iPad15,7" | "iPad15,8" => "iPad",
        "iPad16,1" | "iPad16,2" => "iPad mini",
        "iPad16,3" | "iPad16,4" | "iPad16,5" | "iPad16,6" => "iPad Pro",
        "iPad16,8" | "iPad16,9" | "iPad16,10" | "iPad16,11" => "iPad Air",
        "iPad17,1" | "iPad17,2" | "iPad17,3" | "iPad17,4" => "iPad Pro",
        "iPhone1,1" => "iPhone",
        "iPhone1,2" => "iPhone 3G",
        "iPhone2,1" => "iPhone 3GS",
        "iPhone3,1" | "iPhone3,2" | "iPhone3,3" => "iPhone 4",
        "iPhone4,1" => "iPhone 4S",
        "iPhone5,1" | "iPhone5,2" => "iPhone 5",
        "iPhone5,3" | "iPhone5,4" => "iPhone 5c",
        "iPhone6,1" | "iPhone6,2" => "iPhone 5s",
        "iPhone7,1" => "iPhone 6 Plus",
        "iPhone7,2" => "iPhone 6",
        "iPhone8,1" => "iPhone 6s",
        "iPhone8,2" => "iPhone 6s Plus",
        "iPhone8,4" => "iPhone SE",
        "iPhone9,1" => "iPhone 7",
        "iPhone9,2" => "iPhone 7 Plus",
        "iPhone9,3" => "iPhone 7",
        "iPhone9,4" => "iPhone 7 Plus",
        "iPhone10,1" => "iPhone 8",
        "iPhone10,2" => "iPhone 8 Plus",
        "iPhone10,3" => "iPhone X",
        "iPhone10,4" => "iPhone 8",
        "iPhone10,5" => "iPhone 8 Plus",
        "iPhone10,6" => "iPhone X",
        "iPhone11,2" => "iPhone XS",
        "iPhone11,4" | "iPhone11,6" => "iPhone XS Max",
        "iPhone11,8" => "iPhone XR",
        "iPhone12,1" => "iPhone 11",
        "iPhone12,3" => "iPhone 11 Pro",
        "iPhone12,5" => "iPhone 11 Pro Max",
        "iPhone12,8" => "iPhone SE",
        "iPhone13,1" => "iPhone 12 mini",
        "iPhone13,2" => "iPhone 12",
        "iPhone13,3" => "iPhone 12 Pro",
        "iPhone13,4" => "iPhone 12 Pro Max",
        "iPhone14,2" => "iPhone 13 Pro",
        "iPhone14,3" => "iPhone 13 Pro Max",
        "iPhone14,4" => "iPhone 13 mini",
        "iPhone14,5" => "iPhone 13",
        "iPhone14,6" => "iPhone SE",
        "iPhone14,7" => "iPhone 14",
        "iPhone14,8" => "iPhone 14 Plus",
        "iPhone15,2" => "iPhone 14 Pro",
        "iPhone15,3" => "iPhone 14 Pro Max",
        "iPhone15,4" => "iPhone 15",
        "iPhone15,5" => "iPhone 15 Plus",
        "iPhone16,1" => "iPhone 15 Pro",
        "iPhone16,2" => "iPhone 15 Pro Max",
        "iPhone17,1" => "iPhone 16 Pro",
        "iPhone17,2" => "iPhone 16 Pro Max",
        "iPhone17,3" => "iPhone 16",
        "iPhone17,4" => "iPhone 16 Plus",
        "iPhone17,5" => "iPhone 16e",
        "iPhone18,1" => "iPhone 17 Pro",
        "iPhone18,2" => "iPhone 17 Pro Max",
        "iPhone18,3" => "iPhone 17",
        "iPhone18,4" => "iPhone Air",
        "iPhone18,5" => "iPhone 17e",
        "iPhone99,11" => "virtual machine for iPhone Research Environment",
        _ => return None,
    })
}
