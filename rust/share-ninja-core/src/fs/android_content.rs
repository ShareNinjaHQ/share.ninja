//! Android: open a picked `content://` file via the app's `ContentResolver` over
//! JNI, returning an owned fd — so we upload it **without copying** into the cache
//! (the analogue of the iOS security-scoped bookmark). Files only; folder
//! (`OpenDocumentTree`) traversal via SAF is not handled here.
//!
//! The JavaVM + app `Context` are published into `ndk_context` by the droid native
//! crate at startup (see `droid/native/src/tls_init.rs`). We attach the calling
//! (tokio worker) thread to the VM for the duration of the call.

use std::ffi::CStr;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};

use jni::JavaVM;
use jni::objects::{JObject, JValue};
use jni::signature::RuntimeMethodSignature;
use jni::strings::JNIStr;
use jni::sys::jobject;

fn io_err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

fn name(cstr: &CStr) -> &JNIStr {
    // ASCII method/class names are always valid modified UTF-8.
    JNIStr::from_cstr(cstr).expect("ascii identifier is valid mutf8")
}

/// True if `path` is an Android `content://` URI rather than a filesystem path.
pub fn is_content_uri(path: &str) -> bool {
    path.starts_with("content://")
}

/// Open `content://…` for reading and return an owned fd (closed on drop). Reads
/// go through this fd; size comes from `fstat`, so no extra JNI round-trips.
pub fn open_fd(uri: &str) -> io::Result<OwnedFd> {
    let ctx = ndk_context::android_context();
    // SAFETY: ndk_context publishes a valid JavaVM pointer for the process.
    let vm = unsafe { JavaVM::from_raw(ctx.vm().cast()) };
    let context_ptr = ctx.context() as jobject;

    let fd: RawFd = vm
        .attach_current_thread(|env| -> jni::errors::Result<RawFd> {
            // SAFETY: ndk_context hands us the app Context jobject, valid for the
            // process lifetime; only used within this attached scope.
            let context = unsafe { JObject::from_raw(env, context_ptr) };

            // Uri uri = Uri.parse(uriStr)
            let uri_str: JObject = env.new_string(uri)?.into();
            let uri_class = env.find_class(name(c"android/net/Uri"))?;
            let parse_sig = RuntimeMethodSignature::from_str("(Ljava/lang/String;)Landroid/net/Uri;")?;
            let juri = env
                .call_static_method(
                    &uri_class,
                    name(c"parse"),
                    parse_sig.method_signature(),
                    &[JValue::Object(&uri_str)],
                )?
                .l()?;

            // ContentResolver cr = context.getContentResolver()
            let cr_sig = RuntimeMethodSignature::from_str("()Landroid/content/ContentResolver;")?;
            let cr = env
                .call_method(&context, name(c"getContentResolver"), cr_sig.method_signature(), &[])?
                .l()?;

            // ParcelFileDescriptor pfd = cr.openFileDescriptor(uri, "r")
            let mode: JObject = env.new_string("r")?.into();
            let ofd_sig = RuntimeMethodSignature::from_str(
                "(Landroid/net/Uri;Ljava/lang/String;)Landroid/os/ParcelFileDescriptor;",
            )?;
            let pfd = env
                .call_method(
                    &cr,
                    name(c"openFileDescriptor"),
                    ofd_sig.method_signature(),
                    &[JValue::Object(&juri), JValue::Object(&mode)],
                )?
                .l()?;
            if pfd.is_null() {
                return Err(jni::errors::Error::NullPtr("openFileDescriptor returned null"));
            }

            // int fd = pfd.detachFd()  — transfers ownership to us
            let detach_sig = RuntimeMethodSignature::from_str("()I")?;
            let raw = env
                .call_method(&pfd, name(c"detachFd"), detach_sig.method_signature(), &[])?
                .i()?;
            Ok(raw as RawFd)
        })
        .map_err(io_err)?;

    if fd < 0 {
        return Err(io::Error::other("detachFd returned an invalid descriptor"));
    }
    // SAFETY: detachFd transferred ownership of a fresh fd to us.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Metadata of the `content://` file (via `fstat` on a freshly opened fd — no
/// extra JNI/query round-trip): its size, and the mtime its etag is built from.
pub fn metadata(uri: &str) -> io::Result<std::fs::Metadata> {
    let fd = open_fd(uri)?;
    std::fs::File::from(fd).metadata()
}
