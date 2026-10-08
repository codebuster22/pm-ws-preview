//! A hand-rolled Node-API shim over [`crate::ffi`]'s C ABI: no `napi.h`, no build-time link
//! against `libnode`, no `napi`/`neon` crate. Node loads this `cdylib` directly with
//! `process.dlopen` and calls the bare export [`napi_register_module_v1`]; every `napi_*`
//! function is resolved at that moment via `dlsym(RTLD_DEFAULT, ...)` and cached as a
//! function pointer — nothing here links against a `napi_*` symbol at build time.
//!
//! Every JS function funnels through the `pmws_*` entry points in [`crate::ffi`] exactly as a
//! C caller would. A session crosses into JS as a `napi_external` wrapping a [`SessionBox`],
//! so a GC'd session cannot leak its mapping and an explicit `close()` cannot double-free it.
//! Node serializes every callback and finalizer onto the JS thread, so `SessionBox`'s [`Cell`]
//! needs no atomics.
//!
//! This module is boundary-only, like [`crate::ffi`] itself: every function here calls only
//! resolved `napi_*` functions and this crate's own `pmws_*` entry points, never `src/shm`'s
//! internals directly. Each function's own `// SAFETY:` covers every call inside it; the
//! recurring justification, stated once here rather than on every call site, is that `env`,
//! `info`, and every argument `napi_value` are the live handles Node passed into that
//! callback, and every out-pointer is a single writable local this function owns for exactly
//! the call's duration.
#![allow(unsafe_code)]

use crate::ffi::{
    self, PmwsDecimal, PmwsEvent, PmwsLevel, PmwsSegmentInfo, PmwsSession, PmwsState,
};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::mem::MaybeUninit;
use std::sync::OnceLock;

type NapiEnv = *mut c_void;
type NapiValue = *mut c_void;
type NapiCallbackInfo = *mut c_void;
type NapiStatus = c_int;
type NapiCallback = unsafe extern "C" fn(NapiEnv, NapiCallbackInfo) -> NapiValue;
type NapiFinalize = unsafe extern "C" fn(NapiEnv, *mut c_void, *mut c_void);

type FnCreateFunction = unsafe extern "C" fn(
    NapiEnv,
    *const c_char,
    usize,
    NapiCallback,
    *mut c_void,
    *mut NapiValue,
) -> NapiStatus;
type FnSetNamedProperty =
    unsafe extern "C" fn(NapiEnv, NapiValue, *const c_char, NapiValue) -> NapiStatus;
type FnGetCbInfo = unsafe extern "C" fn(
    NapiEnv,
    NapiCallbackInfo,
    *mut usize,
    *mut NapiValue,
    *mut NapiValue,
    *mut *mut c_void,
) -> NapiStatus;
type FnGetValueStringUtf8 =
    unsafe extern "C" fn(NapiEnv, NapiValue, *mut c_char, usize, *mut usize) -> NapiStatus;
type FnGetValueUint32 = unsafe extern "C" fn(NapiEnv, NapiValue, *mut u32) -> NapiStatus;
type FnGetValueDouble = unsafe extern "C" fn(NapiEnv, NapiValue, *mut f64) -> NapiStatus;
type FnGetValueBigintUint64 =
    unsafe extern "C" fn(NapiEnv, NapiValue, *mut u64, *mut bool) -> NapiStatus;
type FnCreateObject = unsafe extern "C" fn(NapiEnv, *mut NapiValue) -> NapiStatus;
type FnCreateArrayWithLength = unsafe extern "C" fn(NapiEnv, usize, *mut NapiValue) -> NapiStatus;
type FnSetElement = unsafe extern "C" fn(NapiEnv, NapiValue, u32, NapiValue) -> NapiStatus;
type FnCreateStringUtf8 =
    unsafe extern "C" fn(NapiEnv, *const c_char, usize, *mut NapiValue) -> NapiStatus;
type FnCreateUint32 = unsafe extern "C" fn(NapiEnv, u32, *mut NapiValue) -> NapiStatus;
type FnCreateBigintWords =
    unsafe extern "C" fn(NapiEnv, c_int, usize, *const u64, *mut NapiValue) -> NapiStatus;
type FnCreateBigintUint64 = unsafe extern "C" fn(NapiEnv, u64, *mut NapiValue) -> NapiStatus;
type FnGetUndefined = unsafe extern "C" fn(NapiEnv, *mut NapiValue) -> NapiStatus;
type FnGetNull = unsafe extern "C" fn(NapiEnv, *mut NapiValue) -> NapiStatus;
type FnThrowError = unsafe extern "C" fn(NapiEnv, *const c_char, *const c_char) -> NapiStatus;
type FnCreateExternal = unsafe extern "C" fn(
    NapiEnv,
    *mut c_void,
    NapiFinalize,
    *mut c_void,
    *mut NapiValue,
) -> NapiStatus;
type FnGetValueExternal = unsafe extern "C" fn(NapiEnv, NapiValue, *mut *mut c_void) -> NapiStatus;

unsafe extern "C" {
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
}

/// The pseudo-handle that makes `dlsym` search every image already loaded into the process.
/// Its value is platform ABI: `-2` on Darwin, the null handle on glibc — passing Darwin's
/// value to glibc's `dlsym` dereferences it as a real handle and crashes the host process.
#[cfg(target_os = "macos")]
const RTLD_DEFAULT: *mut c_void = -2isize as *mut c_void;
#[cfg(not(target_os = "macos"))]
const RTLD_DEFAULT: *mut c_void = std::ptr::null_mut();

struct NapiFns {
    create_function: FnCreateFunction,
    set_named_property: FnSetNamedProperty,
    get_cb_info: FnGetCbInfo,
    get_value_string_utf8: FnGetValueStringUtf8,
    get_value_uint32: FnGetValueUint32,
    get_value_double: FnGetValueDouble,
    get_value_bigint_uint64: FnGetValueBigintUint64,
    create_object: FnCreateObject,
    create_array_with_length: FnCreateArrayWithLength,
    set_element: FnSetElement,
    create_string_utf8: FnCreateStringUtf8,
    create_uint32: FnCreateUint32,
    create_bigint_words: FnCreateBigintWords,
    create_bigint_uint64: FnCreateBigintUint64,
    get_undefined: FnGetUndefined,
    get_null: FnGetNull,
    throw_error: FnThrowError,
    create_external: FnCreateExternal,
    get_value_external: FnGetValueExternal,
}

static NAPI: OnceLock<NapiFns> = OnceLock::new();

/// Resolves `name` via `dlsym(RTLD_DEFAULT, ...)`, or prints one line to stderr and returns
/// `None` for a missing symbol.
///
/// # Safety
/// `F` must be a pointer-sized `extern "C" fn` type matching the real signature `name` names.
unsafe fn resolve<F: Copy>(name: &CStr) -> Option<F> {
    // SAFETY: `RTLD_DEFAULT` searches every symbol already loaded into the process, which
    // includes libnode's `napi_*` exports once this `cdylib` is `dlopen`'d by Node.
    // `transmute_copy`, not `transmute`, sidesteps the generic size check that would
    // otherwise reject an unconstrained `F`; every concrete `F` this module instantiates is
    // pointer-sized like `*mut c_void`, so the copy is sound.
    unsafe {
        let raw = dlsym(RTLD_DEFAULT, name.as_ptr());
        if raw.is_null() {
            eprintln!(
                "pm-ws napi: missing required symbol {}",
                name.to_string_lossy()
            );
            return None;
        }
        Some(std::mem::transmute_copy::<*mut c_void, F>(&raw))
    }
}

unsafe fn resolve_table() -> Option<NapiFns> {
    // SAFETY: every symbol named below is a stable, always-present Node-API v8+ export;
    // `resolve`'s own contract is discharged by this module's fixed `Fn*` type aliases.
    unsafe {
        Some(NapiFns {
            create_function: resolve(c"napi_create_function")?,
            set_named_property: resolve(c"napi_set_named_property")?,
            get_cb_info: resolve(c"napi_get_cb_info")?,
            get_value_string_utf8: resolve(c"napi_get_value_string_utf8")?,
            get_value_uint32: resolve(c"napi_get_value_uint32")?,
            get_value_double: resolve(c"napi_get_value_double")?,
            get_value_bigint_uint64: resolve(c"napi_get_value_bigint_uint64")?,
            create_object: resolve(c"napi_create_object")?,
            create_array_with_length: resolve(c"napi_create_array_with_length")?,
            set_element: resolve(c"napi_set_element")?,
            create_string_utf8: resolve(c"napi_create_string_utf8")?,
            create_uint32: resolve(c"napi_create_uint32")?,
            create_bigint_words: resolve(c"napi_create_bigint_words")?,
            create_bigint_uint64: resolve(c"napi_create_bigint_uint64")?,
            get_undefined: resolve(c"napi_get_undefined")?,
            get_null: resolve(c"napi_get_null")?,
            throw_error: resolve(c"napi_throw_error")?,
            create_external: resolve(c"napi_create_external")?,
            get_value_external: resolve(c"napi_get_value_external")?,
        })
    }
}

/// The native side of one `open()`'d session; `ptr` is null exactly when the session is
/// closed. `close()` nulls this cell before calling `pmws_close` itself, so the finalizer —
/// racing nothing, since both run on the JS thread — never double-closes.
///
/// `pending_events`, keyed by market index, holds an event `pmws_next_event` has already
/// advanced the native retained-mutation cursor past but whose JS object
/// [`next_event_callback`] has not yet finished building. The slot is filled the instant the
/// native call reports [`ffi::PMWS_STATUS_OK`] and cleared only once [`event_object`]
/// finishes without failing, so a mid-build napi failure throws `PMWS_INTERNAL` with the
/// mutation still held here — the next call retries the same event instead of losing it.
///
/// A stashed event stays valid exactly as long as the native event-stream cursor it was
/// read from has not been replaced. `pmws_attach` and `pmws_reattach` are the only calls
/// that replace it, and only when they settle on [`ffi::PMWS_STATUS_OK`] — a buffer-retry
/// that ultimately fails never gets there, and `pmws_read_state` never touches the cursor
/// at all. [`snapshot_callback`] is the single place that clears a market's slot, and only
/// on that exact condition: clearing any earlier, or on any other outcome, would either
/// erase a still-valid stash or keep one the native side has already moved past.
struct SessionBox {
    ptr: Cell<*mut PmwsSession>,
    pending_events: RefCell<HashMap<u32, PmwsEvent>>,
}

/// The `napi_external` finalizer `open` registers: runs at most once per external and
/// reclaims [`SessionBox`] itself, skipping `pmws_close` when `close()` already ran it.
unsafe extern "C" fn finalize_session(_env: NapiEnv, data: *mut c_void, _hint: *mut c_void) {
    if data.is_null() {
        return;
    }
    // SAFETY: `data` is the `SessionBox` pointer `open` leaked into `napi_create_external`;
    // Node calls a finalizer at most once per external.
    unsafe {
        let boxed = Box::from_raw(data as *mut SessionBox);
        let ptr = boxed.ptr.get();
        if !ptr.is_null() {
            ffi::pmws_close(ptr);
        }
    }
}

const MAX_ARGS: usize = 4;

/// Reads up to [`MAX_ARGS`] arguments from `info`; slots beyond the real count stay null.
unsafe fn read_args(
    napi: &NapiFns,
    env: NapiEnv,
    info: NapiCallbackInfo,
) -> ([NapiValue; MAX_ARGS], usize) {
    let mut argv: [NapiValue; MAX_ARGS] = [std::ptr::null_mut(); MAX_ARGS];
    let mut argc = MAX_ARGS;
    // SAFETY: `env`/`info` are this callback's live handles; `argv` is a valid
    // `MAX_ARGS`-length buffer for Node-API to fill.
    unsafe {
        (napi.get_cb_info)(
            env,
            info,
            &mut argc,
            argv.as_mut_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    (argv, argc.min(MAX_ARGS))
}

unsafe fn string_arg(napi: &NapiFns, env: NapiEnv, value: NapiValue) -> Option<String> {
    let mut len = 0_usize;
    let mut buf;
    let mut written = 0_usize;
    // SAFETY: `value` is a live `napi_value`; a null `buf` with `bufsize == 0` asks only for
    // the required length, and the second call's `buf` is sized `len + 1`, one more than
    // Node-API needs for its trailing NUL.
    unsafe {
        if (napi.get_value_string_utf8)(env, value, std::ptr::null_mut(), 0, &mut len) != 0 {
            return None;
        }
        buf = vec![0_u8; len + 1];
        if (napi.get_value_string_utf8)(
            env,
            value,
            buf.as_mut_ptr() as *mut c_char,
            buf.len(),
            &mut written,
        ) != 0
        {
            return None;
        }
    }
    buf.truncate(written);
    String::from_utf8(buf).ok()
}

unsafe fn uint32_arg(napi: &NapiFns, env: NapiEnv, value: NapiValue) -> Option<u32> {
    let mut out = 0_u32;
    // SAFETY: `value` is a live `napi_value`.
    (unsafe { (napi.get_value_uint32)(env, value, &mut out) } == 0).then_some(out)
}

/// Reads `value` as a JS number that is exactly an integer inside `range`, or `None`.
///
/// The narrowing readers Node-API offers do not refuse a value they cannot represent:
/// `napi_get_value_int32` and `napi_get_value_uint32` apply ECMAScript's own `ToInt32` /
/// `ToUint32`, which wrap modulo 2³², so `-1` reaches an unsigned parameter as `u32::MAX` and
/// `2³² + 5` reaches it as `5`. Every numeric argument of this shim is therefore read as the
/// `double` JS actually holds and range-checked *before* it is narrowed, so an out-of-range
/// argument is a typed refusal rather than a different, plausible-looking call. A `NaN`, an
/// infinity and a fraction all fail the same way, since none of them names an integer.
unsafe fn ranged_integer_arg(
    napi: &NapiFns,
    env: NapiEnv,
    value: NapiValue,
    range: core::ops::RangeInclusive<f64>,
) -> Option<f64> {
    let mut out = 0_f64;
    // SAFETY: `value` is a live `napi_value`.
    if unsafe { (napi.get_value_double)(env, value, &mut out) } != 0 {
        return None;
    }
    (out.fract() == 0.0 && range.contains(&out)).then_some(out)
}

/// Reads `value` as a JS `BigInt` that fits 64 bits exactly — `pmws_wait`'s `lastGeneration`.
///
/// The `lossless` flag Node-API reports is honoured, not ignored: a `BigInt` outside
/// `[0, 2⁶⁴)` is truncated by `napi_get_value_bigint_uint64` rather than refused, so `2⁶⁴`
/// arrives as `0` — the generation a fresh segment carries. A caller that passed a bad
/// generation would then be told "unchanged" against the very first publication and park
/// through everything that followed. Silent truncation is refused instead: `None` here
/// becomes `PMWS_INVALID_ARGUMENT` at the callback.
unsafe fn bigint_u64_arg(napi: &NapiFns, env: NapiEnv, value: NapiValue) -> Option<u64> {
    let mut out = 0_u64;
    let mut lossless = false;
    // SAFETY: `value` is a live `napi_value`.
    if unsafe { (napi.get_value_bigint_uint64)(env, value, &mut out, &mut lossless) } != 0 {
        return None;
    }
    lossless.then_some(out)
}

unsafe fn undefined(napi: &NapiFns, env: NapiEnv) -> NapiValue {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live.
    unsafe { (napi.get_undefined)(env, &mut out) };
    out
}

unsafe fn null(napi: &NapiFns, env: NapiEnv) -> NapiValue {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live.
    unsafe { (napi.get_null)(env, &mut out) };
    out
}

unsafe fn create_object(napi: &NapiFns, env: NapiEnv) -> Option<NapiValue> {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live.
    (unsafe { (napi.create_object)(env, &mut out) } == 0).then_some(out)
}

unsafe fn create_array(napi: &NapiFns, env: NapiEnv, len: usize) -> Option<NapiValue> {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live.
    (unsafe { (napi.create_array_with_length)(env, len, &mut out) } == 0).then_some(out)
}

unsafe fn create_string(napi: &NapiFns, env: NapiEnv, text: &str) -> Option<NapiValue> {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live; `text` is a valid UTF-8 `&str` borrowed for exactly this call.
    (unsafe {
        (napi.create_string_utf8)(env, text.as_ptr() as *const c_char, text.len(), &mut out)
    } == 0)
        .then_some(out)
}

unsafe fn create_uint32_value(napi: &NapiFns, env: NapiEnv, value: u32) -> Option<NapiValue> {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live.
    (unsafe { (napi.create_uint32)(env, value, &mut out) } == 0).then_some(out)
}

unsafe fn create_bigint_u64(napi: &NapiFns, env: NapiEnv, value: u64) -> Option<NapiValue> {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live.
    (unsafe { (napi.create_bigint_uint64)(env, value, &mut out) } == 0).then_some(out)
}

/// `words` is least-significant-word first, the order `napi_create_bigint_words` documents.
unsafe fn create_bigint_words_value(
    napi: &NapiFns,
    env: NapiEnv,
    negative: bool,
    words: &[u64; 2],
) -> Option<NapiValue> {
    let mut out: NapiValue = std::ptr::null_mut();
    // SAFETY: `env` is live; `words` is a 2-element array borrowed for exactly this call.
    (unsafe { (napi.create_bigint_words)(env, i32::from(negative), 2, words.as_ptr(), &mut out) }
        == 0)
        .then_some(out)
}

unsafe fn set_prop(
    napi: &NapiFns,
    env: NapiEnv,
    object: NapiValue,
    name: &'static CStr,
    value: NapiValue,
) -> bool {
    // SAFETY: `env`/`object` are live; `name` is a `'static`, NUL-terminated property name.
    unsafe { (napi.set_named_property)(env, object, name.as_ptr(), value) == 0 }
}

unsafe fn put_value(
    napi: &NapiFns,
    env: NapiEnv,
    object: NapiValue,
    name: &'static CStr,
    value: NapiValue,
) -> Option<()> {
    // SAFETY: as `set_prop`.
    unsafe { set_prop(napi, env, object, name, value) }.then_some(())
}

unsafe fn put_str(
    napi: &NapiFns,
    env: NapiEnv,
    object: NapiValue,
    name: &'static CStr,
    text: &str,
) -> Option<()> {
    // SAFETY: as `create_string`/`put_value`.
    unsafe {
        let value = create_string(napi, env, text)?;
        put_value(napi, env, object, name, value)
    }
}

unsafe fn put_u32(
    napi: &NapiFns,
    env: NapiEnv,
    object: NapiValue,
    name: &'static CStr,
    value: u32,
) -> Option<()> {
    // SAFETY: as `create_uint32_value`/`put_value`.
    unsafe {
        let value = create_uint32_value(napi, env, value)?;
        put_value(napi, env, object, name, value)
    }
}

unsafe fn put_bigint(
    napi: &NapiFns,
    env: NapiEnv,
    object: NapiValue,
    name: &'static CStr,
    value: u64,
) -> Option<()> {
    // SAFETY: as `create_bigint_u64`/`put_value`.
    unsafe {
        let value = create_bigint_u64(napi, env, value)?;
        put_value(napi, env, object, name, value)
    }
}

fn status_code_name(status: i32) -> &'static CStr {
    match status {
        ffi::PMWS_STATUS_OK => c"PMWS_OK",
        ffi::PMWS_STATUS_NONE => c"PMWS_NONE",
        ffi::PMWS_STATUS_INVALID_ARGUMENT => c"PMWS_INVALID_ARGUMENT",
        ffi::PMWS_STATUS_IO => c"PMWS_IO",
        ffi::PMWS_STATUS_SEGMENT_INCOMPATIBLE => c"PMWS_SEGMENT_INCOMPATIBLE",
        ffi::PMWS_STATUS_MARKET_NOT_FOUND => c"PMWS_MARKET_NOT_FOUND",
        ffi::PMWS_STATUS_NO_PUBLISHED_STATE => c"PMWS_NO_PUBLISHED_STATE",
        ffi::PMWS_STATUS_CONTENDED => c"PMWS_CONTENDED",
        ffi::PMWS_STATUS_WRITER_STALLED => c"PMWS_WRITER_STALLED",
        ffi::PMWS_STATUS_MALFORMED_RECORD => c"PMWS_MALFORMED_RECORD",
        ffi::PMWS_STATUS_BUFFER_TOO_SMALL => c"PMWS_BUFFER_TOO_SMALL",
        ffi::PMWS_STATUS_CONTINUITY_LOST => c"PMWS_CONTINUITY_LOST",
        ffi::PMWS_STATUS_NOT_ATTACHED => c"PMWS_NOT_ATTACHED",
        ffi::PMWS_STATUS_INTERNAL => c"PMWS_INTERNAL",
        ffi::PMWS_STATUS_ATTACH_REFUSED => c"PMWS_ATTACH_REFUSED",
        ffi::PMWS_STATUS_ATTACH_INCOMPLETE => c"PMWS_ATTACH_INCOMPLETE",
        ffi::PMWS_STATUS_DOORBELL_UNAVAILABLE => c"PMWS_DOORBELL_UNAVAILABLE",
        ffi::PMWS_STATUS_FOREIGN_SEGMENT => c"PMWS_FOREIGN_SEGMENT",
        _ => c"PMWS_UNKNOWN",
    }
}

/// Throws a JS `Error` whose message is `pmws_status_text(status)` and whose `code` property
/// is `status`'s `PMWS_*` name; returns the null `napi_value` a throwing callback must return.
unsafe fn throw_pmws_error(napi: &NapiFns, env: NapiEnv, status: i32) -> NapiValue {
    let code = status_code_name(status);
    // SAFETY: `env` is live; `pmws_status_text` always returns a valid, NUL-terminated,
    // `'static` C string, so wrapping it in `CStr::from_ptr` and handing both strings to
    // `throw_error` is sound.
    unsafe {
        let message = CStr::from_ptr(ffi::pmws_status_text(status));
        (napi.throw_error)(env, code.as_ptr(), message.as_ptr());
    }
    std::ptr::null_mut()
}

unsafe fn throw_closed(napi: &NapiFns, env: NapiEnv) -> NapiValue {
    // SAFETY: `env` is live; both strings are `'static` and NUL-terminated.
    unsafe {
        (napi.throw_error)(
            env,
            c"PMWS_SESSION_CLOSED".as_ptr(),
            c"session is closed".as_ptr(),
        )
    };
    std::ptr::null_mut()
}

/// Throws the sticky loss error `nextEvent` raises on [`ffi::PMWS_STATUS_CONTINUITY_LOST`]:
/// `code` is the stable string `'PMWS_CONTINUITY_LOST'`; the message names `reason_word`, the
/// same `BREAK_*` word `out->continuity_reason` carries.
unsafe fn throw_continuity_lost(napi: &NapiFns, env: NapiEnv, reason_word: u32) -> NapiValue {
    let message = CString::new(format!("continuity lost, reason word {reason_word}"))
        .unwrap_or_else(|_| CString::new("continuity lost").expect("no NUL bytes"));
    // SAFETY: `env` is live; both strings are NUL-terminated for the call's duration.
    unsafe { (napi.throw_error)(env, c"PMWS_CONTINUITY_LOST".as_ptr(), message.as_ptr()) };
    std::ptr::null_mut()
}

/// Throws the declared full-rescan signal `nextDirty` raises on
/// [`ffi::PMWS_STATUS_CONTINUITY_LOST`]: a distinct code from
/// [`throw_continuity_lost`]'s `PMWS_CONTINUITY_LOST`, because unlike an event stream's
/// sticky loss this is never sticky — the session's dirty cursor has already rebased past
/// the entry that overran it by the time this throws.
unsafe fn throw_dirty_rescan(napi: &NapiFns, env: NapiEnv) -> NapiValue {
    // SAFETY: `env` is live; both strings are `'static` and NUL-terminated.
    unsafe {
        (napi.throw_error)(
            env,
            c"PMWS_DIRTY_RESCAN".as_ptr(),
            c"dirty-index ring lapped this cursor; re-read every attached market's state and \
              events once, then resume nextDirty()"
                .as_ptr(),
        )
    };
    std::ptr::null_mut()
}

/// Resolves `value` to the `SessionBox` pointer Node's external wraps, or throws
/// `PMWS_INVALID_ARGUMENT` and returns the thrown value as `Err` for an unresolvable
/// external. Never null on `Ok`; the pointee's own `ptr` may still be null (closed).
unsafe fn session_box_ptr(
    napi: &NapiFns,
    env: NapiEnv,
    value: NapiValue,
) -> Result<*const SessionBox, NapiValue> {
    // SAFETY: `env`/`value` are live; `data`, once non-null, is a `SessionBox` pointer this
    // module's own `open` stored as this external's data — no other caller can construct a
    // `napi_external` carrying one.
    unsafe {
        let mut data: *mut c_void = std::ptr::null_mut();
        if (napi.get_value_external)(env, value, &mut data) != 0 || data.is_null() {
            return Err(throw_pmws_error(
                napi,
                env,
                ffi::PMWS_STATUS_INVALID_ARGUMENT,
            ));
        }
        Ok(data as *const SessionBox)
    }
}

/// Resolves `value` to a live session pointer, or throws and returns the thrown value as
/// `Err` — as [`session_box_ptr`], plus a resolvable but already `close()`d external throws
/// `PMWS_SESSION_CLOSED`.
unsafe fn session_from_external(
    napi: &NapiFns,
    env: NapiEnv,
    value: NapiValue,
) -> Result<*mut PmwsSession, NapiValue> {
    // SAFETY: `boxed` came from `session_box_ptr`, which resolves it from the same live
    // external `session_box_ptr` itself documents.
    unsafe {
        let boxed = session_box_ptr(napi, env, value)?;
        let ptr = (&*boxed).ptr.get();
        if ptr.is_null() {
            return Err(throw_closed(napi, env));
        }
        Ok(ptr)
    }
}

/// `1` maps to `"bid"`, `2` to `"ask"` — the same word [`crate::ffi`]'s own `side_word` uses.
fn side_str(side: u32) -> &'static str {
    match side {
        1 => "bid",
        2 => "ask",
        _ => "unknown",
    }
}

/// Maps an `(origin, derivation)` word pair — see `src/ffi/mod.rs`'s `origin_words` — to the
/// three origins this ABI generation can carry: `sourceReported`, `normalizedFromSource`, and
/// `snapshotDiff` (`origin == 3` is always paired with `derivation == 1`, the only derivation
/// this ABI defines).
fn origin_str(origin: u32, derivation: u32) -> &'static str {
    match (origin, derivation) {
        (1, _) => "sourceReported",
        (2, _) => "normalizedFromSource",
        (3, 1) => "snapshotDiff",
        _ => "unknown",
    }
}

/// Splits `decimal`'s 128-bit two's-complement coefficient into the sign bit and
/// little-endian-first magnitude words [`napi_create_bigint_words`] wants. Every price and
/// quantity this ABI ever carries has a non-negative coefficient (`coefficient_high`'s top
/// bit clear — see `PmwsDecimal`'s own doc comment), so `negative` is always `false` in
/// practice; the two's-complement negation below is exercised only if a future ABI generation
/// ever sets that bit.
fn decimal_bigint_words(decimal: &PmwsDecimal) -> (bool, [u64; 2]) {
    let negative = decimal.coefficient_high >> 63 == 1;
    if !negative {
        return (false, [decimal.coefficient_low, decimal.coefficient_high]);
    }
    let magnitude = ((u128::from(decimal.coefficient_high) << 64)
        | u128::from(decimal.coefficient_low))
    .wrapping_neg();
    (true, [magnitude as u64, (magnitude >> 64) as u64])
}

/// Renders `decimal` through [`ffi::pmws_decimal_text`], retrying once into a heap buffer
/// sized by the call's own reported length on [`ffi::PMWS_STATUS_BUFFER_TOO_SMALL`].
fn decimal_text(decimal: &PmwsDecimal) -> Option<String> {
    let mut stack = [0_u8; 48];
    let mut len = 0_usize;
    // SAFETY: `decimal` is a valid, initialized `PmwsDecimal`; `stack`/`heap` are exactly
    // `cap` writable bytes for the call's duration, and the retry buffer is sized exactly to
    // the length the first call reported on `PMWS_STATUS_BUFFER_TOO_SMALL`.
    unsafe {
        let status = ffi::pmws_decimal_text(decimal, stack.as_mut_ptr(), stack.len(), &mut len);
        match status {
            ffi::PMWS_STATUS_OK => String::from_utf8(stack[..len].to_vec()).ok(),
            ffi::PMWS_STATUS_BUFFER_TOO_SMALL => {
                let mut heap = vec![0_u8; len];
                let status =
                    ffi::pmws_decimal_text(decimal, heap.as_mut_ptr(), heap.len(), &mut len);
                (status == ffi::PMWS_STATUS_OK)
                    .then(|| String::from_utf8(heap).ok())
                    .flatten()
            }
            _ => None,
        }
    }
}

fn zero_decimal() -> PmwsDecimal {
    PmwsDecimal {
        coefficient_low: 0,
        coefficient_high: 0,
        scale: 0,
        reserved: 0,
    }
}

fn zero_level() -> PmwsLevel {
    PmwsLevel {
        side: 0,
        reserved: 0,
        price: zero_decimal(),
        quantity: zero_decimal(),
    }
}

/// `{ coefficient: BigInt, scale: number, text: string }`.
unsafe fn decimal_object(napi: &NapiFns, env: NapiEnv, decimal: &PmwsDecimal) -> Option<NapiValue> {
    // SAFETY: `env` is live for the whole call; every callee shares that same contract.
    unsafe {
        let obj = create_object(napi, env)?;
        let (negative, words) = decimal_bigint_words(decimal);
        let coefficient = create_bigint_words_value(napi, env, negative, &words)?;
        put_value(napi, env, obj, c"coefficient", coefficient)?;
        put_u32(napi, env, obj, c"scale", decimal.scale)?;
        put_str(
            napi,
            env,
            obj,
            c"text",
            &decimal_text(decimal).unwrap_or_default(),
        )?;
        Some(obj)
    }
}

/// `[{ side: 'bid'|'ask', price, quantity }, ...]`.
unsafe fn levels_array(napi: &NapiFns, env: NapiEnv, levels: &[PmwsLevel]) -> Option<NapiValue> {
    // SAFETY: `env` is live for the whole call; `array` was just created by this same call,
    // and `index < levels.len()`, the array's own length.
    unsafe {
        let array = create_array(napi, env, levels.len())?;
        for (index, level) in levels.iter().enumerate() {
            let entry = create_object(napi, env)?;
            let side = create_string(napi, env, side_str(level.side))?;
            put_value(napi, env, entry, c"side", side)?;
            let price = decimal_object(napi, env, &level.price)?;
            put_value(napi, env, entry, c"price", price)?;
            let quantity = decimal_object(napi, env, &level.quantity)?;
            put_value(napi, env, entry, c"quantity", quantity)?;
            if (napi.set_element)(env, array, index as u32, entry) != 0 {
                return None;
            }
        }
        Some(array)
    }
}

/// Builds the JS state object `attach`/`readState`/`reattach` return: [`PmwsState`]'s fields
/// field-for-field in camelCase, except `commit_time_present`/`publication_present` fold into
/// `undefined` on their respective fields rather than surfacing as raw booleans, and
/// `level_count`/`level_capacity_required` are dropped — `levels.length` already carries the
/// former, and the latter is this shim's own retry-sizing internal, never surfaced to JS.
unsafe fn state_object(
    napi: &NapiFns,
    env: NapiEnv,
    state: &PmwsState,
    levels: &[PmwsLevel],
) -> Option<NapiValue> {
    // SAFETY: `env` is live for the whole call; every callee shares that same contract.
    unsafe {
        let obj = create_object(napi, env)?;
        put_bigint(napi, env, obj, c"revision", state.revision)?;
        put_bigint(napi, env, obj, c"cursorEpoch", state.cursor_epoch)?;
        put_bigint(napi, env, obj, c"cursorPosition", state.cursor_position)?;
        put_bigint(napi, env, obj, c"syncDivergences", state.sync_divergences)?;
        let commit_time = if state.commit_time_present != 0 {
            create_bigint_u64(napi, env, state.commit_time)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"commitTime", commit_time)?;
        let arrival_time = if state.arrival_time != 0 {
            create_bigint_u64(napi, env, state.arrival_time)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"arrivalTime", arrival_time)?;
        put_u32(napi, env, obj, c"authorityState", state.authority_state)?;
        put_u32(napi, env, obj, c"authorityReason", state.authority_reason)?;
        put_u32(napi, env, obj, c"continuityKind", state.continuity_kind)?;
        put_u32(napi, env, obj, c"continuityReason", state.continuity_reason)?;
        let (origin, representation, native_family) = if state.publication_present != 0 {
            let origin = create_string(napi, env, origin_str(state.origin, state.derivation))?;
            let representation = create_uint32_value(napi, env, state.representation)?;
            let bytes = &state.native_family[..state.native_family_len as usize];
            let native_family = create_string(napi, env, std::str::from_utf8(bytes).unwrap_or(""))?;
            (origin, representation, native_family)
        } else {
            let value = undefined(napi, env);
            (value, value, value)
        };
        put_value(napi, env, obj, c"origin", origin)?;
        put_value(napi, env, obj, c"representation", representation)?;
        put_value(napi, env, obj, c"nativeFamily", native_family)?;
        let levels = levels_array(napi, env, levels)?;
        put_value(napi, env, obj, c"levels", levels)?;
        Some(obj)
    }
}

/// Builds the JS event object `nextEvent` returns for a delivered kind: `"mutation"`
/// (`delivery_kind == 1`, [`mutation_object`]) or `"resolution"` (`delivery_kind == 3`,
/// [`resolution_object`]); a continuity loss never reaches here — see
/// [`throw_continuity_lost`].
///
/// Any other delivered kind yields `None`, which the caller reports as
/// [`ffi::PMWS_STATUS_INTERNAL`] and which leaves the event stashed.
unsafe fn event_object(napi: &NapiFns, env: NapiEnv, event: &PmwsEvent) -> Option<NapiValue> {
    match event.delivery_kind {
        ffi::DELIVERY_MUTATION => unsafe { mutation_object(napi, env, event) },
        ffi::DELIVERY_RESOLUTION => unsafe { resolution_object(napi, env, event) },
        _ => None,
    }
}

/// Builds the JS event object [`event_object`] returns for `delivery_kind == 1`.
unsafe fn mutation_object(napi: &NapiFns, env: NapiEnv, event: &PmwsEvent) -> Option<NapiValue> {
    // SAFETY: `env` is live for the whole call; every callee shares that same contract.
    unsafe {
        let obj = create_object(napi, env)?;
        let kind = create_string(napi, env, "mutation")?;
        put_value(napi, env, obj, c"kind", kind)?;
        put_bigint(napi, env, obj, c"revision", event.book_revision)?;
        let cursor = create_object(napi, env)?;
        put_bigint(napi, env, cursor, c"epoch", event.cursor_epoch)?;
        put_bigint(napi, env, cursor, c"position", event.cursor_position)?;
        put_value(napi, env, obj, c"cursor", cursor)?;
        let commit_time = if event.commit_time != 0 {
            create_bigint_u64(napi, env, event.commit_time)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"commitTime", commit_time)?;
        let arrival_time = if event.arrival_time != 0 {
            create_bigint_u64(napi, env, event.arrival_time)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"arrivalTime", arrival_time)?;
        let origin = create_string(napi, env, origin_str(event.origin, event.derivation))?;
        put_value(napi, env, obj, c"origin", origin)?;
        put_u32(napi, env, obj, c"representation", event.representation)?;
        let family = &event.native_family[..event.native_family_len as usize];
        put_str(
            napi,
            env,
            obj,
            c"nativeFamily",
            std::str::from_utf8(family).unwrap_or(""),
        )?;
        let side = create_string(napi, env, side_str(event.side))?;
        put_value(napi, env, obj, c"side", side)?;
        let price = decimal_object(napi, env, &event.price)?;
        put_value(napi, env, obj, c"price", price)?;
        let old_quantity = if event.old_present != 0 {
            decimal_object(napi, env, &event.old_quantity)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"oldQuantity", old_quantity)?;
        let new_quantity = if event.new_present != 0 {
            decimal_object(napi, env, &event.new_quantity)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"newQuantity", new_quantity)?;
        put_bigint(napi, env, obj, c"daemonGeneration", event.daemon_generation)?;
        put_bigint(
            napi,
            env,
            obj,
            c"subscriptionGeneration",
            event.subscription_generation,
        )?;
        Some(obj)
    }
}

/// Maps `event.delivery_path` — see [`crate::shm::codec::delivery_path_word`] — to the three
/// paths this ABI generation can carry, or `"unknown"` for a discriminant this binding does
/// not recognize.
fn delivery_path_str(delivery_path: u32) -> &'static str {
    match delivery_path {
        1 => "marketFeed",
        2 => "lifecycleFeed",
        3 => "resolutionFeed",
        _ => "unknown",
    }
}

/// Builds the JS event object [`event_object`] returns for `delivery_kind == 3`: the venue's
/// own winning-outcome text, market-type label, and resolution-date lexeme, verbatim — never
/// parsed into a number, a boolean, or a date. No mutation field is meaningful here, so none
/// is surfaced.
unsafe fn resolution_object(napi: &NapiFns, env: NapiEnv, event: &PmwsEvent) -> Option<NapiValue> {
    // SAFETY: `env` is live for the whole call; every callee shares that same contract.
    unsafe {
        let obj = create_object(napi, env)?;
        let kind = create_string(napi, env, "resolution")?;
        put_value(napi, env, obj, c"kind", kind)?;
        put_bigint(napi, env, obj, c"revision", event.book_revision)?;
        let cursor = create_object(napi, env)?;
        put_bigint(napi, env, cursor, c"epoch", event.cursor_epoch)?;
        put_bigint(napi, env, cursor, c"position", event.cursor_position)?;
        put_value(napi, env, obj, c"cursor", cursor)?;
        let commit_time = if event.commit_time != 0 {
            create_bigint_u64(napi, env, event.commit_time)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"commitTime", commit_time)?;
        let arrival_time = if event.arrival_time != 0 {
            create_bigint_u64(napi, env, event.arrival_time)?
        } else {
            undefined(napi, env)
        };
        put_value(napi, env, obj, c"arrivalTime", arrival_time)?;
        let origin = create_string(napi, env, origin_str(event.origin, event.derivation))?;
        put_value(napi, env, obj, c"origin", origin)?;
        put_u32(napi, env, obj, c"representation", event.representation)?;
        put_u32(napi, env, obj, c"winningIndex", event.winning_index)?;
        let outcome = &event.winning_outcome[..event.winning_outcome_len as usize];
        put_str(
            napi,
            env,
            obj,
            c"winningOutcome",
            std::str::from_utf8(outcome).unwrap_or(""),
        )?;
        let market_type = &event.market_type[..event.market_type_len as usize];
        put_str(
            napi,
            env,
            obj,
            c"marketType",
            std::str::from_utf8(market_type).unwrap_or(""),
        )?;
        let resolution_date = &event.resolution_date[..event.resolution_date_len as usize];
        put_str(
            napi,
            env,
            obj,
            c"resolutionDate",
            std::str::from_utf8(resolution_date).unwrap_or(""),
        )?;
        let delivery_path = create_string(napi, env, delivery_path_str(event.delivery_path))?;
        put_value(napi, env, obj, c"deliveryPath", delivery_path)?;
        put_bigint(napi, env, obj, c"daemonGeneration", event.daemon_generation)?;
        put_bigint(
            napi,
            env,
            obj,
            c"subscriptionGeneration",
            event.subscription_generation,
        )?;
        Some(obj)
    }
}

unsafe fn segment_info_object(
    napi: &NapiFns,
    env: NapiEnv,
    info: &PmwsSegmentInfo,
) -> Option<NapiValue> {
    // SAFETY: `env` is live for the whole call; every callee shares that same contract.
    unsafe {
        let obj = create_object(napi, env)?;
        let instance_id = create_bigint_words_value(
            napi,
            env,
            false,
            &[info.instance_id_low, info.instance_id_high],
        )?;
        put_value(napi, env, obj, c"instanceId", instance_id)?;
        put_bigint(
            napi,
            env,
            obj,
            c"segmentGeneration",
            info.segment_generation,
        )?;
        put_bigint(
            napi,
            env,
            obj,
            c"publicationGeneration",
            info.publication_generation,
        )?;
        put_u32(
            napi,
            env,
            obj,
            c"directoryCapacity",
            info.directory_capacity,
        )?;
        put_u32(
            napi,
            env,
            obj,
            c"stateSlotCapacity",
            info.state_slot_capacity,
        )?;
        put_u32(napi, env, obj, c"levelCapacity", info.level_capacity)?;
        put_u32(napi, env, obj, c"eventCapacity", info.event_capacity)?;
        Some(obj)
    }
}

enum SnapshotKind {
    Attach,
    Read,
    Reattach,
}

/// Drives `pmws_attach`/`pmws_read_state`/`pmws_reattach` per `kind`, sizing the level buffer
/// from the segment's own `level_capacity` and retrying once, larger, on
/// [`ffi::PMWS_STATUS_BUFFER_TOO_SMALL`] — the buffer dance the JS surface never sees.
unsafe fn fetch_snapshot(
    session: *mut PmwsSession,
    market: u32,
    kind: &SnapshotKind,
) -> Result<(PmwsState, Vec<PmwsLevel>), i32> {
    // SAFETY: `session` is a live `pmws_open` pointer for the whole call; `out`/`levels` are
    // always sized and owned by this function, and `out` is read back only on the status
    // each entry point documents as having written it.
    unsafe {
        let mut geometry = MaybeUninit::<PmwsSegmentInfo>::uninit();
        let status = ffi::pmws_segment_info(session, geometry.as_mut_ptr());
        if status != ffi::PMWS_STATUS_OK {
            return Err(status);
        }
        let mut capacity = geometry.assume_init().level_capacity;
        loop {
            let mut levels = vec![zero_level(); capacity as usize];
            let mut out = MaybeUninit::<PmwsState>::uninit();
            let status = match kind {
                SnapshotKind::Attach => ffi::pmws_attach(
                    session,
                    market,
                    out.as_mut_ptr(),
                    levels.as_mut_ptr(),
                    capacity,
                ),
                SnapshotKind::Read => ffi::pmws_read_state(
                    session,
                    market,
                    out.as_mut_ptr(),
                    levels.as_mut_ptr(),
                    capacity,
                ),
                SnapshotKind::Reattach => ffi::pmws_reattach(
                    session,
                    market,
                    out.as_mut_ptr(),
                    levels.as_mut_ptr(),
                    capacity,
                ),
            };
            match status {
                ffi::PMWS_STATUS_OK => {
                    let state = out.assume_init();
                    levels.truncate(state.level_count as usize);
                    return Ok((state, levels));
                }
                ffi::PMWS_STATUS_BUFFER_TOO_SMALL => {
                    let state = out.assume_init();
                    if state.level_capacity_required <= capacity {
                        return Err(ffi::PMWS_STATUS_INTERNAL);
                    }
                    capacity = state.level_capacity_required;
                }
                other => return Err(other),
            }
        }
    }
}

unsafe extern "C" fn open_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles; every pointer formed below is
    // either null-checked, a live local, or freshly allocated by this same function.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 1 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let Some(path) = string_arg(napi, env, argv[0]) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let bytes = path.as_bytes();
        let mut session: *mut PmwsSession = std::ptr::null_mut();
        let status = ffi::pmws_open(bytes.as_ptr(), bytes.len(), &mut session);
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        let data = Box::into_raw(Box::new(SessionBox {
            ptr: Cell::new(session),
            pending_events: RefCell::new(HashMap::new()),
        })) as *mut c_void;
        let mut external: NapiValue = std::ptr::null_mut();
        let status = (napi.create_external)(
            env,
            data,
            finalize_session,
            std::ptr::null_mut(),
            &mut external,
        );
        if status != 0 {
            finalize_session(env, data, std::ptr::null_mut());
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL);
        }
        external
    }
}

/// `connect(controlSocket, market)`: the descriptor-transfer attachment, wrapped exactly as
/// [`open_callback`] wraps the path-based one.
///
/// The session it yields is the same `napi_external` over the same [`SessionBox`], so every
/// other callback here works on it unchanged; only how the segment was reached differs — and,
/// on a page-placement segment, whether [`wait_callback`] can park at all, since the doorbell
/// page's descriptor never crosses the control channel (`crate::Attachment::descriptors`).
unsafe extern "C" fn connect_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles; every pointer formed below is
    // either null-checked, a live local, or freshly allocated by this same function.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 2 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let (Some(control), Some(market)) = (
            string_arg(napi, env, argv[0]),
            string_arg(napi, env, argv[1]),
        ) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let control = control.as_bytes();
        let market = market.as_bytes();
        let mut session: *mut PmwsSession = std::ptr::null_mut();
        let status = ffi::pmws_connect(
            control.as_ptr(),
            control.len(),
            market.as_ptr(),
            market.len(),
            &mut session,
        );
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        let data = Box::into_raw(Box::new(SessionBox {
            ptr: Cell::new(session),
            pending_events: RefCell::new(HashMap::new()),
        })) as *mut c_void;
        let mut external: NapiValue = std::ptr::null_mut();
        let status = (napi.create_external)(
            env,
            data,
            finalize_session,
            std::ptr::null_mut(),
            &mut external,
        );
        if status != 0 {
            finalize_session(env, data, std::ptr::null_mut());
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL);
        }
        external
    }
}

unsafe extern "C" fn close_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles; `data`, once non-null, is a
    // `SessionBox` pointer this module's own `open` produced.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 1 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let mut data: *mut c_void = std::ptr::null_mut();
        let status = (napi.get_value_external)(env, argv[0], &mut data);
        if status != 0 || data.is_null() {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let ptr = (&*(data as *const SessionBox))
            .ptr
            .replace(std::ptr::null_mut());
        if ptr.is_null() {
            return throw_closed(napi, env);
        }
        ffi::pmws_close(ptr);
        undefined(napi, env)
    }
}

/// `renew(session)` — renews the session's market leases at the daemon that granted them.
///
/// Returns `undefined` on success and throws the typed error otherwise, exactly as every
/// other status-returning entry point here does. A session opened from a path holds no lease
/// and throws `PMWS_STATUS_INVALID_ARGUMENT`.
unsafe extern "C" fn renew_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 1 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let status = ffi::pmws_renew(session);
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        undefined(napi, env)
    }
}

/// `lease(session, market)` — takes a further market lease on the control connection this
/// session already holds, for a market in the session's own segment.
///
/// Returns `undefined` on success and throws the typed error otherwise. A market another
/// shard holds throws `PMWS_FOREIGN_SEGMENT`, with the lease already handed back; a session
/// opened from a path has no connection to lease on and throws `PMWS_INVALID_ARGUMENT`.
unsafe extern "C" fn lease_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 2 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let Some(market) = string_arg(napi, env, argv[1]) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let market = market.as_bytes();
        let status = ffi::pmws_lease(session, market.as_ptr(), market.len());
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        undefined(napi, env)
    }
}

/// `release(session, market)` — gives up this session's lease on one market, leaving the
/// session, its other leases, and its mapping exactly as they were.
///
/// Returns `undefined` on success and throws the typed error otherwise. Releasing a market
/// this session does not hold is a success, as it is at the C ABI.
unsafe extern "C" fn release_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 2 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let Some(market) = string_arg(napi, env, argv[1]) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let market = market.as_bytes();
        let status = ffi::pmws_release(session, market.as_ptr(), market.len());
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        undefined(napi, env)
    }
}

unsafe extern "C" fn segment_info_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 1 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let mut geometry = MaybeUninit::<PmwsSegmentInfo>::uninit();
        let status = ffi::pmws_segment_info(session, geometry.as_mut_ptr());
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        match segment_info_object(napi, env, &geometry.assume_init()) {
            Some(value) => value,
            None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
        }
    }
}

unsafe extern "C" fn resolve_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 4 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let (Some(venue), Some(kind), Some(key)) = (
            string_arg(napi, env, argv[1]),
            string_arg(napi, env, argv[2]),
            string_arg(napi, env, argv[3]),
        ) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let mut market = 0_u32;
        let status = ffi::pmws_resolve(
            session,
            venue.as_bytes().as_ptr(),
            venue.len(),
            kind.as_bytes().as_ptr(),
            kind.len(),
            key.as_bytes().as_ptr(),
            key.len(),
            &mut market,
        );
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        match create_uint32_value(napi, env, market) {
            Some(value) => value,
            None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
        }
    }
}

/// Whether settling a snapshot drive on `status` replaced the market's native
/// event-stream cursor, and so must clear its stashed [`SessionBox::pending_events`] entry.
///
/// [`ffi::PMWS_STATUS_OK`] is the only status a drive can settle on that means the native
/// call actually replaced the cursor — a buffer-retry loop that gives up never gets there —
/// and only [`SnapshotKind::Attach`] and [`SnapshotKind::Reattach`] ever replace it;
/// [`SnapshotKind::Read`] never moves the cursor and so never clears regardless of status.
fn clears_pending_events(kind: &SnapshotKind, status: i32) -> bool {
    matches!(kind, SnapshotKind::Attach | SnapshotKind::Reattach) && status == ffi::PMWS_STATUS_OK
}

unsafe fn snapshot_callback(env: NapiEnv, info: NapiCallbackInfo, kind: SnapshotKind) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 2 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let boxed = match session_box_ptr(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let session_box = &*boxed;
        let session = session_box.ptr.get();
        if session.is_null() {
            return throw_closed(napi, env);
        }
        let Some(market) = uint32_arg(napi, env, argv[1]) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let outcome = fetch_snapshot(session, market, &kind);
        let status = match &outcome {
            Ok(_) => ffi::PMWS_STATUS_OK,
            Err(status) => *status,
        };
        if clears_pending_events(&kind, status) {
            session_box.pending_events.borrow_mut().remove(&market);
        }
        match outcome {
            Ok((state, levels)) => match state_object(napi, env, &state, &levels) {
                Some(value) => value,
                None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
            },
            Err(status) => throw_pmws_error(napi, env, status),
        }
    }
}

unsafe extern "C" fn attach_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    // SAFETY: forwards this callback's own live `env`/`info` unchanged.
    unsafe { snapshot_callback(env, info, SnapshotKind::Attach) }
}

unsafe extern "C" fn read_state_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    // SAFETY: as `attach_callback`.
    unsafe { snapshot_callback(env, info, SnapshotKind::Read) }
}

unsafe extern "C" fn reattach_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    // SAFETY: as `attach_callback`.
    unsafe { snapshot_callback(env, info, SnapshotKind::Reattach) }
}

/// Delivers the next retained mutation as a JS object, stashing it in the session's
/// per-market `pending_events` slot the instant the native cursor advances past it so a
/// mid-build napi failure loses nothing — see [`SessionBox`]'s own doc comment.
unsafe extern "C" fn next_event_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles; `event` is read back only on the
    // statuses `pmws_next_event` documents as having written it.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 2 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let boxed = match session_box_ptr(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let session_box = &*boxed;
        let session = session_box.ptr.get();
        if session.is_null() {
            return throw_closed(napi, env);
        }
        let Some(market) = uint32_arg(napi, env, argv[1]) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };

        let already_pending = session_box.pending_events.borrow().get(&market).copied();
        let event = match already_pending {
            Some(event) => event,
            None => {
                let mut event = MaybeUninit::<PmwsEvent>::uninit();
                let status = ffi::pmws_next_event(session, market, event.as_mut_ptr());
                match status {
                    ffi::PMWS_STATUS_OK => {
                        let event = event.assume_init();
                        session_box
                            .pending_events
                            .borrow_mut()
                            .insert(market, event);
                        event
                    }
                    ffi::PMWS_STATUS_NONE => return undefined(napi, env),
                    ffi::PMWS_STATUS_CONTINUITY_LOST => {
                        return throw_continuity_lost(
                            napi,
                            env,
                            event.assume_init().continuity_reason,
                        );
                    }
                    other => return throw_pmws_error(napi, env, other),
                }
            }
        };

        match event_object(napi, env, &event) {
            Some(value) => {
                session_box.pending_events.borrow_mut().remove(&market);
                value
            }
            None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
        }
    }
}

/// `versions()` — `{ ffi, abi }`, the two numbers a caller must agree with this artifact on
/// before it trusts anything else it returns.
///
/// Both come from [`crate::ffi`]'s own exported entry points rather than a second literal, so
/// the JS gate and the C ABI can never name different versions of one build. Both are
/// non-negative by construction — `pmws_ffi_version` returns this surface's own generation
/// and `pmws_abi_version` the segment layout's — so both cross as plain numbers. Takes no
/// arguments and needs no session: a caller checks versions before it opens one.
unsafe extern "C" fn versions_callback(env: NapiEnv, _info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    let ffi_version = u32::try_from(ffi::pmws_ffi_version()).unwrap_or(0);
    // SAFETY: `env` is this callback's live handle.
    unsafe {
        let built = (|| {
            let object = create_object(napi, env)?;
            put_u32(napi, env, object, c"ffi", ffi_version)?;
            put_u32(napi, env, object, c"abi", ffi::pmws_abi_version())?;
            Some(object)
        })();
        match built {
            Some(value) => value,
            None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
        }
    }
}

unsafe extern "C" fn publication_generation_callback(
    env: NapiEnv,
    info: NapiCallbackInfo,
) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 1 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let generation = ffi::pmws_publication_generation(session);
        match create_bigint_u64(napi, env, generation) {
            Some(value) => value,
            None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
        }
    }
}

/// `wait(lastGeneration, spinMicros, timeoutMillis)` — [`ffi::pmws_wait`]. Blocks the calling
/// (JS) thread for up to the spin-then-park duration this call describes; the dedicated,
/// non-JS-event-loop consumer pattern `docs/notes/shared-memory-model.md` and the S7a design
/// note both assume. Returns the new generation as a `BigInt` on a change, or `null` — not
/// `undefined`, unlike `nextEvent`'s no-delivery answer — when `timeoutMillis` elapses first.
///
/// A session from [`connect_callback`] over a page-placement segment cannot park: the park
/// throws the typed `PMWS_DOORBELL_UNAVAILABLE` error instead of blocking, and such a
/// consumer polls or spins.
unsafe extern "C" fn wait_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 4 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let (Some(last_generation), Some(spin_micros), Some(timeout_millis)) = (
            bigint_u64_arg(napi, env, argv[1]),
            ranged_integer_arg(
                napi,
                env,
                argv[2],
                0.0..=f64::from(ffi::PMWS_MAX_SPIN_MICROS),
            ),
            ranged_integer_arg(
                napi,
                env,
                argv[3],
                f64::from(i32::MIN)..=f64::from(i32::MAX),
            ),
        ) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let mut generation = 0_u64;
        let status = ffi::pmws_wait(
            session,
            last_generation,
            spin_micros as u32,
            timeout_millis as i32,
            &mut generation,
        );
        match status {
            ffi::PMWS_STATUS_OK => match create_bigint_u64(napi, env, generation) {
                Some(value) => value,
                None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
            },
            ffi::PMWS_STATUS_NONE => null(napi, env),
            other => throw_pmws_error(napi, env, other),
        }
    }
}

/// `nextDirty()` — [`ffi::pmws_next_dirty`]. Returns `{ directoryIndex, bookRevision }` for a
/// changed market, `null` — not `undefined` — when the writer has not reached this session's
/// dirty-ring position yet, or throws `PMWS_DIRTY_RESCAN` for the declared full-rescan
/// signal, mirroring how [`throw_continuity_lost`] surfaces `nextEvent`'s sticky loss except
/// that this one is never sticky: the session's cursor has already rebased by the time this
/// throws, and the very next call resumes ordinary polling.
///
/// `directoryIndex` is the segment's own index, never the session-local market index
/// `resolve` returns; `marketDirectoryIndex` is what maps between the two.
unsafe extern "C" fn next_dirty_callback(env: NapiEnv, info: NapiCallbackInfo) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 1 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let mut directory_index = 0_u32;
        let mut book_revision = 0_u64;
        let status = ffi::pmws_next_dirty(session, &mut directory_index, &mut book_revision);
        match status {
            ffi::PMWS_STATUS_OK => {
                let built = (|| {
                    let obj = create_object(napi, env)?;
                    put_u32(napi, env, obj, c"directoryIndex", directory_index)?;
                    put_bigint(napi, env, obj, c"bookRevision", book_revision)?;
                    Some(obj)
                })();
                match built {
                    Some(value) => value,
                    None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
                }
            }
            ffi::PMWS_STATUS_NONE => null(napi, env),
            ffi::PMWS_STATUS_CONTINUITY_LOST => throw_dirty_rescan(napi, env),
            other => throw_pmws_error(napi, env, other),
        }
    }
}

/// `marketDirectoryIndex(session, market)` — [`ffi::pmws_market_directory_index`]. Returns the
/// segment directory index of a resolved market, which is the number `nextDirty` delivers.
unsafe extern "C" fn market_directory_index_callback(
    env: NapiEnv,
    info: NapiCallbackInfo,
) -> NapiValue {
    let napi = NAPI.get().expect("napi table resolved at registration");
    // SAFETY: `env`/`info` are this callback's live handles.
    unsafe {
        let (argv, argc) = read_args(napi, env, info);
        if argc < 2 {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        }
        let session = match session_from_external(napi, env, argv[0]) {
            Ok(ptr) => ptr,
            Err(thrown) => return thrown,
        };
        let Some(market) = ranged_integer_arg(napi, env, argv[1], 0.0..=f64::from(u32::MAX)) else {
            return throw_pmws_error(napi, env, ffi::PMWS_STATUS_INVALID_ARGUMENT);
        };
        let mut directory_index = 0_u32;
        let status = ffi::pmws_market_directory_index(session, market as u32, &mut directory_index);
        if status != ffi::PMWS_STATUS_OK {
            return throw_pmws_error(napi, env, status);
        }
        match create_uint32_value(napi, env, directory_index) {
            Some(value) => value,
            None => throw_pmws_error(napi, env, ffi::PMWS_STATUS_INTERNAL),
        }
    }
}

/// The `cdylib`'s bare Node-API entry point: Node's `process.dlopen` finds this export by
/// name alone, with no version struct and no linked `napi.h` macro. Resolves every `napi_*`
/// function this module needs, then registers the JS functions the module doc
/// describes. A missing `napi_*` symbol or a failed registration call is reported to stderr
/// and leaves `exports` exactly as Node passed it in — this must never panic or abort a host
/// process that otherwise has nothing to do with this shim (a Python `ctypes.CDLL` load, for
/// instance).
///
/// # Safety
/// `env` must be a live Node-API environment handle and `exports` a live `napi_value` object,
/// exactly what Node's own module loader guarantees when it calls this export.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn napi_register_module_v1(env: NapiEnv, exports: NapiValue) -> NapiValue {
    // SAFETY: this function's own contract requires `env` be a live Node-API environment.
    let Some(table) = (unsafe { resolve_table() }) else {
        eprintln!("pm-ws napi: module registration aborted, a required napi_* symbol is missing");
        return exports;
    };
    let _ = NAPI.set(table);
    let napi = NAPI.get().expect("just set above");
    let functions: [(&'static CStr, NapiCallback); 17] = [
        (c"versions", versions_callback),
        (c"open", open_callback),
        (c"connect", connect_callback),
        (c"close", close_callback),
        (c"renew", renew_callback),
        (c"lease", lease_callback),
        (c"release", release_callback),
        (c"segmentInfo", segment_info_callback),
        (c"resolve", resolve_callback),
        (c"attach", attach_callback),
        (c"readState", read_state_callback),
        (c"reattach", reattach_callback),
        (c"nextEvent", next_event_callback),
        (c"publicationGeneration", publication_generation_callback),
        (c"wait", wait_callback),
        (c"nextDirty", next_dirty_callback),
        (c"marketDirectoryIndex", market_directory_index_callback),
    ];
    // SAFETY: `env`/`exports` are this call's live handles; every name is `'static` and
    // NUL-terminated.
    unsafe {
        for (name, callback) in functions {
            let mut value: NapiValue = std::ptr::null_mut();
            let status = (napi.create_function)(
                env,
                name.as_ptr(),
                name.to_bytes().len(),
                callback,
                std::ptr::null_mut(),
                &mut value,
            );
            if status != 0 {
                eprintln!("pm-ws napi: napi_create_function failed for {name:?}");
                continue;
            }
            if (napi.set_named_property)(env, exports, name.as_ptr(), value) != 0 {
                eprintln!("pm-ws napi: napi_set_named_property failed for {name:?}");
            }
        }
    }
    exports
}

#[cfg(test)]
mod tests {
    use super::{SnapshotKind, clears_pending_events};
    use crate::ffi;

    #[test]
    fn attach_clears_only_on_ok() {
        assert!(clears_pending_events(
            &SnapshotKind::Attach,
            ffi::PMWS_STATUS_OK
        ));
        assert!(!clears_pending_events(
            &SnapshotKind::Attach,
            ffi::PMWS_STATUS_CONTENDED
        ));
        assert!(!clears_pending_events(
            &SnapshotKind::Attach,
            ffi::PMWS_STATUS_BUFFER_TOO_SMALL
        ));
        assert!(!clears_pending_events(
            &SnapshotKind::Attach,
            ffi::PMWS_STATUS_INTERNAL
        ));
    }

    #[test]
    fn reattach_clears_only_on_ok() {
        assert!(clears_pending_events(
            &SnapshotKind::Reattach,
            ffi::PMWS_STATUS_OK
        ));
        assert!(!clears_pending_events(
            &SnapshotKind::Reattach,
            ffi::PMWS_STATUS_NOT_ATTACHED
        ));
        assert!(!clears_pending_events(
            &SnapshotKind::Reattach,
            ffi::PMWS_STATUS_CONTENDED
        ));
    }

    #[test]
    fn read_never_clears() {
        assert!(!clears_pending_events(
            &SnapshotKind::Read,
            ffi::PMWS_STATUS_OK
        ));
        assert!(!clears_pending_events(
            &SnapshotKind::Read,
            ffi::PMWS_STATUS_CONTENDED
        ));
    }
}
