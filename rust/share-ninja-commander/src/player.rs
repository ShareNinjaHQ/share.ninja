//! The mini music player (Enter on an mp3/aac/m4a file): a modal dialog that
//! decodes with symphonia and plays through cpal, with a small FFT spectrum.
//!
//! Audio bytes come through a [`StorageReader`], the `Read + Seek` view over
//! a storage key's ranges — the same primitive the F3 viewer pages through —
//! so a local file and a share file (streamed off the warm transfer's P2P /
//! S3 range reads) play through one code path, and nothing is ever copied to
//! disk.
//!
//! Threading: one decode thread owns the symphonia format reader/decoder and
//! the cpal output stream (built on the thread — cpal streams are not `Send`
//! everywhere). Decoded f32 stereo at the device rate flows through
//! `PlayerShared::queue` to the audio callback; the callback tees what it
//! actually played into `PlayerShared::viz` for the spectrum, so the bars
//! track the audible signal, not the decode-ahead. The UI reads/writes only
//! `PlayerShared` atomics plus a seek command channel, and computes the FFT
//! on its own draw tick.

use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use parking_lot::Mutex;
use rustfft::num_complex::Complex;
use rustfft::{Fft, FftPlanner};
use symphonia::core::codecs::CodecParameters;
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatReader, SeekMode, SeekTo, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::{MetadataOptions, StandardTag};
use symphonia::core::units::Time;

use crate::engine::Engine;
use crate::storage_reader::StorageReader;

/// Extensions the player claims on Enter. AAC almost always ships in an MP4
/// container, so `.m4a` rides along with the raw `.aac`/`.mp3` cases.
pub const PLAYER_EXTENSIONS: [&str; 3] = ["mp3", "aac", "m4a"];

/// Returns the lowercase extension when `name` is a playable audio file.
pub fn playable_extension(name: &str) -> Option<String> {
    let ext = name.rsplit_once('.')?.1.to_ascii_lowercase();
    PLAYER_EXTENSIONS.contains(&ext.as_str()).then_some(ext)
}

/// Queued audio bound: ~1 s of stereo at 48 kHz. Backpressure for the decode
/// thread, and the ceiling on how far the sound lags a pause/seek.
const QUEUE_LIMIT: usize = 2 * 48_000;
/// Mono samples retained for the spectrum: enough for one FFT window.
const VIZ_KEEP: usize = FFT_SIZE;

const FFT_SIZE: usize = 2048;
/// Spectrum bands rendered by the dialog.
pub const BANDS: usize = 24;

/// UI → decode-thread commands. Pause, volume and stop are atomics on
/// [`PlayerShared`] (the callback reads them directly); only seeks need the
/// thread's cooperation.
enum PlayerCmd {
    /// Seek relative to the current audible position, in seconds.
    SeekBy(i64),
}

/// State shared between the UI, the decode thread and the audio callback.
pub struct PlayerShared {
    /// Interleaved stereo f32 at the device rate, decode thread → callback.
    queue: Mutex<VecDeque<f32>>,
    /// Most recent mono samples the callback actually played (spectrum input).
    viz: Mutex<VecDeque<f32>>,
    pub paused: AtomicBool,
    /// Decode reached end of stream (the queue may still be draining).
    finished: AtomicBool,
    /// Tells the thread (possibly parked in a storage read) to wind down.
    stop: AtomicBool,
    /// Volume in permille (0..=1000), applied in the callback.
    pub volume_permille: AtomicU32,
    /// Frames the callback consumed since the last seek epoch.
    played_frames: AtomicU64,
    /// Position of the current seek epoch, in ms.
    pos_base_ms: AtomicU64,
    /// Output sample rate; 0 until the stream is up.
    device_rate: AtomicU32,
    /// Track duration in ms; 0 while unknown (raw ADTS, VBR without a table).
    duration_ms: AtomicU64,
    /// Fatal open/decode error, displayed by the dialog.
    error: Mutex<Option<String>>,
    /// "Artist — Title" from the tags, once probed.
    tag_line: Mutex<Option<String>>,
    /// e.g. "MP3 · 44.1 kHz · stereo", once the track is known.
    format_line: Mutex<Option<String>>,
}

impl PlayerShared {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            viz: Mutex::new(VecDeque::new()),
            paused: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            volume_permille: AtomicU32::new(1000),
            played_frames: AtomicU64::new(0),
            pos_base_ms: AtomicU64::new(0),
            device_rate: AtomicU32::new(0),
            duration_ms: AtomicU64::new(0),
            error: Mutex::new(None),
            tag_line: Mutex::new(None),
            format_line: Mutex::new(None),
        }
    }

    /// Audible position in ms: the seek epoch plus what the callback played.
    pub fn position_ms(&self) -> u64 {
        let rate = self.device_rate.load(Ordering::Relaxed) as u64;
        let base = self.pos_base_ms.load(Ordering::Relaxed);
        if rate == 0 {
            return base;
        }
        base + self.played_frames.load(Ordering::Relaxed) * 1000 / rate
    }

    pub fn duration_ms(&self) -> u64 {
        self.duration_ms.load(Ordering::Relaxed)
    }

    pub fn error(&self) -> Option<String> {
        self.error.lock().clone()
    }

    pub fn tag_line(&self) -> Option<String> {
        self.tag_line.lock().clone()
    }

    pub fn format_line(&self) -> Option<String> {
        self.format_line.lock().clone()
    }

    /// Playback ended: decode done and the queue drained.
    pub fn is_done(&self) -> bool {
        self.finished.load(Ordering::Relaxed) && self.queue.lock().is_empty()
    }

    fn fail(&self, msg: String) {
        tracing::error!("player: {msg}");
        *self.error.lock() = Some(msg);
    }
}

/// The UI-side handle: shared state, the seek channel, and the spectrum
/// smoothing computed on the draw tick.
pub struct PlayerState {
    pub title: String,
    pub shared: Arc<PlayerShared>,
    cmd_tx: Sender<PlayerCmd>,
    fft: Arc<dyn Fft<f32>>,
    fft_buf: Vec<Complex<f32>>,
    /// Smoothed band magnitudes (0..1), fast attack / slow decay.
    pub bands: Vec<f32>,
    /// Peak-hold markers above the bars, decaying slower than the bands.
    pub peaks: Vec<f32>,
}

impl PlayerState {
    /// Space: flip pause. Takes effect in the audio callback immediately.
    pub fn toggle_pause(&self) {
        self.shared.paused.fetch_xor(true, Ordering::Relaxed);
    }

    /// ←/→: seek by whole seconds relative to the audible position.
    pub fn seek_by(&self, secs: i64) {
        let _ = self.cmd_tx.send(PlayerCmd::SeekBy(secs));
    }

    /// ↑/↓: volume in 5% steps, clamped to 0..=100%.
    pub fn volume_step(&self, up: bool) {
        let v = self.shared.volume_permille.load(Ordering::Relaxed) as i64;
        let v = (v + if up { 50 } else { -50 }).clamp(0, 1000);
        self.shared.volume_permille.store(v as u32, Ordering::Relaxed);
    }

    /// Signal the decode thread to wind down; it may outlive the dialog for
    /// the duration of one in-flight storage read, holding only `shared`.
    pub fn stop(&self) {
        self.shared.stop.store(true, Ordering::Relaxed);
    }

    /// Per-draw spectrum update: FFT the freshest played samples into
    /// log-spaced bands with attack/decay smoothing. Cheap enough for the
    /// 50 ms draw tick (one 2048-point FFT).
    pub fn tick(&mut self) {
        let rate = self.shared.device_rate.load(Ordering::Relaxed) as f32;
        if rate == 0.0 {
            return;
        }
        {
            let viz = self.shared.viz.lock();
            if viz.len() < FFT_SIZE {
                // Not enough played audio yet (or just seeked): decay only.
                drop(viz);
                self.decay_only();
                return;
            }
            let start = viz.len() - FFT_SIZE;
            for (i, s) in viz.iter().skip(start).enumerate() {
                // Hann window.
                let w = 0.5 - 0.5 * (2.0 * std::f32::consts::PI * i as f32 / (FFT_SIZE - 1) as f32).cos();
                self.fft_buf[i] = Complex::new(s * w, 0.0);
            }
        }
        if self.shared.paused.load(Ordering::Relaxed) || self.shared.is_done() {
            self.decay_only();
            return;
        }
        self.fft.process(&mut self.fft_buf);

        // Log-spaced bands between ~50 Hz and 16 kHz (capped at Nyquist).
        let f_lo: f32 = 50.0;
        let f_hi: f32 = (rate / 2.0).min(16_000.0);
        let bin_hz = rate / FFT_SIZE as f32;
        for b in 0..BANDS {
            let lo = f_lo * (f_hi / f_lo).powf(b as f32 / BANDS as f32);
            let hi = f_lo * (f_hi / f_lo).powf((b + 1) as f32 / BANDS as f32);
            let lo_bin = ((lo / bin_hz) as usize).max(1);
            let hi_bin = ((hi / bin_hz).ceil() as usize).clamp(lo_bin + 1, FFT_SIZE / 2);
            let mut power = 0.0f32;
            for bin in lo_bin..hi_bin {
                power += self.fft_buf[bin].norm_sqr();
            }
            let rms = (power / (hi_bin - lo_bin) as f32).sqrt() / (FFT_SIZE as f32 / 4.0);
            // ~60 dB display range.
            let db = 20.0 * (rms + 1e-9).log10();
            let v = ((db + 60.0) / 60.0).clamp(0.0, 1.0);
            self.bands[b] = v.max(self.bands[b] * 0.75);
            self.peaks[b] = self.bands[b].max(self.peaks[b] - 0.02);
        }
    }

    fn decay_only(&mut self) {
        for b in 0..BANDS {
            self.bands[b] *= 0.75;
            self.peaks[b] = (self.peaks[b] - 0.02).max(0.0);
        }
    }
}

/// Open the player on a storage key and start its decode thread. The dialog
/// can render immediately; probe results and errors land in `shared`.
pub fn open(
    engine: Arc<Engine>,
    rt: tokio::runtime::Handle,
    storage_id: String,
    path: String,
    title: String,
    extension: String,
) -> PlayerState {
    let shared = Arc::new(PlayerShared::new());
    let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();

    let thread_shared = shared.clone();
    std::thread::Builder::new()
        .name("player-decode".into())
        .spawn(move || {
            if let Err(e) = decode_thread(engine, rt, storage_id, path, extension, &thread_shared, cmd_rx) {
                thread_shared.fail(e);
            }
        })
        .expect("spawn player thread");

    let fft = FftPlanner::new().plan_fft_forward(FFT_SIZE);
    PlayerState {
        title,
        shared,
        cmd_tx,
        fft,
        fft_buf: vec![Complex::new(0.0, 0.0); FFT_SIZE],
        bands: vec![0.0; BANDS],
        peaks: vec![0.0; BANDS],
    }
}

// ── storage-backed media source ────────────────────────────────────────────

/// The player's `MediaSource`: a [`StorageReader`] that refuses reads once
/// the player is closed (a block fetch in flight is not interrupted, but no
/// further one starts). Lives only on the decode thread, which is allowed
/// to wait — a cold P2P seek stalls playback, not the UI.
struct PlayerReader {
    inner: StorageReader,
    stop: Arc<PlayerShared>,
}

impl Read for PlayerReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.stop.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("player closed"));
        }
        self.inner.read(buf)
    }
}

impl Seek for PlayerReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl MediaSource for PlayerReader {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.inner.size())
    }
}

// ── decode thread ──────────────────────────────────────────────────────────

fn decode_thread(
    engine: Arc<Engine>,
    rt: tokio::runtime::Handle,
    storage_id: String,
    path: String,
    extension: String,
    shared: &Arc<PlayerShared>,
    cmd_rx: Receiver<PlayerCmd>,
) -> Result<(), String> {
    let size = rt
        .block_on(engine.file_size(&storage_id, &path))
        .map_err(|e| format!("open failed: {e}"))?;
    let source = rt
        .block_on(engine.source(&storage_id))
        .map_err(|e| format!("open failed: {e}"))?;
    let reader = PlayerReader {
        inner: StorageReader::new(source, path, size, rt),
        stop: shared.clone(),
    };

    let mss = MediaSourceStream::new(Box::new(reader), Default::default());
    let mut hint = Hint::new();
    hint.with_extension(&extension);
    let mut format = symphonia::default::get_probe()
        .probe(&hint, mss, Default::default(), MetadataOptions::default())
        .map_err(|e| format!("unrecognized audio: {e}"))?;

    let track = format
        .default_track(TrackType::Audio)
        .ok_or("no audio track")?
        .clone();
    let track_id = track.id;
    let Some(CodecParameters::Audio(params)) = track.codec_params else {
        return Err("no audio track".into());
    };
    let mut decoder = symphonia::default::get_codecs()
        .make_audio_decoder(&params, &Default::default())
        .map_err(|e| format!("unsupported codec: {e}"))?;

    let src_rate = params.sample_rate.unwrap_or(44_100);
    let time_base = track.time_base;
    if let (Some(dur), Some(tb)) = (track.duration, time_base)
        && let Some(t) = tb.calc_duration(dur)
    {
        shared
            .duration_ms
            .store(t.as_millis().max(0) as u64, Ordering::Relaxed);
    }
    let channels = params.channels.as_ref().map(|c| c.count()).unwrap_or(2);
    *shared.format_line.lock() = Some(format!(
        "{} · {:.1} kHz · {}",
        extension.to_ascii_uppercase(),
        src_rate as f32 / 1000.0,
        if channels == 1 { "mono" } else { "stereo" },
    ));
    read_tags(&mut *format, shared);

    // The output stream: default device, default config, our sample pump.
    let stream = build_stream(shared.clone())?;
    let device_rate = shared.device_rate.load(Ordering::Relaxed) as f64;

    // Linear resampler state: src position advances by src_rate/device_rate
    // per output frame. Identity when the rates match.
    let step = src_rate as f64 / device_rate;
    let mut resample_pos = 0.0f64;
    // Interleaved f32 the decoded packet is copied into, reused every packet.
    let mut samples: Vec<f32> = Vec::new();

    loop {
        if shared.stop.load(Ordering::Relaxed) {
            break;
        }
        while let Ok(cmd) = cmd_rx.try_recv() {
            let PlayerCmd::SeekBy(secs) = cmd;
            let cur_ms = shared.position_ms() as i64;
            let target_ms = (cur_ms + secs * 1000).max(0) as u64;
            let time = Time::from_millis_u64(target_ms);
            match format.seek(
                SeekMode::Coarse,
                SeekTo::Time {
                    time,
                    track_id: Some(track_id),
                },
            ) {
                Ok(seeked) => {
                    decoder.reset();
                    shared.queue.lock().clear();
                    shared.viz.lock().clear();
                    let actual_ms = time_base
                        .and_then(|tb| tb.calc_time(seeked.actual_ts))
                        .map(|t| t.as_millis().max(0) as u64)
                        .unwrap_or(target_ms);
                    shared.pos_base_ms.store(actual_ms, Ordering::Relaxed);
                    shared.played_frames.store(0, Ordering::Relaxed);
                    shared.finished.store(false, Ordering::Relaxed);
                    resample_pos = 0.0;
                }
                Err(e) => {
                    // Raw ADTS without a seek index: stay put.
                    tracing::debug!("player: seek failed: {e}");
                }
            }
        }
        if shared.paused.load(Ordering::Relaxed) || shared.queue.lock().len() >= QUEUE_LIMIT {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }

        let packet = match format.next_packet() {
            Ok(Some(p)) => p,
            Ok(None) => {
                shared.finished.store(true, Ordering::Relaxed);
                break;
            }
            Err(SymphoniaError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                shared.finished.store(true, Ordering::Relaxed);
                break;
            }
            Err(e) => {
                return Err(format!("read failed: {e}"));
            }
        };
        if packet.track_id != track_id {
            continue;
        }
        let decoded = match decoder.decode(&packet) {
            Ok(d) => d,
            Err(SymphoniaError::DecodeError(e)) => {
                // A corrupt frame: skip it, keep playing.
                tracing::debug!("player: bad frame: {e}");
                continue;
            }
            Err(e) => {
                return Err(format!("decode failed: {e}"));
            }
        };

        let ch = decoded.spec().channels().count().max(1);
        decoded.copy_to_vec_interleaved(&mut samples);

        // Fold to stereo, resample linearly to the device rate, enqueue.
        let frames = samples.len() / ch;
        let mut out = Vec::with_capacity((frames as f64 / step) as usize * 2 + 4);
        let frame_at = |i: usize| -> [f32; 2] {
            let base = i * ch;
            let l = samples[base];
            let r = if ch > 1 { samples[base + 1] } else { l };
            [l, r]
        };
        while resample_pos < frames as f64 {
            let i = resample_pos as usize;
            let frac = (resample_pos - i as f64) as f32;
            let a = frame_at(i);
            let b = frame_at((i + 1).min(frames - 1));
            out.push(a[0] + (b[0] - a[0]) * frac);
            out.push(a[1] + (b[1] - a[1]) * frac);
            resample_pos += step;
        }
        resample_pos -= frames as f64;
        shared.queue.lock().extend(out);
    }

    // Keep the stream alive until the queue drains or the dialog closes.
    while !shared.stop.load(Ordering::Relaxed) && !shared.queue.lock().is_empty() {
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(stream);
    Ok(())
}

/// "Artist — Title" from the container tags, when present.
fn read_tags(format: &mut (dyn FormatReader + '_), shared: &PlayerShared) {
    let mut metadata = format.metadata();
    let Some(rev) = metadata.skip_to_latest() else {
        return;
    };
    let mut title = None;
    let mut artist = None;
    for tag in &rev.media.tags {
        match &tag.std {
            Some(StandardTag::TrackTitle(t)) => title = Some(t.to_string()),
            Some(StandardTag::Artist(a)) => artist = Some(a.to_string()),
            _ => {}
        }
    }
    let line = match (artist, title) {
        (Some(a), Some(t)) => Some(format!("{a} — {t}")),
        (None, Some(t)) => Some(t),
        (Some(a), None) => Some(a),
        (None, None) => None,
    };
    if line.is_some() {
        *shared.tag_line.lock() = line;
    }
}

// ── audio output ───────────────────────────────────────────────────────────

/// Build and start the output stream on the default device/config. The
/// callback pulls stereo frames off the queue, applies the volume, maps them
/// onto however many channels the device has, counts consumed frames for the
/// position display, and tees mono into the viz tail.
fn build_stream(shared: Arc<PlayerShared>) -> Result<cpal::Stream, String> {
    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

    let device = cpal::default_host()
        .default_output_device()
        .ok_or("no audio output device")?;
    let config = device
        .default_output_config()
        .map_err(|e| format!("no output config: {e}"))?;
    shared.device_rate.store(config.sample_rate(), Ordering::Relaxed);

    let stream = match config.sample_format() {
        cpal::SampleFormat::F32 => run_stream::<f32>(&device, config.into(), shared),
        cpal::SampleFormat::I16 => run_stream::<i16>(&device, config.into(), shared),
        cpal::SampleFormat::U16 => run_stream::<u16>(&device, config.into(), shared),
        other => Err(format!("unsupported sample format {other}")),
    }?;
    stream.play().map_err(|e| format!("audio start failed: {e}"))?;
    Ok(stream)
}

fn run_stream<T>(
    device: &cpal::Device,
    config: cpal::StreamConfig,
    shared: Arc<PlayerShared>,
) -> Result<cpal::Stream, String>
where
    T: cpal::SizedSample + cpal::FromSample<f32>,
{
    use cpal::traits::DeviceTrait;

    let channels = config.channels as usize;
    device
        .build_output_stream(
            config,
            move |data: &mut [T], _| {
                let volume = shared.volume_permille.load(Ordering::Relaxed) as f32 / 1000.0;
                let paused = shared.paused.load(Ordering::Relaxed);
                let mut queue = shared.queue.lock();
                let mut viz = shared.viz.lock();
                let mut consumed = 0u64;
                for frame in data.chunks_mut(channels) {
                    let (l, r) = if paused {
                        (0.0, 0.0)
                    } else if let (Some(l), Some(r)) = (queue.pop_front(), queue.pop_front()) {
                        consumed += 1;
                        viz.push_back((l + r) * 0.5);
                        (l * volume, r * volume)
                    } else {
                        // Underrun (or drained at EOF): silence.
                        (0.0, 0.0)
                    };
                    for (i, out) in frame.iter_mut().enumerate() {
                        let s = if i % 2 == 0 { l } else { r };
                        *out = T::from_sample(s);
                    }
                }
                while viz.len() > VIZ_KEEP {
                    viz.pop_front();
                }
                if consumed > 0 {
                    shared.played_frames.fetch_add(consumed, Ordering::Relaxed);
                }
            },
            |e| tracing::warn!("player: stream error: {e}"),
            None,
        )
        .map_err(|e| format!("audio open failed: {e}"))
}
