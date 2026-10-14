//! The shell behind the panels — what the command line runs in and what
//! Ctrl+O shows.
//!
//! On unix with a bash / zsh / fish `$SHELL`: MC's subshell. One `$SHELL`
//! for the whole session under a pty, its screen the terminal's normal
//! buffer (the panels draw on the alternate one). Its prompt hook writes
//! `$PWD` to a pipe and stops the shell (`kill -STOP $$`); reading that
//! line, waiting for the stop, draining the pty and resuming the shell is
//! the one synchronization point — after it the shell is idle at a fresh
//! prompt and every byte the last command printed has been read. What the
//! shell prints while the panels are up (the prompt after a command, the
//! `cd` a panel sends) waits in `pending` and is replayed onto the normal
//! buffer the next time the shell is shown.
//!
//! On Windows, and under any other `$SHELL`: FAR's console. A process per
//! command line (`%COMSPEC% /C`, `$SHELL -c`) with the terminal handed
//! over, the normal buffer keeping every command's echo and output.

use std::io::{self, Write};
use std::path::Path;

use crossterm::cursor;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};

use crate::local_path;

#[cfg(unix)]
pub use pty::{Left, PtyShell, Until};

/// The user's shell and its "run one command" flag.
#[cfg(not(windows))]
pub fn user_shell() -> (String, &'static str) {
    (std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string()), "-c")
}

#[cfg(windows)]
pub fn user_shell() -> (String, &'static str) {
    (std::env::var("COMSPEC").unwrap_or_else(|_| "cmd".to_string()), "/C")
}

/// The session's shell, per platform and `$SHELL`.
pub enum Shell {
    /// MC's subshell.
    #[cfg(unix)]
    Pty(PtyShell),
    /// FAR's console.
    Console,
}

impl Shell {
    /// The subshell where `$SHELL`'s prompt can be hooked, the console
    /// otherwise. `cols` × `rows` is the terminal, the pty's first size.
    pub fn open(cols: u16, rows: u16) -> Shell {
        #[cfg(unix)]
        {
            let (shell, _) = user_shell();
            match pty::ShellKind::of(&shell) {
                Some(kind) => match PtyShell::spawn(&shell, kind, cols, rows) {
                    Ok(p) => return Shell::Pty(p),
                    Err(e) => tracing::warn!(shell = %shell, "subshell: {e}; commands run on the console"),
                },
                None => tracing::info!(shell = %shell, "no prompt hook for this shell; commands run on the console"),
            }
        }
        #[cfg(not(unix))]
        let _ = (cols, rows);
        Shell::Console
    }
}

/// One command line on the console (the FAR model): its `cwd> command`
/// echo, then `$SHELL -c` / `%COMSPEC% /C` in `cwd` with the terminal
/// handed over, then its exit code when not zero. The terminal is on the
/// normal screen in raw mode before and after; whatever the cursor was
/// left after, the echo starts on a line of its own.
pub fn console_run(cwd: &Path, command: &str) -> io::Result<()> {
    let (shell, flag) = user_shell();
    {
        let mut out = io::stdout().lock();
        if cursor::position()?.0 > 0 {
            write!(out, "\r\n")?;
        }
        write!(out, "{}{}\r\n", local_path::prompt(cwd), command)?;
        out.flush()?;
    }
    disable_raw_mode()?;
    let result = std::process::Command::new(&shell)
        .arg(flag)
        .arg(command)
        .current_dir(cwd)
        .status();
    match result {
        Ok(status) if !status.success() => match status.code() {
            Some(code) => println!("[exit code {}]", code),
            None => println!("[{}]", status),
        },
        Err(e) => println!("{}: {}", shell, e),
        Ok(_) => {}
    }
    enable_raw_mode()
}

#[cfg(unix)]
mod pty {
    use std::io::{self, Write};
    use std::os::fd::{AsFd, AsRawFd, OwnedFd};
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use nix::errno::Errno;
    use nix::fcntl::{FcntlArg, FdFlag, OFlag, fcntl};
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use nix::pty::{Winsize, openpty};
    use nix::sys::signal::{Signal, kill};
    use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
    use nix::unistd::{Pid, pipe, read, write};

    /// How long the shell gets to reach a prompt when nobody is watching:
    /// its rc files at startup, a `cd` the panels sent.
    const QUIET_TIMEOUT: Duration = Duration::from_secs(10);
    /// The pump's tick, ms: window size checks between events.
    const TICK_MS: u16 = 50;

    /// The shells whose prompt the hook can ride.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ShellKind {
        Bash,
        Zsh,
        Fish,
    }

    impl ShellKind {
        pub fn of(shell: &str) -> Option<Self> {
            match Path::new(shell).file_name().and_then(|n| n.to_str()) {
                Some("bash") => Some(ShellKind::Bash),
                Some("zsh") => Some(ShellKind::Zsh),
                Some("fish") => Some(ShellKind::Fish),
                _ => None,
            }
        }
    }

    /// Where the shell is between two synchronization points.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        /// Running its rc files towards the first prompt.
        Starting,
        /// Idle at a prompt, reading the pty.
        Prompt,
        /// Executing a line — typed into it, or sent by the panels.
        Running,
    }

    /// What ends a view of the shell.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Until {
        /// The next prompt: a command sent from the panels' command line.
        Prompt,
        /// Ctrl+O typed at a prompt.
        CtrlO,
    }

    /// How a view of the shell ended.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Left {
        /// At a prompt, per [`Until`].
        Prompt,
        /// The shell exited (`exit`, a crash): the session is over.
        Died,
    }

    /// Where the shell's output goes while the pump runs.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Sink {
        /// The terminal: the shell is being shown.
        Screen,
        /// `pending`: the panels are up, the bytes wait for the next view.
        Pending,
    }

    /// What a read off the shell brought.
    enum Read {
        Data,
        Nothing,
        Gone,
    }

    pub struct PtyShell {
        master: OwnedFd,
        pid: Pid,
        /// The prompt hook's `pwd` lines.
        prompt_pipe: OwnedFd,
        /// A partial line off `prompt_pipe`.
        prompt_buf: Vec<u8>,
        state: State,
        /// The shell's directory as of its last prompt.
        cwd: PathBuf,
        /// Output produced while the panels were up, replayed on the next
        /// view.
        pending: Vec<u8>,
        /// The rc files the hook rides in; removed on drop.
        init_dir: PathBuf,
        /// The pty's window, kept equal to the terminal's.
        size: (u16, u16),
    }

    impl PtyShell {
        /// `shell` under a fresh pty, its prompt hooked per `kind`. It runs
        /// its rc files from here on; the first prompt is awaited lazily.
        pub fn spawn(shell: &str, kind: ShellKind, cols: u16, rows: u16) -> io::Result<Self> {
            let ws = Winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            let pty = openpty(&ws, None)?;
            // The write end reaches the shell under its own number — the
            // hook names it; the read end is ours alone.
            let (pipe_r, pipe_w) = pipe()?;
            fcntl(&pipe_r, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
            let fd = pipe_w.as_raw_fd();
            // One rc directory per shell (the tests start several).
            static SPAWNED: AtomicU64 = AtomicU64::new(0);
            let init_dir = std::env::temp_dir().join(format!(
                "sninja-shell-{}-{}",
                std::process::id(),
                SPAWNED.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&init_dir)?;

            let mut cmd = Command::new(shell);
            match kind {
                ShellKind::Bash => {
                    // `--rcfile` replaces the system and user rc: chain to them.
                    let rc = init_dir.join("bashrc");
                    std::fs::write(
                        &rc,
                        format!(
                            "for f in /etc/bash.bashrc /etc/bashrc \"$HOME/.bashrc\"; do [ -f \"$f\" ] && . \"$f\"; done\n\
                             PROMPT_COMMAND=\"${{PROMPT_COMMAND:+$PROMPT_COMMAND;}}pwd>&{fd};kill -STOP \\$\\$\"\n"
                        ),
                    )?;
                    cmd.arg("--rcfile").arg(&rc);
                }
                ShellKind::Zsh => {
                    // ZDOTDIR points here for the startup only: .zshenv and
                    // .zshrc chain to the user's own, then put ZDOTDIR back
                    // so a zsh started from this one does not hook itself.
                    if let Ok(orig) = std::env::var("ZDOTDIR") {
                        cmd.env("SNINJA_ZDOTDIR", orig);
                    }
                    std::fs::write(
                        init_dir.join(".zshenv"),
                        "[ -f \"${SNINJA_ZDOTDIR:-$HOME}/.zshenv\" ] && . \"${SNINJA_ZDOTDIR:-$HOME}/.zshenv\"\n",
                    )?;
                    std::fs::write(
                        init_dir.join(".zshrc"),
                        format!(
                            "[ -f \"${{SNINJA_ZDOTDIR:-$HOME}}/.zshrc\" ] && . \"${{SNINJA_ZDOTDIR:-$HOME}}/.zshrc\"\n\
                             if [ -n \"$SNINJA_ZDOTDIR\" ]; then ZDOTDIR=\"$SNINJA_ZDOTDIR\"; else unset ZDOTDIR; fi\n\
                             unset SNINJA_ZDOTDIR\n\
                             __sninja_precmd() {{ pwd >&{fd}; kill -STOP $$ }}\n\
                             precmd_functions+=(__sninja_precmd)\n"
                        ),
                    )?;
                    cmd.env("ZDOTDIR", &init_dir);
                }
                ShellKind::Fish => {
                    cmd.arg("--init-command").arg(format!(
                        "function __sninja_prompt --on-event fish_prompt; pwd >&{fd}; kill -STOP $fish_pid; end"
                    ));
                }
            }
            cmd.stdin(Stdio::from(pty.slave.try_clone()?))
                .stdout(Stdio::from(pty.slave.try_clone()?))
                .stderr(Stdio::from(pty.slave));
            // The shell leads its own session with the pty as its
            // controlling terminal (job control, ^C to its jobs).
            unsafe {
                cmd.pre_exec(|| {
                    libc::setsid();
                    if libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            // The `Child` is only its pid: it is neither awaited nor killed
            // through it (see `Drop`).
            let pid = Pid::from_raw(cmd.spawn()?.id() as i32);
            drop(pipe_w);
            // The master is read from a poll loop: never block on it.
            fcntl(&pty.master, FcntlArg::F_SETFL(OFlag::O_NONBLOCK))?;
            tracing::info!(shell, pid = pid.as_raw(), "subshell started");
            Ok(Self {
                master: pty.master,
                pid,
                prompt_pipe: pipe_r,
                prompt_buf: Vec::new(),
                state: State::Starting,
                cwd: PathBuf::new(),
                pending: Vec::new(),
                init_dir,
                size: (cols, rows),
            })
        }

        /// The shell's directory, as its last prompt reported it.
        pub fn cwd(&self) -> &Path {
            &self.cwd
        }

        /// Show the shell: `pending` onto the normal buffer, then the pump
        /// with the keyboard until `until`. The terminal is the caller's to
        /// hand over first (normal screen, raw mode).
        pub fn view(&mut self, until: Until) -> io::Result<Left> {
            self.ready()?;
            self.replay()?;
            self.pump(true, Sink::Screen, until, None)
        }

        /// A command line into the shell, shown until its prompt returns.
        pub fn run(&mut self, command: &str) -> io::Result<Left> {
            self.ready()?;
            self.replay()?;
            self.send(format!("{command}\n").as_bytes())?;
            self.state = State::Running;
            self.pump(true, Sink::Screen, Until::Prompt, None)
        }

        /// Move the idle shell to `dir` ahead of showing it (the panel's
        /// directory): a ` cd` it echoes — the leading space keeps it out of
        /// its history — awaited quietly.
        pub fn sync_cwd(&mut self, dir: &Path) -> io::Result<()> {
            self.ready()?;
            if self.state != State::Prompt || self.cwd == dir {
                return Ok(());
            }
            self.send(format!(" cd {}\n", quote_single(&dir.to_string_lossy())).as_bytes())?;
            self.state = State::Running;
            match self.pump(false, Sink::Pending, Until::Prompt, Some(QUIET_TIMEOUT))? {
                Left::Prompt => Ok(()),
                Left::Died => Err(died()),
            }
        }

        /// The first prompt, when the shell is still starting up.
        fn ready(&mut self) -> io::Result<()> {
            if self.state != State::Starting {
                return Ok(());
            }
            match self.pump(false, Sink::Pending, Until::Prompt, Some(QUIET_TIMEOUT))? {
                Left::Prompt => Ok(()),
                Left::Died => Err(died()),
            }
        }

        /// What the shell printed while the panels were up, onto the screen.
        fn replay(&mut self) -> io::Result<()> {
            if self.pending.is_empty() {
                return Ok(());
            }
            let mut out = io::stdout().lock();
            out.write_all(&self.pending)?;
            out.flush()?;
            self.pending.clear();
            Ok(())
        }

        /// The pump: the keyboard (when `keys`) to the shell, the shell to
        /// `sink`, the prompt hook synchronizing — until `until`, or
        /// `timeout` with no prompt.
        fn pump(&mut self, keys: bool, sink: Sink, until: Until, timeout: Option<Duration>) -> io::Result<Left> {
            let deadline = timeout.map(|t| Instant::now() + t);
            loop {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no prompt from the shell in {} s", QUIET_TIMEOUT.as_secs()),
                    ));
                }
                self.fit_window();
                let (master_ready, prompt_ready, keys_ready) = {
                    let stdin = io::stdin();
                    let mut fds = vec![
                        PollFd::new(self.master.as_fd(), PollFlags::POLLIN),
                        PollFd::new(self.prompt_pipe.as_fd(), PollFlags::POLLIN),
                    ];
                    if keys {
                        fds.push(PollFd::new(stdin.as_fd(), PollFlags::POLLIN));
                    }
                    match poll(&mut fds, PollTimeout::from(TICK_MS)) {
                        Ok(_) => {}
                        Err(Errno::EINTR) => continue,
                        Err(e) => return Err(e.into()),
                    }
                    (readable(&fds[0]), readable(&fds[1]), fds.get(2).is_some_and(readable))
                };
                if master_ready && matches!(self.read_master(sink)?, Read::Gone) {
                    return Ok(Left::Died);
                }
                if prompt_ready {
                    match self.read_prompt()? {
                        Read::Gone => return Ok(Left::Died),
                        Read::Data => {
                            while let Some(line) = self.take_prompt_line() {
                                if !self.synchronize(&line, sink)? {
                                    return Ok(Left::Died);
                                }
                                if until == Until::Prompt {
                                    return Ok(Left::Prompt);
                                }
                            }
                        }
                        Read::Nothing => {}
                    }
                }
                if keys_ready && let Some(left) = self.relay_keys(until)? {
                    return Ok(left);
                }
            }
        }

        /// The keyboard's bytes to the shell. At a prompt, Ctrl+O ends a
        /// [`Until::CtrlO`] view instead, and an Enter means a command runs.
        fn relay_keys(&mut self, until: Until) -> io::Result<Option<Left>> {
            let mut buf = [0u8; 1024];
            let n = read(io::stdin().as_fd(), &mut buf)?;
            if n == 0 {
                return Ok(Some(Left::Prompt));
            }
            let mut bytes = &buf[..n];
            let mut leave = false;
            if until == Until::CtrlO
                && self.state == State::Prompt
                && let Some(i) = bytes.iter().position(|&b| b == 0x0f)
            {
                bytes = &bytes[..i];
                leave = true;
            }
            if self.state == State::Prompt && bytes.iter().any(|&b| b == b'\r' || b == b'\n') {
                self.state = State::Running;
            }
            self.send(bytes)?;
            Ok(leave.then_some(Left::Prompt))
        }

        /// The shell hit its prompt hook: `line` is its pwd. Wait for its
        /// stop, take everything it printed before, let it go on to the
        /// prompt. `false` when it exited instead.
        fn synchronize(&mut self, line: &str, sink: Sink) -> io::Result<bool> {
            self.cwd = PathBuf::from(line);
            match waitpid(self.pid, Some(WaitPidFlag::WUNTRACED))? {
                WaitStatus::Stopped(..) => {}
                _ => return Ok(false),
            }
            while let Read::Data = self.read_master(sink)? {}
            kill(self.pid, Signal::SIGCONT)?;
            self.state = State::Prompt;
            Ok(true)
        }

        /// One read off the pty master into `sink`.
        fn read_master(&mut self, sink: Sink) -> io::Result<Read> {
            let mut buf = [0u8; 4096];
            match read(self.master.as_fd(), &mut buf) {
                Ok(0) => Ok(Read::Gone),
                Ok(n) => {
                    match sink {
                        Sink::Screen => {
                            let mut out = io::stdout().lock();
                            out.write_all(&buf[..n])?;
                            out.flush()?;
                        }
                        Sink::Pending => self.pending.extend_from_slice(&buf[..n]),
                    }
                    Ok(Read::Data)
                }
                Err(Errno::EAGAIN) => Ok(Read::Nothing),
                // The slave side closed: the shell is gone.
                Err(Errno::EIO) => Ok(Read::Gone),
                Err(e) => Err(e.into()),
            }
        }

        /// One read off the prompt pipe into `prompt_buf`.
        fn read_prompt(&mut self) -> io::Result<Read> {
            let mut buf = [0u8; 1024];
            match read(self.prompt_pipe.as_fd(), &mut buf) {
                Ok(0) => Ok(Read::Gone),
                Ok(n) => {
                    self.prompt_buf.extend_from_slice(&buf[..n]);
                    Ok(Read::Data)
                }
                Err(Errno::EAGAIN) => Ok(Read::Nothing),
                Err(e) => Err(e.into()),
            }
        }

        /// The next complete line off `prompt_buf`.
        fn take_prompt_line(&mut self) -> Option<String> {
            let end = self.prompt_buf.iter().position(|&b| b == b'\n')?;
            let line: Vec<u8> = self.prompt_buf.drain(..=end).collect();
            Some(String::from_utf8_lossy(&line[..end]).into_owned())
        }

        /// `bytes` into the pty, all of them (a full pty input buffer is
        /// waited out).
        fn send(&self, mut bytes: &[u8]) -> io::Result<()> {
            while !bytes.is_empty() {
                match write(self.master.as_fd(), bytes) {
                    Ok(n) => bytes = &bytes[n..],
                    Err(Errno::EAGAIN) | Err(Errno::EINTR) => {
                        let mut fds = [PollFd::new(self.master.as_fd(), PollFlags::POLLOUT)];
                        poll(&mut fds, PollTimeout::from(1000u16))?;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(())
        }

        /// The pty's window follows the terminal's.
        fn fit_window(&mut self) {
            let Ok(size) = crossterm::terminal::size() else {
                return;
            };
            if size == self.size {
                return;
            }
            let ws = Winsize {
                ws_row: size.1,
                ws_col: size.0,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            // SAFETY: TIOCSWINSZ takes a pointer to a winsize struct; ws outlives the call.
            let rc = unsafe { libc::ioctl(self.master.as_raw_fd(), libc::TIOCSWINSZ as _, &ws as *const Winsize) };
            if rc == 0 {
                self.size = size;
            }
        }
    }

    impl Drop for PtyShell {
        /// Hang the shell up: continued first (it may sit stopped at its
        /// hook, where a hangup would wait for that forever), then HUP. Not
        /// awaited — nothing here depends on its exit, and a wait would sit
        /// on the pty's unread output (the slave's last close drains it into
        /// a master nobody reads). The master closes with the struct.
        fn drop(&mut self) {
            let _ = kill(self.pid, Signal::SIGCONT);
            let _ = kill(self.pid, Signal::SIGHUP);
            let _ = std::fs::remove_dir_all(&self.init_dir);
        }
    }

    fn readable(fd: &PollFd) -> bool {
        fd.revents()
            .is_some_and(|r| r.intersects(PollFlags::POLLIN | PollFlags::POLLHUP | PollFlags::POLLERR))
    }

    fn died() -> io::Error {
        io::Error::other("the shell exited")
    }

    /// `s` as one single-quoted shell word (bash, zsh and fish all read
    /// `'\''` as a quote inside it).
    fn quote_single(s: &str) -> String {
        format!("'{}'", s.replace('\'', "'\\''"))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn quotes_a_directory_with_a_quote() {
            assert_eq!(quote_single("/tmp/it's"), "'/tmp/it'\\''s'");
        }

        #[test]
        fn shell_kind_by_basename() {
            assert_eq!(ShellKind::of("/bin/zsh"), Some(ShellKind::Zsh));
            assert_eq!(ShellKind::of("/usr/local/bin/fish"), Some(ShellKind::Fish));
            assert_eq!(ShellKind::of("/bin/sh"), None);
        }

        /// The hook protocol end to end on a real shell under a pty, with
        /// nobody watching: the first prompt, a `cd` the panel sends, the
        /// shell's exit.
        fn hook_round_trip(shell: &str, kind: ShellKind) {
            let mut sh = PtyShell::spawn(shell, kind, 80, 24).expect("spawn");
            sh.ready().expect("first prompt");
            assert_eq!(sh.state, State::Prompt);
            let dir = std::env::temp_dir().join(format!("sninja-shell-test-{}-{:?}", std::process::id(), kind));
            std::fs::create_dir_all(&dir).unwrap();
            sh.sync_cwd(&dir).expect("cd");
            assert_eq!(sh.cwd(), dir);
            assert_eq!(sh.state, State::Prompt);
            sh.send(b"exit\n").unwrap();
            sh.state = State::Running;
            let left = sh.pump(false, Sink::Pending, Until::Prompt, Some(QUIET_TIMEOUT)).expect("pump");
            assert_eq!(left, Left::Died);
            std::fs::remove_dir_all(&dir).unwrap();
        }

        /// Dropping a shell nobody ever showed — stopped at its first hook,
        /// its output unread — returns at once and the shell goes away.
        #[test]
        fn drop_of_a_never_viewed_shell_returns_and_ends_it() {
            let sh = PtyShell::spawn("/bin/bash", ShellKind::Bash, 80, 24).expect("spawn");
            let pid = sh.pid;
            std::thread::sleep(Duration::from_millis(1500));
            let t0 = Instant::now();
            drop(sh);
            assert!(t0.elapsed() < Duration::from_millis(500), "drop took {:?}", t0.elapsed());
            let gone = (0..100).any(|_| {
                std::thread::sleep(Duration::from_millis(50));
                !matches!(waitpid(pid, Some(WaitPidFlag::WNOHANG)), Ok(WaitStatus::StillAlive))
            });
            assert!(gone, "the shell is still alive after the drop");
        }

        #[test]
        fn bash_hook_round_trip() {
            hook_round_trip("/bin/bash", ShellKind::Bash);
        }

        #[test]
        fn zsh_hook_round_trip() {
            if !Path::new("/bin/zsh").exists() {
                eprintln!("no /bin/zsh here: skipped");
                return;
            }
            hook_round_trip("/bin/zsh", ShellKind::Zsh);
        }

        #[test]
        fn fish_hook_round_trip() {
            let Some(fish) = ["/opt/homebrew/bin/fish", "/usr/local/bin/fish", "/usr/bin/fish"]
                .into_iter()
                .find(|p| Path::new(p).exists())
            else {
                eprintln!("no fish here: skipped");
                return;
            };
            hook_round_trip(fish, ShellKind::Fish);
        }
    }
}
