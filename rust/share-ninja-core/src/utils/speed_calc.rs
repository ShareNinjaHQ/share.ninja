//! Speed calculation utilities for transfer monitoring

use std::time::Instant;

/// Calculates average bytes per second over a sliding window
pub struct SpeedCalc {
    window_ms: u64,
    samples: Vec<(Instant, u64)>,
    last_total: u64,
}

impl SpeedCalc {
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            samples: Vec::new(),
            last_total: 0,
        }
    }

    pub fn update_total(&mut self, total: u64) {
        let now = Instant::now();
        self.samples.push((now, total));
        self.last_total = total;

        // Remove old samples outside the window
        let cutoff = now - std::time::Duration::from_millis(self.window_ms);
        self.samples.retain(|(t, _)| *t >= cutoff);
    }

    pub fn avg_bytes_per_sec(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }

        let first = &self.samples[0];
        let last = &self.samples[self.samples.len() - 1];

        let duration = last.0.duration_since(first.0);
        let bytes = last.1.saturating_sub(first.1);

        if duration.as_secs_f64() > 0.0 {
            bytes as f64 / duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_speed_calc() {
        let mut calc = SpeedCalc::new(1000);
        calc.update_total(0);
        sleep(Duration::from_millis(100));
        calc.update_total(1000);

        let bps = calc.avg_bytes_per_sec();
        // Should be approximately 10000 bytes/sec (1000 bytes in 0.1 sec)
        assert!(bps > 5000.0 && bps < 15000.0);
    }
}
