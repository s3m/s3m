use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

// "█▉▊▋▌▍▎▏  ·"
const PROGRES_CHARS: &str =
    "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f}  \u{b7}";

const PROGRES_CHARS_SPINNER: &[&str] = &[
    "\u{2801}", "\u{2802}", "\u{2804}", "\u{2840}", "\u{2880}", "\u{2820}", "\u{2810}", "\u{2808}",
    "",
];

#[derive(Default, Debug)]
pub struct Bar {
    pub progress: Option<ProgressBar>,
}

impl Bar {
    #[must_use]
    pub fn new(file_size: u64, quiet: Option<bool>) -> Self {
        if quiet == Some(true) {
            return Self::default();
        }

        let pb = ProgressBar::new(file_size);

        let style_result = ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {bytes}/{total_bytes} ({bytes_per_sec} - {eta})");

        let style = match style_result {
            Ok(style) => style,
            Err(err) => {
                eprintln!("Error creating progress bar style: {err}");
                return Self { progress: None };
            }
        };

        pb.set_style(style.progress_chars(PROGRES_CHARS));

        Self { progress: Some(pb) }
    }

    #[must_use]
    pub fn new_spinner() -> Self {
        let pb = ProgressBar::new_spinner();

        pb.enable_steady_tick(Duration::from_millis(200));

        let style_result = ProgressStyle::default_spinner()
            .tick_strings(PROGRES_CHARS_SPINNER)
            .template("checksum: {spinner:.green}");

        let style = match style_result {
            Ok(s) => s,
            Err(err) => {
                eprintln!("Error creating spinner style: {err}");
                return Self { progress: None };
            }
        };

        pb.set_style(style);

        Self { progress: Some(pb) }
    }

    #[must_use]
    pub fn new_spinner_stream() -> Self {
        let pb = ProgressBar::new_spinner();

        pb.enable_steady_tick(Duration::from_millis(200));

        let style_result = ProgressStyle::default_spinner()
            .tick_strings(PROGRES_CHARS_SPINNER)
            .template("[{elapsed_precise}] {msg} {spinner:.green}");

        let style = match style_result {
            Ok(s) => s,
            Err(err) => {
                eprintln!("Error creating spinner style: {err}");
                return Self { progress: None };
            }
        };

        pb.set_style(style);

        Self { progress: Some(pb) }
    }
}
