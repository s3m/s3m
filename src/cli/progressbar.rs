use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;

// "█▉▊▋▌▍▎▏  ·"
const PROGRES_CHARS: &str =
    "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f}  \u{b7}";

const PROGRES_CHARS_SPINNER: &[&str] = &["◜", "◠", "◝", "◞", "◡", "◟"];

#[derive(Default, Debug)]
pub struct Bar {
    pub progress: Option<ProgressBar>,
}

pub struct StreamBars {
    _multi: MultiProgress,
    pub staging: ProgressBar,
    pub status: ProgressBar,
}

impl Bar {
    #[must_use]
    pub fn new(file_size: u64) -> Self {
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
    pub fn new_stream(buffer_size: u64) -> Option<StreamBars> {
        let multi = MultiProgress::new();

        let staging = multi.add(ProgressBar::new(buffer_size));
        let staging_style = match ProgressStyle::with_template(
            "[{elapsed_precise}] buffering {bytes}/{total_bytes}",
        ) {
            Ok(style) => style,
            Err(err) => {
                eprintln!("Error creating staging progress style: {err}");
                return None;
            }
        };
        staging.set_style(staging_style);

        let status = multi.add(ProgressBar::new_spinner());
        status.enable_steady_tick(Duration::from_millis(200));

        let uploaded_style = match ProgressStyle::default_spinner()
            .tick_strings(PROGRES_CHARS_SPINNER)
            .template("[{elapsed_precise}] {msg} {spinner:.green}")
        {
            Ok(style) => style,
            Err(err) => {
                eprintln!("Error creating uploaded progress style: {err}");
                return None;
            }
        };
        status.set_style(uploaded_style);

        Some(StreamBars {
            _multi: multi,
            staging,
            status,
        })
    }
}
