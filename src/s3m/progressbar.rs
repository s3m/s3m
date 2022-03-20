use indicatif::{ProgressBar, ProgressStyle};

// "█▉▊▋▌▍▎▏  ·"
const PROGRES_CHARS: &str =
    "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f}  \u{b7}";

const PROGRES_CHARS_SPINNER: &[&str] = &[
    "\u{2801}", "\u{2802}", "\u{2804}", "\u{2840}", "\u{2880}", "\u{2820}", "\u{2810}", "\u{2808}",
    "",
];

#[derive(Default)]
pub struct Bar {
    pub progress: Option<ProgressBar>,
}

#[must_use]
impl Bar {
    #[must_use]
    pub fn new(file_size: u64) -> Self {
        let pb = ProgressBar::new(file_size);
        pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {bytes}/{total_bytes} ({bytes_per_sec} - {eta})")
            .progress_chars(PROGRES_CHARS),
    );
        Self { progress: Some(pb) }
    }

    #[must_use]
    pub fn new_spinner() -> Self {
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(200);
        pb.set_style(
            ProgressStyle::default_spinner()
                .tick_strings(PROGRES_CHARS_SPINNER)
                .template("checksum: {spinner:.green}"),
        );
        Self { progress: Some(pb) }
    }
}
