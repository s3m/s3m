use super::{MonitorCheck, MonitorOutputFormat, handle};
use crate::{
    cli::{actions::Action, config::MonitorRule},
    s3::{Credentials, Region, S3},
};
use anyhow::Result;
use futures::FutureExt;
use log::{LevelFilter, Log, Metadata, Record};
use secrecy::SecretString;
use std::{cell::RefCell, net::TcpListener, pin::pin};

thread_local! {
    static SIGNED: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
}

struct SigningCapture(LevelFilter);

impl Log for SigningCapture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        SIGNED.with_borrow_mut(|capture| {
            if let Some(urls) = capture
                && let Some(url) = record.args().to_string().strip_prefix("URL to sign: ")
            {
                urls.push(url.to_owned());
            }
        });
    }

    fn flush(&self) {}
}

impl Drop for SigningCapture {
    fn drop(&mut self) {
        SIGNED.with_borrow_mut(|capture| *capture = None);
        log::set_max_level(self.0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn monitor_handle_applies_number_before_starting_checks() -> Result<()> {
    static LOGGER: SigningCapture = SigningCapture(LevelFilter::Off);
    log::set_logger(&LOGGER)?;
    let _capture = SigningCapture(log::max_level());
    log::set_max_level(LevelFilter::Info);

    for (number, expected) in [
        (0, vec!["/first"]),
        (1, vec!["/first"]),
        (2, vec!["/first", "/second"]),
    ] {
        // Keep the endpoint open without accepting or answering either request.
        // Each admitted check stays independently pending on its own HTTP future.
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let s3 = S3::new(
            &Credentials::new("access", &SecretString::new("secret".into())),
            &Region::custom("test", endpoint),
            None,
            false,
        );
        let checks = ["first", "second"]
            .map(|bucket| MonitorCheck {
                host: "admission".into(),
                bucket: bucket.into(),
                rule: MonitorRule {
                    prefix: "held".into(),
                    suffix: String::new(),
                    age: 60,
                    size: 0,
                },
            })
            .to_vec();
        SIGNED.with_borrow_mut(|capture| *capture = Some(Vec::new()));
        let mut action = pin!(handle(
            &s3,
            Action::Monitor {
                host: "admission".into(),
                checks,
                format: MonitorOutputFormat::Prometheus,
                exit_on_check_failure: true,
                number,
            }
        ));

        // A complete poll reaches Pending after polling every admitted check.
        // Signing occurs synchronously before the network await, so this is a
        // closed admission observation, with no elapsed absence measurement.
        assert!(action.as_mut().now_or_never().is_none(), "number={number}");
        let signed = SIGNED.with_borrow_mut(|capture| capture.take().unwrap_or_default());
        let paths = signed
            .iter()
            .map(|url| url::Url::parse(url).map(|url| url.path().to_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(paths, expected, "number={number}");
    }
    Ok(())
}
