use super::queued;
use anyhow::{Context, Result, ensure};
use socket2::{Domain, SockRef, Socket, Type};
use std::{
    io::{self, Write as _},
    net::SocketAddr,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt as _,
    net::TcpStream,
    time::{Instant, sleep, timeout},
};

const RECEIVE_BUFFER: usize = 4 * 1024;
const SEND_BUFFER: usize = 64 * 1024;
const WRITE_LIMIT: u64 = 4 * 1024 * 1024;
const READ_BYTES: u64 = 32 * 1024;
const WAIT: Duration = Duration::from_secs(5);

fn loopback_pair() -> Result<(Socket, TcpStream)> {
    let listener = Socket::new(Domain::IPV4, Type::STREAM, None)?;
    listener.set_recv_buffer_size(RECEIVE_BUFFER)?;
    listener.bind(&SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
    listener.listen(1)?;

    let sender = Socket::new(Domain::IPV4, Type::STREAM, None)?;
    sender.set_send_buffer_size(SEND_BUFFER)?;
    sender.connect_timeout(&listener.local_addr()?, WAIT)?;
    sender.set_nonblocking(true)?;

    let (receiver, _) = listener.accept()?;
    // Apply the limit to the accepted socket too so this test isolates the
    // queue reader from any platform difference in listener inheritance.
    receiver.set_recv_buffer_size(RECEIVE_BUFFER)?;
    receiver.set_nonblocking(true)?;
    Ok((sender, TcpStream::from_std(receiver.into())?))
}

#[tokio::test]
async fn native_send_queue_reports_a_blocked_sender_and_then_drain() -> Result<()> {
    let (sender, receiver) = loopback_pair()?;
    let peer = SockRef::from(&receiver);
    let buffers = format!(
        "sender SO_RCVBUF={:?} SO_SNDBUF={:?}, receiver SO_RCVBUF={:?} SO_SNDBUF={:?}",
        sender.recv_buffer_size(),
        sender.send_buffer_size(),
        peer.recv_buffer_size(),
        peer.send_buffer_size(),
    );
    let status = |written, read, before, after| {
        format!(
            "written={written} read={read} queue_before={before:?} queue_after={after:?}; {buffers}"
        )
    };

    let mut block = [42; 4096];
    let mut written = 0;
    let blocked = loop {
        match (&sender).write(&block) {
            Ok(bytes) => {
                written += bytes as u64;
                if bytes == 0 || written >= WRITE_LIMIT {
                    break false;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break true,
            Err(error) => {
                return Err(error)
                    .with_context(|| status(written, 0, queued(&sender, written), None));
            }
        }
    };
    let before = queued(&sender, written);
    ensure!(
        blocked,
        "writes must reach WouldBlock before the write limit; {}",
        status(written, 0, before, None)
    );
    ensure!(
        before.is_some_and(|bytes| bytes > 0),
        "a blocked TCP sender must have a nonempty native queue; {}",
        status(written, 0, before, None)
    );

    let mut receiver = receiver.take(READ_BYTES);
    let mut read = 0;
    let reading = timeout(WAIT, async {
        while read < READ_BYTES {
            let count = receiver.read(&mut block).await?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            read += count as u64;
        }
        Ok(())
    })
    .await;
    let mut after = queued(&sender, written);
    reading
        .context("the loopback receiver stalled")
        .and_then(|result| result.context("read the queued TCP data"))
        .with_context(|| status(written, read, before, after))?;

    // The receiver's read can finish before its window update or ACK arrives.
    let decreased = |after| {
        before
            .zip(after)
            .is_some_and(|(before, after)| after < before)
    };
    let deadline = Instant::now() + WAIT;
    while !decreased(after) && Instant::now() < deadline {
        sleep(Duration::from_millis(10)).await;
        after = queued(&sender, written);
    }
    ensure!(
        decreased(after),
        "the native queue must decrease after the receiver reads; {}",
        status(written, read, before, after)
    );
    println!("{}", status(written, read, before, after));
    Ok(())
}
