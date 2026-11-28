use hyper_util::client::legacy::connect::dns::{GaiResolver, Name};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tower_service::Service;

type Override = Box<dyn Fn(&Name) -> io::Result<SocketAddr> + Send + Sync>;

tokio::task_local! {
    pub(crate) static TEST_RESOLVE: Override;
}

#[derive(Clone)]
pub(in crate::s3::request::transport) struct Resolver(GaiResolver);

impl Resolver {
    pub(super) fn new() -> Self {
        Self(GaiResolver::new())
    }
}

type Addresses = Box<dyn Iterator<Item = SocketAddr> + Send>;

impl Service<Name> for Resolver {
    type Response = Addresses;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Addresses>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        if let Ok(address) = TEST_RESOLVE.try_with(|resolve| resolve(&name)) {
            return Box::pin(async move { Ok(Box::new(std::iter::once(address?)) as Addresses) });
        }
        let resolving = self.0.call(name);
        Box::pin(async move { Ok(Box::new(resolving.await?) as Addresses) })
    }
}
