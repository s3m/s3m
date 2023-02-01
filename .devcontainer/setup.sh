apt update
apt install -y \
  curl \
  git \
  gnupg2 \
  jq \
  sudo \
  zsh \
  vim \
  build-essential \
  openssl

# Install rustup and common components
curl https://sh.rustup.rs -sSf | sh -s -- -y
rustup component add rustfmt
rustup component add clippy

cargo install cargo-expand
cargo install cargo-edit
