#!/bin/sh
# Builds the gem for one Linux platform inside a ruby:4.0 image, e.g.
#   docker run --rm -v "$PWD:/src" -w /src ruby:4.0-trixie tools/build_native_gem.sh x86_64-linux-gnu
set -eu
platform="$1"
upkg_installer="https://raw.githubusercontent.com/seuros/upkg/master/install.sh"

if ! command -v upkg >/dev/null 2>&1; then
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o /tmp/upkg-install.sh "$upkg_installer"
  else
    wget -qO /tmp/upkg-install.sh "$upkg_installer"
  fi
  sh /tmp/upkg-install.sh
fi

if [ -f /etc/alpine-release ]; then
  upkg install build-base clang-dev curl git
else
  upkg install build-essential clang libclang-dev curl git
fi

curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.99
. "$HOME/.cargo/env"

gem install rake --no-document
cd ruby
rake "gem:native[$platform]"
