#!/bin/sh
# Builds the gem for one Linux platform inside a ruby:4.0 image, e.g.
#   docker run --rm -v "$PWD:/src" -w /src ruby:4.0-trixie tools/build_native_gem.sh x86_64-linux-gnu
set -eu
platform="$1"
upkg_setup="https://raw.githubusercontent.com/seuros/setup-upkg/master/setup.sh"

# setup.sh resolves the release with GITHUB_API_TOKEN when set; empty RUNNER_TEMP links upkg into /usr/local/bin.
if ! command -v upkg >/dev/null 2>&1; then
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o /tmp/setup-upkg.sh "$upkg_setup"
  else
    wget -qO /tmp/setup-upkg.sh "$upkg_setup"
  fi
  RUNNER_TEMP='' INPUT_UPDATE=true sh /tmp/setup-upkg.sh
fi

if [ -f /etc/alpine-release ]; then
  upkg install build-base curl git
else
  upkg install build-essential curl git
fi

curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.99
. "$HOME/.cargo/env"

gem install rake --no-document
cd ruby
rake "gem:native[$platform]"
