#!/bin/sh
# Installs a built gem with no Rust in reach, then loads its library and runs its daemon.
set -eu
gem install --local --no-document --ignore-dependencies "$1"
gem install --no-document ffi
dir="$(ruby -e 'print Gem::Specification.find_by_name("stampeded").full_gem_path')"
cd /tmp
ruby -I "$dir/lib" -e '
  require "stampeded/errors"
  require "stampeded/daemon"
  require "stampeded/native"
  puts "libstampeded_ffi #{Stampeded::Native.version} from #{Stampeded::Native.library_path}"
  system(Stampeded::Daemon.executable, "--help", out: File::NULL, exception: true)
  puts "daemon #{Stampeded::Daemon.executable} runs"
'
