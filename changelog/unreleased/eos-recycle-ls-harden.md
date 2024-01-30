Bugfix:  Harden parsing of recycle ls -m

EOS 5.2.0 added new keys to the outpuot of this command which broke the parsing.
This PR fixes it in a poor man approach.

https://github.com/cs3org/reva/pull/4486
