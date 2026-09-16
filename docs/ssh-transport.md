# SSH transport boundary

`go/internal/sshproxy` is the bounded process boundary for configured SSH
endpoints. It is intentionally separate from the app-server request pump and
from journal storage.

## App-server proxy

The production command shape is fixed:

```text
ssh -- <host> codex app-server proxy
```

The package passes this argv directly to `exec.Command`; it never invokes a
shell. OpenSSH therefore remains authoritative for `~/.ssh/config`, host-key
verification, the SSH agent, `ProxyJump`, and the user's configured policy.
Mektup owns neither credentials nor SSH policy. A configured host is validated
as one host token; the remote executable name is a fixed literal, not route
input.

The child's stdin/stdout are exposed as a `net.Conn`-shaped byte stream. The
bytes are the Codex app-server's raw HTTP Upgrade/WebSocket stream. There is no
JSONL framing, method inspection, path interpolation, or message-body argv
argument. The remote daemon must already exist; Mektup does not start or fall
back to an embedded daemon.

`Wait` and write errors retain separate evidence:

- `spawn`: the local child could not be created or started;
- `authentication`: OpenSSH's bounded stderr matched an authentication or
  host-verification failure;
- `proxy`: the remote proxy command exited with another failure;
- `eof`: the raw stream/child ended cleanly;
- `possible_write`: a write had begun before cancellation or process failure;
- `canceled`: context cancellation happened before a write.

The `Cause` field preserves the underlying classification when a
`possible_write` wrapper is required. Stderr is bounded and is not included in
`Failure.Error`, so ordinary diagnostics do not echo SSH configuration. A
caller must still redact it before putting it in an audit artifact.

Child cancellation closes the pipes, asks the process to terminate, and waits
only for the configured cleanup bound. A timed-out cleanup is explicit
`ErrCleanupTimeout`; it is never silently treated as a successful disconnect.
Deadline setters wake already-blocked reads and writes, and a short child-pipe
write with a nil error remains `possible_write` with `io.ErrShortWrite`.

## One-shot custody control

`InvokeControl` uses a different fixed command:

```text
ssh -- <host> mektup control receive
```

It sends one versioned `mektup/control/v1` metadata document on stdin. The
document may select only opaque pre-registered endpoint/store/message IDs and
digests. It may not contain a body, path, executable, shell fragment, or local
journal location. `ControlValidator` is the seam for the journal package to
check the pre-existing custody relationship; this transport package does not
invent or resolve journal internals.

The control response is bounded complete JSON. A missing/uncertain result is
not converted into an accepted custody transition. A response's app-server
daemon version is separate evidence from the remote Mektup/Codex executable's
binary version. This package does not run `codex --version` and cannot claim a
daemon version; the app-server initialize handshake remains authoritative.

The package has deterministic process seams covering exact argv, raw binary
bytes, cancellation/child cleanup, control stdin validation, and the
spawn/authentication/proxy/EOF/possible-write classes. Those tests do not
constitute live SSH acceptance.
