# Bug: `HTTPConfig(bearer_token=...)` hangs hf:// reads indefinitely

Found 2026-07-06 while benchmarking on `BitRobot/HIW-500-LeRobot` (branch
`yk/lerobot-brightest-frame`, base `yk/lerobot-batch-decode`). Not related to
the lerobot reader - reproduces with a bare `daft.open_file`.

## Repro

```python
import daft

io_config = daft.io.IOConfig(http=daft.io.HTTPConfig(bearer_token="hf_..."))  # valid token
with daft.open_file("hf://datasets/BitRobot/HIW-500-LeRobot/meta/info.json", io_config=io_config) as f:
    f.read(200)  # never returns
```

- Anonymous (`io_config=None`): returns in 0.3s.
- `IOConfig(hf=HuggingFaceConfig(token=...))` (same token): returns in 0.5s.
- `IOConfig(http=HTTPConfig(bearer_token=...))`: no return within 60s
  (SIGALRM test); a full pipeline sat blocked for 32 minutes before being
  killed. No error, no warning, no output.

The same token works via plain HTTP (`curl -H "Authorization: Bearer ..."`
against both the `/api/whoami-v2` and `resolve/` endpoints), so the token and
network are fine.

## Where it blocks

Sampling the hung process (`sample <pid>`) shows the main thread inside
`daft.abi3.so` blocked on `oneshot::Receiver::recv` /
`_dispatch_semaphore_wait_slow`, waiting on a tokio blocking-pool task that
never completes - i.e. the async HTTP request inside the hf source never
resolves and there is no timeout.

## Notes

- `src/daft-io/src/huggingface.rs:319` has a deprecation warning for exactly
  this configuration ("use `HuggingFaceConfig` instead, removed in v0.6"),
  but in this repro no warning surfaced before the hang.
- Expected behavior: either honor the token (as before deprecation) or fail
  fast with the deprecation message - anything but a silent infinite hang.

## Workaround

Use `daft.io.IOConfig(hf=daft.io.HuggingFaceConfig(token=...))` for hf://
paths.
