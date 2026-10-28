# T-0014: Age-aware global nearest-ETA chair matching

- Owner/session: `shm11C3-codex-isu14-r3-20261029-01`; branch `shm11C3/T-0014-global-matching`.
- Hypothesis: FIFO-local nearest-chair assignment can consume a chair that is a much better ETA for a later, non-urgent ride. Matching satisfaction was only 21–39% in recent T8/T9 samples. Keep rides aged at least 2s on FIFO-nearest to protect the official `<100`-tick Matching criterion (world tick 30ms), then globally select minimum ETA edges from each remaining ride's top four free chairs. FIFO-nearest fallback assigns all remaining feasible rides.
- Base: accepted main `84773626160fb5abbd010a674fa66692a2bda301` (T7 fallback polling and T9 flush changes).
- Changed files: `go/internal_handlers.go`, `go/matching_selector.go`, and focused `go/matching_selector_test.go`. No SQL/schema/config/resource changes.
- Preserved contracts: active/free chair filter, nonpositive-speed skip, unique chair per selection batch, conditional `UPDATE ... WHERE chair_id IS NULL`, and SSE `notify` after a successful update.
- Expected effect is unproven. Improve aggregate approach ETA for younger rides while age protection limits Matching regressions. Not a global optimum; Manhattan ETA ignores congestion and status/notification delay. It may alter Dispatch/Pickup satisfaction; official same-condition benchmark decides score effect.
- Validation scope: focused urgent/global/fallback/unique/zero-speed/empty fixtures, `go build ./...`, and a 700-ride × 500-chair local selection microbenchmark. No full suite or official benchmark run by worker.
- Microbenchmark at prototype stage: top-4 ~3.2ms/op and ~83KB/op on Apple M4/darwin-arm64; target CPU and live queue size not measured. Full-edge sort was ~47.6ms/op and ~8.4MB/op on the same host.
- Stop/recovery: revert the candidate commit if the official checker fails or same-condition score/quality declines. The change is isolated to Go source and can be reverted without database or config cleanup.
