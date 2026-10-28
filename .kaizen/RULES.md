# ISUCON14 simulation R3: inherited contest harness

Source: official `isucon/isucon14` commit `53f8b627e040c30ebec600457c6c97da008b84b0`, `webapp/go`, `webapp/sql`, and `webapp/openapi.yaml`, preserved as the initial R3 application baseline.

References:
- https://github.com/isucon/isucon14/blob/main/docs/manual.md
- https://github.com/isucon/isucon14/blob/main/docs/ISURIDE.md
- https://github.com/isucon/isucon14/blob/main/README.md

## Rules retained

- Use only this practice's three c5.large application servers, each with gp3 20GB, for application processing and persistent state. The control and benchmark hosts must not serve application work or caches.
- Preserve the API and frontend behavior in the official specification and `openapi.yaml`: ride transitions, notification order, coupon order, fares, and payment correctness.
- `POST /api/initialize` must reset application data and any added state, store the supplied payment server URL, report `language: go`, and complete within 30 seconds.
- Run the official benchmark with its 60-second load phase and correctness/static checks. Do not bypass checks or count a failed run as an improvement. Payments outstanding at load completion must finish within five seconds.
- Notification updates must arrive within three seconds. Chair coordinates and accumulated movement may lag by at most three seconds; notification after a ride is matched may lag by at most 30 seconds as described in the official manual.
- Keep all three application servers bootable. Final verification includes rebooting all three, rerunning the benchmark on the same target, and checking persistence and frontend access. The reproduced score must exceed 75% of the final score.
- Do not change organizer envcheck files or administrative users if present, remove existing login keys, or obstruct reboot. Do not access unrelated resources or change instance types during measurement.
- The shared control ledger owns atomic task claims and environment reservations. A Discord announcement is a notification, not a lock. Deploy, benchmark, adoption, recovery, and final verification remain under one reservation.

## Recorded practice deviations

- This is an authorized past-problem practice with AI analysis and code generation, coordinated in the designated private Discord channel; it is not a live competition submission.
- Application resources match the official three c5.large instances with gp3 20GB each. The dedicated benchmark host is c5.2xlarge (8 vCPU, 16GB), whereas the original competition used Fargate (8 vCPU, 8GB).
- Benchmarks are launched over SSH instead of the retired competition portal. The benchmark's payment mock remains on the benchmark host and must be reachable from the application servers.
- The practice AMI uses a self-signed TLS certificate. Keep HTTPS and hostname `isuride.xiv.isucon.net`; record this certificate difference rather than disabling static or correctness checks.
- Scope is this new practice environment. Existing ISUCON practice repositories, stopped instances, and unfinished experiments are excluded.
