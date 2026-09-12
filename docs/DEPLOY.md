# Deploy (us-east-2)

Develop and dry-run on your laptop first. Move the binary next to the sequencer only after `bodkin doctor --probe` and a dry-run `snipe` look right. Distance to `sequencer.mainnet.chain.robinhood.com` *is* queue position.

## Box

- AWS us-east-2 (Ohio). The sequencer’s three IPs are EC2 in that region.
- 2 vCPU / 4 GB is enough for the binary. A local nitro node later wants 8+ cores and 64 GB; skip that until public RPCs 429 you.
- Open no inbound ports. The board binds `127.0.0.1` only; if you want it remotely, SSH-tunnel `:4663`.

## Clock

Chrony against Amazon Time Sync. ~10 ms wall-clock error is enough for a 1 s tax step.

```
# /etc/chrony/chrony.conf (Amazon Linux / Ubuntu on AWS)
server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4
makestep 1.0 3
rtcsync
```

`systemctl restart chrony` (or `chronyd`). `chronyc tracking` should show the AWS hop.

## Binary

On the box:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
git clone https://github.com/Phosphenq/bodkin && cd bodkin
cp .env.example .env
# set PRIVATE_KEY / HELPER_ADDRESS / RPC_URL only in .env
cargo build --release
./target/release/bodkin doctor --probe
```

Or copy `target/release/bodkin` from a matching GNU/Linux build. The process cwd should be the checkout so `.env` and `data/` resolve.

## systemd

```
# /etc/systemd/system/bodkin.service
[Unit]
Description=bodkin
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=bodkin
WorkingDirectory=/home/bodkin/bodkin
Environment=RUST_LOG=info
ExecStart=/home/bodkin/bodkin/target/release/bodkin snipe --live --yes
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
```

`--yes` skips the `arm` prompt. The service should only exist after you have armed a session by hand once.

For the board, SSH-tunnel instead of publishing the port:

```sh
ssh -N -L 4663:127.0.0.1:4663 box
```

## After boot

1. `chronyc tracking`
2. `bodkin doctor --probe` — per-IP sequencer RTT, clock offset, helper bytecode, Conditional reject code
3. `bodkin helper check` if `HELPER_ADDRESS` is set
4. Dry-run `snipe --for 3600` and read `data/outcomes.jsonl` before `--live`
