#!/bin/bash
# POS feed dashboard.
#
# Bound to 127.0.0.1 ON PURPOSE. Streamlit defaults to 0.0.0.0, which puts
# every client's revenue on the LAN — and on the public IP if the router
# forwards the port — with no authentication at all. This data covers
# BrewDog, Bake My Day, Street Food and Smash Tag; it does not belong on an
# open port.
#
# To share it with colleagues, put it behind something that authenticates:
# Tailscale, an SSH tunnel, or Streamlit Community Cloud with a PRIVATE repo.
# Do not simply change the address below.
cd "$(dirname "$0")" || exit 1
exec .venv/bin/streamlit run dashboard.py \
  --server.address 127.0.0.1 \
  --server.port "${PORT:-8502}" \
  --server.headless true \
  --browser.gatherUsageStats false
