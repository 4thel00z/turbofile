# turbofile

## The README's benchmark table and turbofile.dev must match

The landing page at https://turbofile.dev shows the same `vs aiofiles`
multipliers as the README's benchmark table. Any change that touches that
table (a perf PR that moves a row, a new `make bench` run recorded in the
README) updates the site in the same piece of work, before the change is
reported as done.

1. The site is not in this repo. Its source lives on the Hetzner box under
   `/home/httpserver/private/turbofile.dev` (ssh alias `hetzner`; read and
   write there with `sudo -n -u httpserver`).
2. The multipliers are hardcoded in `template.html`, in the terminal block
   inside `bench-card`: one `<div class="line">` per README row, the number
   in `<span class="num">`. Mirror the README table row for row, same labels
   and same numbers. Current numbers only, no before/after values, the same
   rule the README follows.
3. Back up the current template on the box, copy the edited `template.html`
   back (`sudo -n -u httpserver tee <path>` over ssh), then run
   `sudo -n -u httpserver python3 /home/httpserver/private/turbofile.dev/render.py`
   by absolute path: the login user cannot `cd` into that directory, and the
   script finds its own files from its location. It writes `www/index.html`,
   which Caddy serves statically; nothing restarts. On any error it aborts
   and keeps the old page.
4. Fetch the live page from the box (`curl -s https://turbofile.dev`) and
   confirm the new numbers are there.

The copy around the table comes from `sections.json`, overridden by Ghost
pages of the same slug; it carries no numbers, so a table update never needs
the Ghost admin.
