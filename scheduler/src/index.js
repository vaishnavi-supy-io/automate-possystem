/**
 * POS pipeline scheduler.
 *
 * WHY THIS EXISTS
 * ---------------
 * GitHub's `schedule:` trigger is best-effort and was firing these pipelines
 * 4h10m-5h19m late, every day, landing reports at ~13:00 Dubai instead of
 * ~08:00. Measured 2026-09-20: eight workflows scheduled 03:37-06:41 UTC all
 * started 08:16-09:39 UTC. Moving the crons off the hour changed nothing --
 * they still drained in one late batch, which is the tell that the offset is
 * not being honoured at all.
 *
 * The same workflows dispatched through the REST API started within SECONDS.
 * So the clock moves here and GitHub only ever receives an explicit dispatch.
 *
 * ONE cron trigger ticks every 5 minutes and dispatches whatever is due, so
 * adding a pipeline is a line in SCHEDULE rather than another trigger (a
 * Worker is capped at a handful of cron triggers; ticks are effectively free).
 * Every job time below must therefore be a multiple of 5 minutes.
 */

const OWNER = "vaishnavi-supy-io";
const REPO = "automate-possystem";

// UTC HH:MM -> workflow file. Order and spacing are deliberate:
//   * the four portal pipelines run first, spread out so one client's slow
//     run never delays another's,
//   * SFTP runs late because BrewDog's ~22 store files land 03:06-04:35 UTC
//     (measured) -- pulling earlier reads a half-uploaded day,
//   * the digest runs last: it reports on everything above, so it has to see
//     the finished state.
const SCHEDULE = {
  "03:35": "sapapad_daily.yml",
  "03:45": "sapaad_tenants_daily.yml",
  "03:50": "symphony_daily.yml",
  "03:55": "talabat_daily.yml",
  "04:05": "dines_daily.yml",
  "04:15": "streetfood_daily.yml",
  "05:30": "sftp_daily.yml",
  "06:40": "delivery_digest.yml",
};

async function dispatch(workflow, token) {
  const res = await fetch(
    `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${workflow}/dispatches`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        // GitHub rejects API calls without a User-Agent.
        "User-Agent": `${OWNER}-pos-scheduler`,
      },
      body: JSON.stringify({ ref: "main" }),
    },
  );
  // 204 No Content is success. Anything else is worth shouting about, because
  // a silent dispatch failure looks exactly like a client that had no sales.
  if (res.status !== 204) {
    throw new Error(`${workflow}: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
  }
}

async function notify(webhook, text) {
  if (!webhook) return;
  try {
    await fetch(webhook, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
  } catch {
    // A failed alert must never mask the dispatch result.
  }
}

export default {
  async scheduled(event, env, ctx) {
    const now = new Date(event.scheduledTime);
    const hhmm = `${String(now.getUTCHours()).padStart(2, "0")}:${String(
      now.getUTCMinutes(),
    ).padStart(2, "0")}`;

    const workflow = SCHEDULE[hhmm];
    if (!workflow) return; // nothing due this tick -- the common case

    const token = env.GH_TOKEN;
    if (!token) {
      await notify(env.SLACK_WEBHOOK_URL, `POS scheduler: GH_TOKEN is not set — ${workflow} not dispatched`);
      return;
    }

    // One retry. Dispatch is a single API call, so a failure is almost always
    // a transient 5xx; a pipeline missed here is a client's day of sales
    // missed, and nothing downstream would re-attempt it.
    try {
      await dispatch(workflow, token);
      console.log(`${hhmm} dispatched ${workflow}`);
    } catch (err) {
      console.error(`${hhmm} ${err.message} — retrying once`);
      try {
        await new Promise((r) => setTimeout(r, 5000));
        await dispatch(workflow, token);
        console.log(`${hhmm} dispatched ${workflow} on retry`);
      } catch (err2) {
        console.error(`${hhmm} FAILED ${err2.message}`);
        await notify(
          env.SLACK_WEBHOOK_URL,
          `:rotating_light: POS scheduler could not dispatch *${workflow}* at ${hhmm} UTC — ${err2.message}. That client will have no report today unless it is run by hand.`,
        );
      }
    }
  },
};
