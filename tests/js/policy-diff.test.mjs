/* Exercises the real diff code by slicing it out of app.js — the functions are
   pure, so they run in node exactly as they do in the browser. */
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';

const src = readFileSync(new URL('../../orchestrator/ui/js/app.js', import.meta.url), 'utf8');
const start = src.indexOf('const POLICY_FIELDS');
const end = src.indexOf('/* --------------------------------------------------------- svg helpers');
assert.ok(start > 0 && end > start, 'could not slice the diff section out of app.js');
const { diffPolicies, policyValue } = await import(
  'data:text/javascript,' + encodeURIComponent(
    src.slice(start, end) + '\nexport { diffPolicies, policyValue };')
);

const repo = (over = {}) => ({
  github_installation: 'metaldevops-personal', default_mode: 'ephemeral',
  warm_capacity: 2, max_concurrent: 0, warm_idle_capacity: 1,
  warm_idle_after_seconds: 7200, profile: 'crossbuild-small',
  allowed_modes: ['ephemeral'], allowed_events: ['workflow_job'], required_labels: [], ...over,
});
const base = {
  capacity: { max_active: 11, pool_size: 2 },
  installations: { 'metaldevops-personal': { account: 'JustShinobi', installation_id: 146433491, scope: 'repository', runner_group_id: 1 } },
  repositories: { 'JustShinobi/NinjaCrawler': repo(), 'JustShinobi/dot-tutors-backend': repo({ warm_capacity: 1 }) },
};
const clone = (o) => JSON.parse(JSON.stringify(o));
let n = 0;
const check = (name, fn) => { fn(); n++; };

check('identical payloads produce no groups', () => {
  assert.deepEqual(diffPolicies(base, clone(base)), []);
});

check('a changed number is reported with both sides', () => {
  const after = clone(base); after.repositories['JustShinobi/NinjaCrawler'].warm_capacity = 4;
  const [g] = diffPolicies(base, after);
  assert.equal(g.group, 'Repositories');
  assert.equal(g.entries.length, 1, 'untouched repos must not appear');
  assert.equal(g.entries[0].state, 'changed');
  assert.deepEqual(g.entries[0].fields, [{ label: 'warm runners', from: '2', to: '4' }]);
});

check('capacity changes get their own group', () => {
  const after = clone(base); after.capacity.max_active = 12;
  const [g] = diffPolicies(base, after);
  assert.equal(g.group, 'Capacity');
  assert.deepEqual(g.entries[0].fields, [{ label: 'max active runners', from: '11', to: '12' }]);
});

check('an added repository lists its settings', () => {
  const after = clone(base); after.repositories['JustShinobi/new-repo'] = repo({ warm_capacity: 3 });
  const g = diffPolicies(base, after)[0];
  const added = g.entries.find((e) => e.name === 'JustShinobi/new-repo');
  assert.equal(added.state, 'added');
  assert.ok(added.fields.some((f) => f.label === 'warm runners' && f.to === '3'));
  // null, not "—": the template hides the before/arrow entirely for new rows.
  assert.ok(added.fields.every((f) => f.from === null), 'added rows must carry no before value');
});

check('a removed repository is as visible as an added one', () => {
  const after = clone(base); delete after.repositories['JustShinobi/dot-tutors-backend'];
  const g = diffPolicies(base, after)[0];
  assert.equal(g.entries.length, 1);
  assert.equal(g.entries[0].state, 'removed');
  assert.deepEqual(g.entries[0].fields, []);
});

check('array fields compare by content, not identity', () => {
  const same = clone(base);
  same.repositories['JustShinobi/NinjaCrawler'].allowed_modes = ['ephemeral'];
  assert.deepEqual(diffPolicies(base, same), [], 'equal arrays must not diff');
  const after = clone(base);
  after.repositories['JustShinobi/NinjaCrawler'].allowed_modes = ['ephemeral', 'warm'];
  const f = diffPolicies(base, after)[0].entries[0].fields[0];
  assert.deepEqual(f, { label: 'allowed modes', from: 'ephemeral', to: 'ephemeral, warm' });
});

check('sentinels and units are rendered for a human', () => {
  assert.equal(policyValue('warm_idle_capacity', -1), 'off');
  assert.equal(policyValue('warm_idle_capacity', 0), '0');
  assert.equal(policyValue('warm_idle_after_seconds', 7200), '120 min');
  assert.equal(policyValue('warm_idle_after_seconds', 0), '0 min');
  assert.equal(policyValue('allowed_events', []), 'none');
  assert.equal(policyValue('profile', ''), '—');
  // The regression that made an added row read "NaN min".
  assert.equal(policyValue('warm_idle_after_seconds', undefined), '—');
});

check('a null baseline reads as all-new rather than as no-change', () => {
  const groups = diffPolicies(null, base);
  assert.ok(groups.length >= 2);
  assert.ok(groups.flatMap((g) => g.entries).every((e) => e.state !== 'removed'));
});

console.log(`${n} diff assertions passed`);
