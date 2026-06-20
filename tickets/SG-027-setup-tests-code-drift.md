# SG-027: `setup` command tests drifted from code (Hailo "already complete" flow)

**Type:** test / bug
**Priority:** P2
**Area:** setup, tests
**Surfaced by:** CI enablement (SG-010) — these were never gated because no CI existed.

## Problem
Five `tests/test_setup.py` tests fail against the current `setup.py`:
`test_setup_clones_repo_if_not_exists`, `test_setup_skips_clone_if_exists`,
`test_setup_runs_install_script`, `test_setup_install_script_failure`,
`test_setup_verifies_hailo_apps`.

They patch `bugcam.commands.setup.check_import` to return `True` and expect the
clone/install/verify flow to run, but `_install_hailo_environment` (`setup.py:100`)
short-circuits with "Hailo setup already complete" when both imports are present —
so the asserted output (`"hailo_apps: OK"`, `"Found Hailo environment"`, install
script invocation) never happens. Test expectations and code have diverged.

## Resolution
**Quarantined with `@pytest.mark.xfail` (non-strict)** to unblock CI (per 2026-06-20
decision). Fix properly through the normal review gate: decide per test whether the
setup flow (`setup.py:94-121`) is correct or the tests are stale, then update one
side and remove the xfail.

## Acceptance criteria
- [ ] Each of the 5 tests is re-aligned with intended `setup` behaviour and the
      `xfail` markers removed.
