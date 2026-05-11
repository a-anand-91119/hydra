"""Tests for hydra.planner — pure functions, no network or DB needed."""

from __future__ import annotations

from collections import Counter

from hydra import planner
from hydra.config import Config, Defaults, HostSpec
from hydra.journal import JournalMirror, JournalRepo, PrimaryRepoSnapshot, ScanDiff
from hydra.providers.base import PrimaryMirror, PrimaryProject
from hydra.wizard import CreateOptions


def _cfg() -> Config:
    return Config(
        hosts=[
            HostSpec(id="primary", kind="gitlab", url="https://primary.gl"),
            HostSpec(id="gh", kind="github", url="https://api.github.com"),
            HostSpec(id="cloud", kind="gitlab", url="https://gitlab.com"),
        ],
        primary="primary",
        forks=["gh", "cloud"],
        defaults=Defaults(private=True, group=""),
    )


class TestPlanCreate:
    def test_action_count_and_order_for_two_forks(self):
        cfg = _cfg()
        opts = CreateOptions(name="probe", description="d", group="t", is_private=True, mirror=True)
        plan = planner.plan_create(cfg, opts)
        kinds = [a.kind for a in plan.actions]
        # primary: ensure_namespace, create_repo, journal_record_repo
        # then per fork: ensure_namespace, create_repo, add_outbound_mirror, journal_record_mirror
        assert kinds == [
            "ensure_namespace",
            "create_repo",
            "journal_record_repo",
            "ensure_namespace",
            "create_repo",
            "add_outbound_mirror",
            "journal_record_mirror",
            "ensure_namespace",
            "create_repo",
            "add_outbound_mirror",
            "journal_record_mirror",
        ]

    def test_no_mirror_skips_mirror_actions(self):
        cfg = _cfg()
        opts = CreateOptions(name="probe", description="", group="", is_private=True, mirror=False)
        plan = planner.plan_create(cfg, opts)
        counts = plan.summary_counts()
        assert counts.get("add_outbound_mirror", 0) == 0
        assert counts.get("journal_record_mirror", 0) == 0
        # Still creates 1 primary + 2 forks + namespaces + journal entry.
        assert counts["create_repo"] == 3
        assert counts["ensure_namespace"] == 3

    def test_mirror_actions_target_primary_host(self):
        cfg = _cfg()
        opts = CreateOptions(name="probe", description="", group="", is_private=True, mirror=True)
        plan = planner.plan_create(cfg, opts)
        for action in plan.actions:
            if action.kind == "add_outbound_mirror":
                assert action.host_id == "primary"


class TestPlanScanApply:
    def test_unknown_emits_record_repo_plus_per_mirror(self):
        cfg = _cfg()
        proj = PrimaryProject(
            project_id=42,
            web_url="https://primary.gl/g/probe",
            name="probe",
            full_path="g/probe",
            mirrors=[
                PrimaryMirror(id=1, url="https://gitlab.com/g/probe.git"),
                PrimaryMirror(id=2, url="https://github.com/me/probe.git"),
            ],
        )
        diff = ScanDiff(
            unknown=[
                PrimaryRepoSnapshot(
                    repo_id=42,
                    repo_url=proj.web_url,
                    name=proj.name,
                    mirror_push_ids=[1, 2],
                )
            ]
        )
        plan = planner.plan_scan_apply(diff, cfg, by_repo_id={42: proj})
        kinds = Counter(a.kind for a in plan.actions)
        assert kinds["journal_record_repo"] == 1
        assert kinds["journal_record_mirror"] == 2

    def test_unknown_skips_mirrors_with_no_matching_fork(self):
        cfg = _cfg()
        proj = PrimaryProject(
            project_id=42,
            web_url="u",
            name="probe",
            full_path="g/probe",
            mirrors=[PrimaryMirror(id=1, url="https://example.org/random.git")],
        )
        diff = ScanDiff(
            unknown=[
                PrimaryRepoSnapshot(repo_id=42, repo_url="u", name="probe", mirror_push_ids=[1])
            ]
        )
        plan = planner.plan_scan_apply(diff, cfg, by_repo_id={42: proj})
        kinds = Counter(a.kind for a in plan.actions)
        assert kinds == Counter({"journal_record_repo": 1})

    def test_drift_emits_update_push_id_only_for_changed_ids(self):
        cfg = _cfg()
        jrepo = JournalRepo(
            id=10,
            name="probe",
            primary_host_id="primary",
            primary_repo_id=42,
            primary_repo_url="u",
            created_at="t",
            mirrors=[
                JournalMirror(
                    id=100,
                    repo_id=10,
                    target_host_id="cloud",
                    target_repo_id=None,
                    target_repo_url="u",
                    push_mirror_id=1,  # matches primary → no action
                ),
                JournalMirror(
                    id=101,
                    repo_id=10,
                    target_host_id="gh",
                    target_repo_id=None,
                    target_repo_url="u",
                    push_mirror_id=99,  # primary now has 2 → should update
                ),
            ],
        )
        proj = PrimaryProject(
            project_id=42,
            web_url="u",
            name="probe",
            full_path="g/probe",
            mirrors=[
                PrimaryMirror(id=1, url="https://gitlab.com/g/probe.git"),
                PrimaryMirror(id=2, url="https://github.com/me/probe.git"),
            ],
        )
        snap = PrimaryRepoSnapshot(repo_id=42, repo_url="u", name="probe", mirror_push_ids=[1, 2])
        diff = ScanDiff(drift=[(jrepo, snap)])
        plan = planner.plan_scan_apply(diff, cfg, by_repo_id={42: proj})
        assert [a.kind for a in plan.actions] == ["journal_update_push_id"]
        only = plan.actions[0]
        assert only.payload["mirror_db_id"] == 101
        assert only.payload["new_push_mirror_id"] == 2

    def test_accept_unknown_ids_filters_plan(self):
        cfg = _cfg()
        p1 = PrimaryProject(
            project_id=1,
            web_url="u1",
            name="a",
            full_path="g/a",
            mirrors=[PrimaryMirror(id=10, url="https://gitlab.com/g/a.git")],
        )
        p2 = PrimaryProject(
            project_id=2,
            web_url="u2",
            name="b",
            full_path="g/b",
            mirrors=[PrimaryMirror(id=20, url="https://gitlab.com/g/b.git")],
        )
        diff = ScanDiff(
            unknown=[
                PrimaryRepoSnapshot(repo_id=1, repo_url="u1", name="a", mirror_push_ids=[10]),
                PrimaryRepoSnapshot(repo_id=2, repo_url="u2", name="b", mirror_push_ids=[20]),
            ]
        )
        plan = planner.plan_scan_apply(diff, cfg, by_repo_id={1: p1, 2: p2}, accept_unknown_ids={2})
        # Only project 2's actions survive.
        names = [a.summary for a in plan.actions]
        assert all("'b'" in n or "→ cloud" in n or "b →" in n for n in names)
        assert not any("'a'" in n for n in names)


class TestPlanShape:
    def test_is_empty(self):
        p = planner.Plan()
        assert p.is_empty
        p2 = planner.Plan(actions=[planner.Action("ensure_namespace", "h", "s")])
        assert not p2.is_empty

    def test_summary_counts(self):
        p = planner.Plan(
            actions=[
                planner.Action("create_repo", "h1", "x"),
                planner.Action("create_repo", "h2", "y"),
                planner.Action("add_outbound_mirror", "h1", "z"),
            ]
        )
        assert p.summary_counts() == {"create_repo": 2, "add_outbound_mirror": 1}
