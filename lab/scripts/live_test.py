#!/usr/bin/env python3
"""Exercise the actual Kubernetes API, Temporal service, and provider processes."""
import datetime
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError

import lab

EVIDENCE = lab.LOCAL / "live"
RESULTS = []


def resource(name):
    result = lab.kubectl("get", "gpucluster", name, "-o", "json", "--ignore-not-found", capture=True)
    return json.loads(result.stdout) if result.stdout.strip() else None


def create(name, *, count=2, gpu="mock-a100", parallel=2):
    definition = {"apiVersion": "infrastructure.tutorial.example.com/v1alpha1", "kind": "GPUCluster",
                  "metadata": {"name": name, "namespace": lab.NAMESPACE},
                  "spec": {"nodeCount": count, "imageVersion": "os-v1", "gpuClass": gpu, "maxParallel": parallel}}
    result = lab.kubectl("create", "-f", "-", "-o", "json", data=json.dumps(definition), capture=True)
    return json.loads(result.stdout)["metadata"]["uid"]


def phase(name, desired="Ready", timeout=120):
    def probe():
        obj = resource(name)
        if not obj:
            return False
        status = obj.get("status", {})
        if status.get("phase") == "Failed" and desired != "Failed":
            raise RuntimeError(f"{name} failed: {status.get('lastError')}")
        if status.get("phase") != desired:
            return False
        if desired == "Ready":
            ready = next((c for c in status.get("conditions", []) if c["type"] == "Ready"), {})
            if ready.get("status") != "True" or status.get("readyHosts", 0) != obj["spec"]["nodeCount"]:
                return False
        return obj
    return lab.wait_for(probe, timeout=timeout, description=f"{name} phase {desired}")


def owner_inventory(uid):
    inventory = lab.http("/v1/inventory")
    hosts = [h for h in inventory.get("hosts", []) if h["ownerUID"] == uid]
    jobs = [j for j in inventory.get("jobs", []) if j["request"].get("OwnerUID", j["request"].get("ownerUID")) == uid]
    return hosts, jobs, inventory


def ready_inventory(uid, count):
    hosts, jobs, inventory = owner_inventory(uid)
    assert len(hosts) == count, (uid, hosts)
    assert len(jobs) == count, (uid, jobs)
    assert len({h["id"] for h in hosts}) == count
    assert len({j["id"] for j in jobs}) == count
    assert all(h["joined"] and h["validated"] for h in hosts), hosts
    assert all(j["done"] for j in jobs), jobs
    return hosts, jobs, inventory


def record(case, obj=None, **details):
    value = {"case": case, "passed": True, **details}
    if obj:
        value.update(name=obj["metadata"]["name"], uid=obj["metadata"]["uid"], status=obj["status"])
    RESULTS.append(value)
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / "results.json").write_text(json.dumps(RESULTS, indent=2))
    print(f"PASS {case}", flush=True)


def delete(name, uid):
    lab.kubectl("delete", "gpucluster", name, "--wait=false", capture=True)
    lab.wait_for(lambda: resource(name) is None, timeout=100, description=f"finalized deletion of {name}")
    hosts, _, inventory = owner_inventory(uid)
    assert not hosts, hosts
    assert uid in inventory.get("closedOwners", []), inventory


def main():
    # Build/start dependencies first; no mock Temporal client or API server.
    lab.up()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%m%d%H%M%S")
    lab.http("/v1/faults", {"imageDelayMillis": 1000, "failValidation": False,
                            "loseAllocationResponseOnce": False, "loseImageResponseOnce": False})

    # Keep one normal Ready resource for the learner to inspect afterwards.
    lab.kubectl("apply", "-f", lab.LAB / "config/live/gpucluster.yaml", capture=True)
    demo = phase("demo-gpu")
    assert demo["status"]["readyHosts"] == 3, demo
    ready_inventory(demo["metadata"]["uid"], 3)
    record("happy path on real servers", demo)

    # Force lost responses only for this run's next resource.
    before = lab.http("/v1/inventory")["counters"]
    lab.http("/v1/faults", {"loseAllocationResponseOnce": True, "loseImageResponseOnce": True})
    lost_name = "e2e-lost-" + stamp
    lost_uid = create(lost_name)
    lost = phase(lost_name)
    _, jobs, after = ready_inventory(lost_uid, 2)
    assert after["counters"]["lostAllocationResponses"] == before["lostAllocationResponses"] + 1
    assert after["counters"]["lostImageResponses"] == before["lostImageResponses"] + 1
    assert after["counters"]["hostsCreated"] == before["hostsCreated"] + 2
    assert after["counters"]["imagesCreated"] == before["imagesCreated"] + 2
    record("lost responses preserve exactly two hosts and jobs", lost, jobIDs=[j["id"] for j in jobs])

    # Reconcile after total status loss must observe the completed execution.
    run_id = lost["status"]["runID"]
    lab.kubectl("patch", "gpucluster", lost_name, "--subresource=status", "--type=json", "-p",
                '[{"op":"replace","path":"/status","value":{}}]', capture=True)
    recovered = phase(lost_name)
    assert recovered["status"]["runID"] == run_id, recovered
    ready_inventory(lost_uid, 2)
    assert lab.http("/v1/inventory")["counters"] == after["counters"]
    record("lost Kubernetes status recovers the same completed workflow", recovered)

    # Reject mutable intent before it can create a second workflow.
    patch = lab.kubectl("patch", "gpucluster", lost_name, "--type=merge", "-p",
                        '{"spec":{"nodeCount":3}}', capture=True, check=False)
    assert patch.returncode != 0 and "spec changes" in patch.stderr, patch
    record("Kubernetes rejects spec changes during the lab lifecycle", recovered)

    # A real provider process restart must preserve ownership and job identity.
    old_ids = sorted(j["id"] for j in jobs)
    lab.restart("provider", kill=True)
    lab.wait_for(lambda: lab.http("/v1/inventory"), description="restarted provider")
    _, reopened_jobs, reopened = ready_inventory(lost_uid, 2)
    assert sorted(j["id"] for j in reopened_jobs) == old_ids
    assert reopened["counters"] == after["counters"]
    record("provider process restart preserves durable inventory", recovered)

    # Kill the OS worker process during imaging. The new process has no workflow cache.
    lab.http("/v1/faults", {"imageDelayMillis": 15000})
    restart_name = "e2e-restart-" + stamp
    restart_uid = create(restart_name)
    initial = lab.wait_for(lambda: owner_inventory(restart_uid)[1], description="accepted image job before crash")
    assert any(not j["done"] for j in initial)
    previous_pid = lab.state()["worker"]["pid"]
    lab.restart("worker", kill=True)
    assert lab.state()["worker"]["pid"] != previous_pid
    restarted = phase(restart_name, timeout=150)
    _, restarted_jobs, _ = ready_inventory(restart_uid, 2)
    assert {j["id"] for j in initial}.issubset({j["id"] for j in restarted_jobs})
    record("worker process crash resumes existing image jobs", restarted,
           oldWorkerPID=previous_pid, newWorkerPID=lab.state()["worker"]["pid"],
           originalJobIDs=[j["id"] for j in initial], jobIDs=[j["id"] for j in restarted_jobs])

    # Download actual history and replay with no activity registration/provider I/O.
    history = EVIDENCE / f"{restart_name}.json"
    counts_before_replay = lab.http("/v1/inventory")["counters"]
    lab.run([lab.LAB / "bin/replay", "--workflow-id", restarted["status"]["workflowID"],
             "--run-id", restarted["status"]["runID"], "--out", history])
    assert lab.http("/v1/inventory")["counters"] == counts_before_replay
    record("actual Temporal history replays without provider side effects", restarted, history=str(history))

    # Unsupported GPU class is terminal, with explicit workflow compensation.
    lab.http("/v1/faults", {"imageDelayMillis": 0})
    failed_name = "e2e-failed-" + stamp
    failed_uid = create(failed_name, gpu="unsupported-gpu", parallel=1)
    failed = phase(failed_name, "Failed")
    assert "unsupported" in failed["status"]["lastError"].lower(), failed
    assert not owner_inventory(failed_uid)[0]
    assert failed_uid in owner_inventory(failed_uid)[2]["closedOwners"]
    record("terminal GPU failure surfaces status and compensates", failed)

    # Close/fence the UID while imaging is still accepted and pending.
    lab.http("/v1/faults", {"imageDelayMillis": 30000})
    deleting_name = "e2e-deleting-" + stamp
    deleting_uid = create(deleting_name, count=1)
    accepted = lab.wait_for(lambda: owner_inventory(deleting_uid)[1], description="image before deletion")
    assert not accepted[0]["done"]
    delete(deleting_name, deleting_uid)
    # Delayed allocations after finalization must be rejected at the provider.
    try:
        lab.http("/v1/hosts/ensure", {"ownerUID": deleting_uid, "slot": 99})
    except HTTPError as error:
        assert error.code == 410 and json.loads(error.read())["code"] == "owner_released"
    else:
        raise AssertionError("closed UID accepted a delayed allocation")
    record("deletion during imaging fences late work and finalizes", uid=deleting_uid)

    # With no worker available, fencing must still happen, while finalization waits.
    offline_name = "e2e-offline-delete-" + stamp
    offline_uid = create(offline_name, count=1)
    lab.wait_for(lambda: owner_inventory(offline_uid)[1], description="image before stopped-worker deletion")
    lab.stop("worker", kill=True)
    lab.kubectl("delete", "gpucluster", offline_name, "--wait=false", capture=True)
    lab.wait_for(lambda: offline_uid in owner_inventory(offline_uid)[2]["closedOwners"],
                 description="owner fenced without a worker")
    assert not owner_inventory(offline_uid)[0]
    pending_deletion = resource(offline_name)
    assert pending_deletion and pending_deletion["metadata"].get("deletionTimestamp"), pending_deletion
    assert "infrastructure.tutorial.example.com/host-cleanup" in pending_deletion["metadata"].get("finalizers", [])
    time.sleep(1.2)  # Allow another controller pass; cancellation alone is not terminal.
    assert resource(offline_name) is not None, "finalizer removed before stopped worker acknowledged cancellation"
    lab.start("worker", lab.command("worker"))
    lab.wait_for(lambda: resource(offline_name) is None, timeout=100, description="finalization after worker restart")
    assert not owner_inventory(offline_uid)[0]
    record("stopped-worker deletion fences immediately and retains finalizer until restart", uid=offline_uid)

    # Exercise actual Temporal cancellation delivery, without controller fencing first.
    cancel_name = "e2e-cancel-" + stamp
    cancel_uid = create(cancel_name, count=1)
    lab.wait_for(lambda: owner_inventory(cancel_uid)[1], description="image before Temporal cancellation")
    cancel_running = resource(cancel_name)
    lab.run(["temporal", "workflow", "cancel", "--address", "127.0.0.1:7233", "--namespace", "default",
             "--workflow-id", cancel_running["status"]["workflowID"]], capture=True)
    canceled = phase(cancel_name, "Failed", timeout=100)
    assert not owner_inventory(cancel_uid)[0]
    cancel_history = EVIDENCE / f"{cancel_name}.json"
    lab.run([lab.LAB / "bin/replay", "--workflow-id", canceled["status"]["workflowID"],
             "--run-id", canceled["status"]["runID"], "--out", cancel_history])
    events = json.loads(cancel_history.read_text())["events"]
    image_cancellations = [int(e["eventId"]) for e in events if "activityTaskCanceledEventAttributes" in e]
    releases = [int(e["eventId"]) for e in events
                if e.get("activityTaskScheduledEventAttributes", {}).get("activityType", {}).get("name") == "ReleaseHosts"]
    assert image_cancellations and releases and min(releases) > max(image_cancellations), events
    record("real activity cancellation acknowledgment precedes workflow compensation", canceled,
           history=str(cancel_history), canceledActivityEventIDs=image_cancellations, releaseScheduledEventIDs=releases)

    for name, uid in [(lost_name, lost_uid), (restart_name, restart_uid), (failed_name, failed_uid), (cancel_name, cancel_uid)]:
        delete(name, uid)
    lab.http("/v1/faults", {"imageDelayMillis": 6000})
    record("cleanup removes all test allocations; Ready demo remains", phase("demo-gpu"))
    print(f"All {len(RESULTS)} live checks passed. Evidence: {EVIDENCE / 'results.json'}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"LIVE TEST FAILED: {error}", file=sys.stderr)
        print(f"Inspect logs in {lab.RUNTIME}; partial evidence in {EVIDENCE}", file=sys.stderr)
        raise
