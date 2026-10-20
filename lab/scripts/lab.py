#!/usr/bin/env python3
"""Local process controls. All Kubernetes commands use the dedicated context."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
LAB = ROOT / "lab"
LOCAL = ROOT / ".local"
RUNTIME = LOCAL / "runtime"
KUBECONFIG = LOCAL / "kubeconfig"
CONTEXT = "kind-infrastructure-tutorial"
NAMESPACE = "infrastructure-tutorial"
ENV = dict(os.environ, PATH="/opt/homebrew/bin:/usr/local/bin:" + os.environ.get("PATH", ""),
           DEVELOPER_DIR="/Library/Developer/CommandLineTools",
           KIND_EXPERIMENTAL_PROVIDER="podman")
CHILDREN = {}  # Handles owned by this invocation, so stopped children can be reaped.


def run(args, *, capture=False, check=True, data=None):
    return subprocess.run([str(a) for a in args], cwd=LAB, env=ENV, text=True,
                          input=data, capture_output=capture, check=check)


def kubectl(*args, **kwargs):
    return run(["kubectl", "--kubeconfig", KUBECONFIG, "--context", CONTEXT,
                "-n", NAMESPACE, *args], **kwargs)


def wait_for(probe, timeout=30, description="service"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = probe()
            if value:
                return value
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            last = error
        time.sleep(0.4)
    raise RuntimeError(f"Timed out waiting for {description}: {last}")


def http(path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request("http://127.0.0.1:9080" + path, data=data,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        raw = response.read()
        return json.loads(raw) if raw else {}


def state():
    try:
        return json.loads((RUNTIME / "processes.json").read_text())
    except FileNotFoundError:
        return {}


def save_state(value):
    RUNTIME.mkdir(parents=True, exist_ok=True)
    temporary = RUNTIME / "processes.tmp"
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(RUNTIME / "processes.json")


def alive(entry):
    # Validate the command as well as PID; never signal a reused unrelated PID.
    result = run(["ps", "-ww", "-p", str(entry["pid"]), "-o", "command="], capture=True, check=False)
    return result.returncode == 0 and result.stdout.strip() == " ".join(entry["command"])


def start(name, command):
    command = [str(a) for a in command]
    executable = Path(command[0])
    modified = executable.stat().st_mtime_ns if executable.exists() else None
    current = state()
    if name in current and alive(current[name]):
        if current[name]["command"] == command and current[name].get("binaryMtime") == modified:
            return current[name]["pid"]
        stop(name)
        current = state()
    RUNTIME.mkdir(parents=True, exist_ok=True)
    with (RUNTIME / f"{name}.log").open("ab") as log:
        child = subprocess.Popen([str(a) for a in command], cwd=LAB, env=ENV,
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                 start_new_session=True)
    CHILDREN[name] = child
    current[name] = {"pid": child.pid, "command": command, "binaryMtime": modified}
    save_state(current)
    return child.pid


def stop(name, *, kill=False):
    current = state()
    entry = current.get(name)
    if entry and alive(entry):
        os.kill(entry["pid"], signal.SIGKILL if kill else signal.SIGTERM)
        deadline = time.monotonic() + 8
        while alive(entry) and time.monotonic() < deadline:
            time.sleep(0.2)
        if alive(entry):
            os.kill(entry["pid"], signal.SIGKILL)
    child = CHILDREN.pop(name, None)
    if child is not None and (not entry or not alive(entry)):
        child.wait(timeout=5)
    current.pop(name, None)
    save_state(current)


def command(name):
    binary = LAB / "bin" / name
    if name == "provider":
        return [binary, "--state", LOCAL / "provider" / "state.json", "--image-delay", "6s"]
    if name == "controller":
        return [binary, "--kubeconfig", KUBECONFIG, "--context", CONTEXT]
    if name == "worker":
        return [binary]
    if name == "web":
        return ["/opt/homebrew/bin/kubectl", "--kubeconfig", KUBECONFIG, "--context", CONTEXT,
                "-n", NAMESPACE, "port-forward", "service/tutorial-web", "8088:80", "--address", "127.0.0.1"]
    raise ValueError(name)


def restart(name, *, kill=False):
    stop(name, kill=kill)
    start(name, command(name))


def up(skip_build=False):
    LOCAL.mkdir(exist_ok=True)
    if not skip_build:
        run(["make", "build"])
    if run(["podman", "info"], capture=True, check=False).returncode:
        run(["podman", "machine", "start", "--update-connection", "infrastructure-tutorial"])
    clusters = run(["kind", "get", "clusters"], capture=True).stdout.splitlines()
    if "infrastructure-tutorial" not in clusters:
        run(["kind", "create", "cluster", "--name", "infrastructure-tutorial", "--image",
             "kindest/node:v1.37.0", "--kubeconfig", KUBECONFIG, "--wait", "120s"])
    elif not KUBECONFIG.exists():
        run(["kind", "export", "kubeconfig", "--name", "infrastructure-tutorial", "--kubeconfig", KUBECONFIG])
    if run(["temporal", "operator", "cluster", "health", "--address", "127.0.0.1:7233"],
           capture=True, check=False).returncode:
        (LOCAL / "temporal").mkdir(exist_ok=True)
        start("temporal", ["/opt/homebrew/bin/temporal", "server", "start-dev", "--ip", "127.0.0.1",
                           "--db-filename", LOCAL / "temporal" / "dev.db"])
    wait_for(lambda: run(["temporal", "operator", "cluster", "health", "--address", "127.0.0.1:7233"],
                         capture=True, check=False).returncode == 0, description="Temporal")
    kubectl("apply", "-f", LAB / "config" / "crd" / "bases")
    kubectl("wait", "--for=condition=Established", "crd/gpuclusters.infrastructure.tutorial.example.com", "--timeout=30s")
    kubectl("apply", "-f", LAB / "config" / "live" / "web.yaml")
    start("provider", command("provider"))
    wait_for(lambda: urllib.request.urlopen("http://127.0.0.1:9080/healthz", timeout=2).status == 200,
             description="provider")
    start("worker", command("worker"))
    start("controller", command("controller"))
    wait_for(lambda: urllib.request.urlopen("http://127.0.0.1:8091/readyz", timeout=2).status == 200,
             description="controller")
    kubectl("rollout", "status", "deployment/tutorial-web", "--timeout=120s")
    start("web", command("web"))
    wait_for(lambda: urllib.request.urlopen("http://127.0.0.1:8088", timeout=2).status == 200,
             description="Kubernetes web workload")
    print("Lab running. Temporal UI: http://127.0.0.1:8233 | Kubernetes web: http://127.0.0.1:8088")
    print("Provider inventory: http://127.0.0.1:9080/v1/inventory")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "down", "status", "restart"])
    parser.add_argument("service", nargs="?", choices=["provider", "worker", "controller", "web"])
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--kill", action="store_true", help="kill the selected lab process to demonstrate a crash")
    args = parser.parse_args()
    if args.action == "up":
        up(args.skip_build)
    elif args.action == "down":
        for name in ["web", "controller", "worker", "provider"]:
            stop(name)
        print("Lab processes stopped; provider data, Temporal server/history, and kind cluster preserved.")
    elif args.action == "restart":
        if not args.service:
            parser.error("restart requires a service")
        restart(args.service, kill=args.kill)
    else:
        print(json.dumps({name: {**entry, "running": alive(entry)} for name, entry in state().items()}, indent=2))
        kubectl("get", "gpuclusters", "-o",
                "custom-columns=NAME:.metadata.name,PHASE:.status.phase,READY:.status.readyHosts,DESIRED:.spec.nodeCount")
        kubectl("get", "pods")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
