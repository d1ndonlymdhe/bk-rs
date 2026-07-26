#!/usr/bin/env python3
import argparse
import random
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

IP_PORT_RE = re.compile(r"IP:\s*([^,]+),\s*Port:\s*(\d+)")


class NodeProcess:
    def __init__(self, name: str, proc: subprocess.Popen[str]):
        self.name = name
        self.proc = proc


def stream_output(name: str, proc: subprocess.Popen[str], on_line=None) -> None:
    if proc.stdout is None:
        return
    for line in proc.stdout:
        text = line.rstrip("\n")
        print(f"[{name}] {text}")
        if on_line is not None:
            on_line(text)


def launch_node(
    work_dir: Path,
    node_name: str,
    root_ip: str | None = None,
    root_port: int | None = None,
    use_binary: bool = False,
) -> NodeProcess:
    if use_binary:
        cmd = [str(work_dir / "target" / "release" / "bk-rs"), node_name]
    else:
        cmd = ["cargo", "run", "--", node_name]
    if root_ip is not None and root_port is not None:
        cmd.extend([root_ip, str(root_port)])

    proc = subprocess.Popen(
        cmd,
        cwd=str(work_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    return NodeProcess(node_name, proc)


def wait_for_root_address(root: NodeProcess, timeout_sec: int = 120) -> tuple[str, int]:
    ready = threading.Event()
    result = {"ip": None, "port": None}

    def on_line(line: str) -> None:
        match = IP_PORT_RE.search(line)
        if match:
            result["ip"] = match.group(1).strip()
            result["port"] = int(match.group(2))
            ready.set()

    t = threading.Thread(target=stream_output, args=(root.name, root.proc, on_line), daemon=True)
    t.start()

    if not ready.wait(timeout_sec):
        raise TimeoutError("Timed out waiting for root node to report IP and port")

    ip = result["ip"]
    port = result["port"]
    if ip is None or port is None:
        raise RuntimeError("Root node started, but could not parse IP/port")

    return ip, port


def monitor_node_output(node: NodeProcess) -> threading.Thread:
    t = threading.Thread(target=stream_output, args=(node.name, node.proc), daemon=True)
    t.start()
    return t


def stop_all(nodes: list[NodeProcess]) -> None:
    print("\nStopping all nodes...")
    for node in nodes:
        if node.proc.poll() is None:
            try:
                node.proc.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass

    deadline = time.time() + 0.5
    for node in nodes:
        if node.proc.poll() is None:
            remaining = max(0.0, deadline - time.time())
            try:
                node.proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                node.proc.terminate()

    for node in nodes:
        if node.proc.poll() is None:
            try:
                node.proc.kill()
            except ProcessLookupError:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch N bk-rs nodes with staggered delays.")
    parser.add_argument("n", type=int, help="Total number of nodes to launch (including root)")
    parser.add_argument("--node-prefix", default="node", help="Node name prefix (default: node)")
    parser.add_argument("--delay-min", type=float, default=5.0, help="Minimum delay between launches in seconds")
    parser.add_argument("--delay-max", type=float, default=10.0, help="Maximum delay between launches in seconds")
    parser.add_argument(
        "--connect-ip",
        default="0.0.0.0",
        help="IP used by non-root nodes to connect to root (default: 0.0.0.0)",
    )
    parser.add_argument(
        "-b",
        "--binary",
        action="store_true",
        help="Use compiled release binary at target/release/bk-rs instead of cargo run",
    )
    args = parser.parse_args()

    if args.n < 1:
        print("n must be >= 1", file=sys.stderr)
        return 1

    if args.delay_min < 0 or args.delay_max < 0 or args.delay_min > args.delay_max:
        print("Invalid delay range", file=sys.stderr)
        return 1

    work_dir = Path(__file__).resolve().parent
    nodes: list[NodeProcess] = []

    try:
        root_name = f"{args.node_prefix}0"
        print(f"Launching root: {root_name}")
        root = launch_node(work_dir, root_name, use_binary=args.binary)
        nodes.append(root)

        reported_ip, root_port = wait_for_root_address(root)
        print(f"Root reported IP={reported_ip}, Port={root_port}")
        print(f"Peers will connect via IP={args.connect_ip}, Port={root_port}")

        for i in range(1, args.n):
            delay = random.uniform(args.delay_min, args.delay_max)
            print(f"Waiting {delay:.1f}s before launching next node...")
            time.sleep(1)

            node_name = f"{args.node_prefix}{i}"
            print(f"Launching peer: {node_name}")
            peer = launch_node(work_dir, node_name, args.connect_ip, root_port, args.binary)
            nodes.append(peer)
            monitor_node_output(peer)

        print("\nAll nodes launched. Press Ctrl+C to stop.")
        while True:
            if any(node.proc.poll() is not None for node in nodes):
                for node in nodes:
                    if node.proc.poll() is not None:
                        print(f"Process exited: {node.name} (code {node.proc.returncode})")
                break
            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        stop_all(nodes)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
