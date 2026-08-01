import argparse
import json
import random
import uuid
import urllib.request
import time


def send_vote_request(host):
    vote_options = ["A", "B", "C", "D", "E", "F"]
    random_vote = random.choice(vote_options)
    rand_uuid = str(uuid.uuid4())
    url = f"http://{host}/add_vote/{rand_uuid}/{random_vote}"
    req = urllib.request.Request(url=url, method="POST")
    with urllib.request.urlopen(req) as response:
        response_text = response.read().decode("utf-8")
        print(f"SENT VOTER {rand_uuid} VOTE {random_vote}")
        print(response_text)
    return rand_uuid, random_vote


def main():
    parser = argparse.ArgumentParser(description="Attempt N votes")
    parser.add_argument("n", type=int, help="Total number of votes to launch")
    parser.add_argument(
        "--root-ip",
        default="0.0.0.0",
        help="IP address of the root node's HTTP server (default: 0.0.0.0, matching the "
        "--public-ip default in the Rust node)",
    )
    parser.add_argument(
        "--root-port",
        type=int,
        default=8000,
        help="Port of the root node's HTTP server (default: 8000, the Rocket server's fixed "
        "port; this is not the P2P --public-port used between nodes)",
    )
    parser.add_argument(
        "--out",
        default="votes_record.jsonl",
        help="File to write cast votes to (cleared at the start of each run), for later tallying with tally.py",
    )
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between votes")

    args = parser.parse_args()

    host = f"{args.root_ip}:{args.root_port}"

    with open(args.out, "w") as f:
        for _ in range(args.n):
            voter_id, vote = send_vote_request(host)
            f.write(json.dumps({"voter_id": voter_id, "vote": vote}) + "\n")
            f.flush()
            time.sleep(args.delay)

    print(f"Recorded {args.n} votes to {args.out}")


main()
