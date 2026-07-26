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
    parser.add_argument("--host", default="localhost:8000", help="Root node host:port")
    parser.add_argument(
        "--out",
        default="votes_record.jsonl",
        help="File to append cast votes to, for later tallying with tally.py",
    )
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between votes")

    args = parser.parse_args()

    with open(args.out, "a") as f:
        for _ in range(args.n):
            voter_id, vote = send_vote_request(args.host)
            f.write(json.dumps({"voter_id": voter_id, "vote": vote}) + "\n")
            f.flush()
            time.sleep(args.delay)

    print(f"Recorded {args.n} votes to {args.out}")


main()
