import argparse
import json
import time
import urllib.request


def read_records(path):
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def get(host, path):
    url = f"http://{host}{path}"
    with urllib.request.urlopen(url) as response:
        return response.read().decode("utf-8")


def get_vote(host, voter_id):
    return get(host, f"/vote/{voter_id}")


def get_total_votes(host):
    return int(get(host, "/votes/total"))


def get_tally(host):
    text = get(host, "/votes/tally")
    tally = {}
    if text:
        for part in text.split(","):
            candidate, count = part.split(":")
            tally[candidate] = int(count)
    return tally


def wait_for_total(host, expected, timeout, poll_interval):
    deadline = time.time() + timeout
    total = get_total_votes(host)
    while total < expected and time.time() < deadline:
        print(f"Waiting for chain to catch up: {total}/{expected} votes visible...")
        time.sleep(poll_interval)
        total = get_total_votes(host)
    return total


def main():
    parser = argparse.ArgumentParser(
        description="Verify individual votes and tally against votes cast by vote.py"
    )
    parser.add_argument(
        "--records",
        default="votes_record.jsonl",
        help="File written by vote.py containing cast votes",
    )
    parser.add_argument("--host", default="localhost:8000", help="Root node host:port")
    parser.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="Seconds to wait for mining/gossip to catch up before tallying",
    )
    parser.add_argument("--poll-interval", type=float, default=2.0)

    args = parser.parse_args()

    records = read_records(args.records)
    expected_tally = {}
    for record in records:
        expected_tally[record["vote"]] = expected_tally.get(record["vote"], 0) + 1

    print(f"Loaded {len(records)} recorded votes from {args.records}")
    total = wait_for_total(args.host, len(records), args.timeout, args.poll_interval)

    print(f"Server reports {total} total votes (expected {len(records)})")

    print("\nVerifying individual votes...")
    matches = 0
    mismatches = []
    not_found = []    

    for record in records:
        voter_id = record["voter_id"]
        expected_vote = record["vote"]
        actual = get_vote(args.host, voter_id)
        if actual == "NOT_FOUND":
            not_found.append(voter_id)
        elif actual == expected_vote:
            matches += 1
        else:
            mismatches.append((voter_id, expected_vote, actual))

    print(f"  Matched:    {matches}/{len(records)}")
    print(f"  Not found:  {len(not_found)}")
    for voter_id in not_found:
        print(f"    - {voter_id} not yet visible on chain")
    print(f"  Mismatched: {len(mismatches)}")
    for voter_id, expected_vote, actual in mismatches:
        print(f"    - {voter_id}: expected {expected_vote}, got {actual}")

    print("\nTally comparison:")
    actual_tally = get_tally(args.host)
    candidates = sorted(set(expected_tally) | set(actual_tally))
    ok = True
    for candidate in candidates:
        expected_count = expected_tally.get(candidate, 0)
        actual_count = actual_tally.get(candidate, 0)
        marker = "OK" if expected_count == actual_count else "MISMATCH"
        if expected_count != actual_count:
            ok = False
        print(f"  {candidate}: expected {expected_count}, got {actual_count} [{marker}]")

    if ok and not mismatches and not not_found:
        print("\nAll votes accounted for and tally matches.")
    else:
        print("\nDiscrepancies found (see above). Try a longer --timeout if votes are still propagating.")


main()
