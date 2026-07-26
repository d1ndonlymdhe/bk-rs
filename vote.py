import argparse
import random
import uuid;
import urllib.request;
import time;
def send_vote_request():
    vote_options = ["A","B","C","D","E","F"];
    random_vote = random.choice(vote_options);
    rand_uuid = uuid.uuid4();
    url = f"http://localhost:8000/add_vote/{rand_uuid}/{random_vote}"
    req = urllib.request.Request(url=url,method="POST")
    with urllib.request.urlopen(req) as response:
        response_text = response.read().decode("utf-8")
        print(f"SENT VOTER {rand_uuid} VOTE {random_vote}")
        print(response_text)

def main():
    parser = argparse.ArgumentParser(description="Attempt N votes")
    parser.add_argument("n", type=int, help="Total number of votes to launch")

    args = parser.parse_args();
    
    n = args.n;
    
    for _ in range(0,n):
        send_vote_request();
        time.sleep(1)
        pass
    
    return
    


    
main()