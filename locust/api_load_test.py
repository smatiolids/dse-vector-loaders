import json
from locust import HttpUser, TaskSet, task, between
from locust.contrib.fasthttp import FastHttpUser
from random import randrange

records = []
with open('/dados/seg/processed/segmentaa', 'r') as file:
    for line in file:
        record = json.loads(line)
        records.append(record)
print("JSON Loaded")


class UserBehavior(TaskSet):

    def on_start(self):
        self.current_index = 0

    @task
    def perform_post_request(self):
        if not records:
            return
        
        record = records[randrange(len(records))]
        query_value = record.get('titleVector', '')
        #print(query_value)

        self.client.post(f"/api/search", json={"emb": query_value , "limit": 720})

class WebsiteUser(FastHttpUser):
    tasks = [UserBehavior]
    wait_time = between(0, 1)

# Entry point to start the locust user
if __name__ == "__main__":
    import os
    os.system("locust -f api_load_test.py --host http://localhost:3000")
