from locust import HttpUser, task, between
import random

class WebsitUser(HttpUser):
    wait_time = between(1, 2)
    
    @task
    def enviar_valoracion(self):
        payload = {
            "userId": random.randint(1, 500),
            "movieId": random.choice([1, 2, 3, 4, 5, 6]),
            "rating": round(random.uniform(0.5, 5.0), 1)
        }
        
        self.client.post("/rate", json=payload)