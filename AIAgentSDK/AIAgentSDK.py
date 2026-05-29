import httpx
import asyncio

class AIAgentSDK:
    def __init__(self, api_url):
        self.api_url = api_url
        self.client = httpx.AsyncClient()
        self.tokens = 100 # Client-side rate limiting bucket

    async def fetch_data(self):
        if self.tokens <= 0:
            print("Client SDK: Burst Limit Reached. Throttling...")
            await asyncio.sleep(1)
            self.tokens = 100
            
        self.tokens -= 1
        response = await self.client.post(f"{self.api_url}/api/v1/crawl_async")
        
        if response.status_code == 202:
            print(f"✅ Fast Accepted: {response.json()['task_id']} (Will poll later)")
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")

async def run_agent():
    sdk = AIAgentSDK("http://localhost:8000")
    tasks = [sdk.fetch_data() for _ in range(5)]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(run_agent())
