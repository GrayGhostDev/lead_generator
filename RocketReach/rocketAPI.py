from dotenv import load_dotenv
import rocketreach
import os

load_dotenv()

rr = rocketreach.Gateway(api_key=os.environ.get('ROCKET_REACH_API_KEY'))

# Check if SDK is working
result = rr.account.get()
if result.is_success:
    print(f'Success: {result.account}')
else:
    print(f'Error: {result.message}!')
    

