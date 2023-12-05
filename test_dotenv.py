from dotenv import load_dotenv
import os

load_dotenv()

print(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
