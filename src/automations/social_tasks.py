import time
from datetime import datetime

def auto_post(platform, content):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Publicando en {platform}: {content}")
    time.sleep(1)
    print(f"✅ Publicación completada en {platform}")

def auto_like(platform, count=10):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Dando {count} likes en {platform}...")
    time.sleep(1)
    print(f"💚 {count} likes completados en {platform}")
