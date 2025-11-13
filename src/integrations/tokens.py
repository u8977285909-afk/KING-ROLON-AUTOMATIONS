# src/integrations/tokens.py
import os
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

# TOKENS DE ACCESO A PLATAFORMAS (se rellenan con datos reales después)
TOKENS = {
    "tiktok": os.getenv("TIKTOK_TOKEN", ""),
    "youtube": os.getenv("YOUTUBE_TOKEN", ""),
    "facebook": os.getenv("FACEBOOK_TOKEN", ""),
    "twitch": os.getenv("TWITCH_TOKEN", ""),
    "kik": os.getenv("KIK_TOKEN", ""),
    "instagram": os.getenv("INSTAGRAM_TOKEN", ""),
    "twitter": os.getenv("TWITTER_TOKEN", "")
}

def get_token(platform: str) -> str:
    """Devuelve el token de una plataforma o None si no existe"""
    return TOKENS.get(platform.lower())

def save_token(platform: str, token: str):
    """Guarda temporalmente un token en memoria (más adelante se guardará cifrado en BD)"""
    TOKENS[platform.lower()] = token

if __name__ == "__main__":
    for name, value in TOKENS.items():
        print(f"{name}: {'✅' if value else '❌'}")
