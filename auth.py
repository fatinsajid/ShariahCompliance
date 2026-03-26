from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
import httpx

# Supabase project config
SUPABASE_URL = "https://YOUR-PROJECT.supabase.co"
SUPABASE_JWK_URL = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"
AUDIENCE = "YOUR_SUPABASE_API_AUDIENCE"  # often your Supabase project URL
ALGORITHM = "RS256"

security = HTTPBearer()

# Cache keys to avoid fetching every time
_jwk_cache = None

async def get_jwk():
    global _jwk_cache
    if _jwk_cache is None:
        async with httpx.AsyncClient() as client:
            r = await client.get(SUPABASE_JWK_URL)
            r.raise_for_status()
            _jwk_cache = r.json()
    return _jwk_cache

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials

    jwks = await get_jwk()
    header = jwt.get_unverified_header(token)
    kid = header.get("kid")
    key = next((k for k in jwks["keys"] if k["kid"] == kid), None)
    if not key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    try:
        payload = jwt.decode(token, key, algorithms=[ALGORITHM], audience=AUDIENCE)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # You can return the payload or transform into a User object
    return payload