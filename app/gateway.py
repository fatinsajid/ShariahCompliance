import os
import time
from fastapi import Request, HTTPException
from jose import jwt, JWTError
from slowapi import Limiter
from slowapi.util import get_remote_address

# -----------------------------
# Config
# -----------------------------
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")
ALGORITHM = "HS256"

# -----------------------------
# Rate limiter (per IP fallback)
# -----------------------------
limiter = Limiter(key_func=get_remote_address)

# Simple in-memory user rate tracking
USER_RATE_BUCKET = {}
RATE_LIMIT_PER_MINUTE = 60


# -----------------------------
# API Gateway Middleware
# -----------------------------
async def api_gateway_middleware(request: Request, call_next):
    start_time = time.time()

    # ✅ Allow public endpoints
    if request.url.path in ["/health", "/"]:
        return await call_next(request)

    # -----------------------------
    # 1️⃣ Auth check
    # -----------------------------
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth_header.split(" ")[1]

    try:
        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    # Attach user context
    request.state.user = payload

    # -----------------------------
    # 2️⃣ Multi-tenant extraction
    # -----------------------------
    tenant_id = payload.get("sub")  # Supabase user id
    request.state.tenant_id = tenant_id

    # -----------------------------
    # 3️⃣ Simple rate limiting (per user)
    # -----------------------------
    now = time.time()
    bucket = USER_RATE_BUCKET.get(tenant_id, [])

    # keep only last minute
    bucket = [t for t in bucket if now - t < 60]

    if len(bucket) >= RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    bucket.append(now)
    USER_RATE_BUCKET[tenant_id] = bucket

    # -----------------------------
    # 4️⃣ Process request
    # -----------------------------
    response = await call_next(request)

    # -----------------------------
    # 5️⃣ Logging (basic)
    # -----------------------------
    duration = round((time.time() - start_time) * 1000, 2)

    print(
        f"[API_GATEWAY] user={tenant_id} "
        f"path={request.url.path} "
        f"status={response.status_code} "
        f"latency_ms={duration}"
    )

    return response
