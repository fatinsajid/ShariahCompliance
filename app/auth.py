# app/auth.py
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from dal.db_connector import get_user_tenant

security = HTTPBearer()  # Handles Bearer <token> from headers

SUPABASE_JWT_SECRET = "your-supabase-jwt-secret"  # or os.getenv("SUPABASE_JWT_SECRET")
ALGORITHM = "HS256"

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    request: Request = None
):
    """
    Decodes Supabase JWT and attaches tenant_id & role to request.state.
    Raises 401 if invalid or expired.
    """
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=[ALGORITHM], audience="authenticated")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    # Get tenant info from DB
    tenant_info = get_user_tenant(payload.get("sub"))
    if not tenant_info:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Tenant not found")

    # Attach to request.state for route usage
    if request:
        request.state.tenant_id = tenant_info.get("tenant_id", "demo-tenant")
        request.state.role = tenant_info.get("role", "user")

    return {"sub": payload.get("sub"), "role": tenant_info.get("role", "user"), "tenant_id": tenant_info.get("tenant_id", "demo-tenant")}