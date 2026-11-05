import os
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt

JWT_SECRET = os.environ.get("FAIRQUEUE_JWT_SECRET", "dev-secret")
JWT_ALG = "HS256"
bearer = HTTPBearer(auto_error=False)


def issue_token(subject: str) -> str:
    return jwt.encode({"sub": subject}, JWT_SECRET, algorithm=JWT_ALG)


def require_user(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> str:
    if creds is None:
        raise HTTPException(status_code=401, detail="missing token")
    try:
        payload = jwt.decode(creds.credentials, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="invalid token") from exc
    subject = payload.get("sub")
    if not subject:
        raise HTTPException(status_code=401, detail="invalid token")
    return subject
