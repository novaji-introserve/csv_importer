from functools import wraps
from typing import Callable, Any
from django.conf import settings
import jwt
from .response_utils import ResponseFormatter
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response

def validate_token(view_func: Callable[[Any, Request], Response]) -> Callable[[Any, Request], Response]:
    """
    Decorator to validate JWT token
    """
    @wraps(view_func)
    def _wrapped_view(self: Any, request: Request, *args: Any, **kwargs: Any) -> Response:
        # Extract token from Authorization header
        auth_header = request.META.get('HTTP_AUTHORIZATION', '')
        if not auth_header.startswith('Bearer '):
            return ResponseFormatter.error_response(
                'Authentication Failed', 
                'Invalid token format', 
                status_code=status.HTTP_401_UNAUTHORIZED
            )
        
        token = auth_header.split(' ')[1]
        
        try:
            # Validate token (replace with your actual token validation logic)
            payload = jwt.decode(
                token, 
                settings.SECRET_KEY, 
                algorithms=['HS256']
            )
            
            # Additional token validation can be added here
            
            # Attach user information to request if needed
            request.user_id = payload.get('user_id')  # type: ignore
            
            return view_func(self, request, *args, **kwargs)
        
        except jwt.ExpiredSignatureError:
            return ResponseFormatter.error_response(
                'Session Expired', 
                'Your session has expired. Please log in again.', 
                status_code=status.HTTP_401_UNAUTHORIZED
            )
        except jwt.InvalidTokenError:
            return ResponseFormatter.error_response(
                'Authentication Failed', 
                'Invalid token. Please log in again.', 
                status_code=status.HTTP_401_UNAUTHORIZED
            )
    
    return _wrapped_view
