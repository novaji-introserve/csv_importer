from rest_framework.response import Response
from rest_framework import status
from typing import Any, Optional

class ResponseFormatter:
    @staticmethod
    def success_response(
        title: str, 
        message: str, 
        data: Optional[Any] = None, 
        code: int = 0
    ) -> Response:
        """
        Create a standardized success response
        """
        response_data = {
            "code": code,
            "error": False,
            "title": title,
            "message": message,
            "data": data
        }
        
        response = Response(response_data, status=status.HTTP_200_OK)
        ResponseFormatter._set_cors_headers(response)
        return response
    
    @staticmethod
    def error_response(
        title: str, 
        message: str, 
        status_code: int = status.HTTP_400_BAD_REQUEST, 
        code: int = 1
    ) -> Response:
        """
        Create a standardized error response
        """
        response_data = {
            "code": code,
            "error": True,
            "title": title,
            "message": message
        }
        
        response = Response(response_data, status=status_code)
        ResponseFormatter._set_cors_headers(response)
        return response
    
    @staticmethod
    def _set_cors_headers(response: Response) -> None:
        """
        Set CORS headers for the response
        """
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS, DELETE'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response['Content-Type'] = 'application/json'
        