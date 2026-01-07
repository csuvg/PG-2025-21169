# Úsalo solo en dev
import traceback
from django.http import JsonResponse

class DebugJSONMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
    def __call__(self, request):
        try:
            return self.get_response(request)
        except Exception:
            tb = traceback.format_exc()
            return JsonResponse({"detail": "ServerError", "traceback": tb}, status=500)
