from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from django.contrib.auth import authenticate
from oauth2_provider.models import AccessToken, RefreshToken, Application
from oauthlib.common import generate_token
from django.utils import timezone
from datetime import timedelta
from .services import verify_password
from .models import Usuario
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample


def _json_error(message, http_status):
    """Estructura consistente para cualquier error"""
    return Response(
        {"ok": False, "error": {"message": message}},
        status=http_status
    )


@extend_schema(
    summary="Login de usuario",
    description="""
    Autentica un usuario y devuelve tokens OAuth2 para acceso a la API.
    
    **Restricciones:**
    - Solo usuarios con `acceso_web=True` pueden acceder
    - El usuario debe estar activo (`activo=True`)
    - Las contraseñas se validan usando Argon2
    
    **Respuesta exitosa:**
    Devuelve access_token y refresh_token que deben usarse en el header:
    `Authorization: Bearer {access_token}`
    """,
    request={
        'application/json': {
            'type': 'object',
            'required': ['nombre_usuario', 'password'],
            'properties': {
                'nombre_usuario': {
                    'type': 'string',
                    'description': 'Nombre de usuario registrado en el sistema',
                    'example': 'juan.perez'
                },
                'password': {
                    'type': 'string',
                    'format': 'password',
                    'description': 'Contraseña del usuario',
                    'example': 'MiPassword123!'
                }
            }
        }
    },
    responses={
        200: OpenApiResponse(
            description="Login exitoso",
            response={
                'type': 'object',
                'properties': {
                    'ok': {
                        'type': 'boolean',
                        'example': True
                    },
                    'access_token': {
                        'type': 'string',
                        'description': 'Token de acceso OAuth2 (válido por 10 horas)',
                        'example': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...'
                    },
                    'refresh_token': {
                        'type': 'string',
                        'description': 'Token de refresco OAuth2 (válido por 24 horas)',
                        'example': 'def50200...'
                    },
                    'token_type': {
                        'type': 'string',
                        'example': 'Bearer'
                    },
                    'expires_in': {
                        'type': 'integer',
                        'description': 'Tiempo de expiración en segundos',
                        'example': 36000
                    },
                    'scope': {
                        'type': 'string',
                        'example': 'read write'
                    },
                    'user': {
                        'type': 'object',
                        'properties': {
                            'nombre_usuario': {'type': 'string', 'example': 'juan.perez'},
                            'nombre': {'type': 'string', 'example': 'Juan Pérez'},
                            'correo': {'type': 'string', 'example': 'juan.perez@example.com'},
                            'acceso_web': {'type': 'boolean', 'example': True}
                        }
                    }
                }
            }
        ),
        400: OpenApiResponse(
            description="Datos inválidos o faltantes",
            response={
                'type': 'object',
                'properties': {
                    'ok': {'type': 'boolean', 'example': False},
                    'error': {
                        'type': 'object',
                        'properties': {
                            'message': {'type': 'string', 'example': 'nombre_usuario y password son requeridos'}
                        }
                    }
                }
            }
        ),
        401: OpenApiResponse(
            description="Credenciales incorrectas",
            response={
                'type': 'object',
                'properties': {
                    'ok': {'type': 'boolean', 'example': False},
                    'error': {
                        'type': 'object',
                        'properties': {
                            'message': {'type': 'string', 'example': 'Credenciales inválidas'}
                        }
                    }
                }
            }
        ),
        403: OpenApiResponse(
            description="Usuario inactivo o sin acceso web",
            response={
                'type': 'object',
                'properties': {
                    'ok': {'type': 'boolean', 'example': False},
                    'error': {
                        'type': 'object',
                        'properties': {
                            'message': {
                                'type': 'string', 
                                'example': 'Este usuario no tiene acceso a la plataforma web. Use la aplicación móvil.'
                            }
                        }
                    }
                }
            }
        ),
        500: OpenApiResponse(
            description="Error interno del servidor",
            response={
                'type': 'object',
                'properties': {
                    'ok': {'type': 'boolean', 'example': False},
                    'error': {
                        'type': 'object',
                        'properties': {
                            'message': {'type': 'string', 'example': 'Error interno al procesar el login'}
                        }
                    }
                }
            }
        )
    },
    examples=[
        OpenApiExample(
            'Login exitoso',
            value={
                'nombre_usuario': 'juan.perez',
                'password': 'MiPassword123!'
            },
            request_only=True
        )
    ],
    tags=['Autenticación']
)
@api_view(['POST'])
@permission_classes([AllowAny])
def login(request):
    """
    Login para WEB – solo usuarios con acceso_web=True
    """
    nombre_usuario = (request.data.get('nombre_usuario') or "").strip()
    password = request.data.get('password') or ""

    if not nombre_usuario or not password:
        return _json_error("nombre_usuario y password son requeridos", status.HTTP_400_BAD_REQUEST)

    try:
        # 1) buscar usuario
        try:
            usuario = Usuario.objects.get(nombre_usuario=nombre_usuario)
        except Usuario.DoesNotExist:
            return _json_error("Credenciales inválidas", status.HTTP_401_UNAUTHORIZED)

        # 2) reglas de acceso antes de validar credenciales
        if not bool(usuario.activo):
            return _json_error("Usuario inactivo", status.HTTP_403_FORBIDDEN)

        if bool(usuario.acceso_web) is not True:
            return _json_error(
                "Este usuario no tiene acceso a la plataforma web. Use la aplicación móvil.",
                status.HTTP_403_FORBIDDEN
            )

        # 3) validar contraseña
        try:
            if not verify_password(usuario.password, password):
                return _json_error("Credenciales inválidas", status.HTTP_401_UNAUTHORIZED)
        except Exception:
            return _json_error("Credenciales inválidas", status.HTTP_401_UNAUTHORIZED)

        # 4) obtener/crear app OAuth2
        app, _ = Application.objects.get_or_create(
            name='Default App',
            defaults={
                'client_type': Application.CLIENT_CONFIDENTIAL,
                'authorization_grant_type': Application.GRANT_PASSWORD,
            }
        )

        # 5) revocar tokens previos
        AccessToken.objects.filter(user=usuario).delete()
        RefreshToken.objects.filter(user=usuario).delete()

        # 6) emitir nuevos tokens
        expires = timezone.now() + timedelta(seconds=36000)
        access_token = AccessToken.objects.create(
            user=usuario,
            token=generate_token(),
            application=app,
            expires=expires,
            scope='read write'
        )
        refresh_token = RefreshToken.objects.create(
            user=usuario,
            token=generate_token(),
            application=app,
            access_token=access_token
        )

        # 7) payload de respuesta
        acceso_web = bool(usuario.acceso_web)

        return Response({
            "ok": True,
            "access_token": access_token.token,
            "refresh_token": refresh_token.token,
            "token_type": "Bearer",
            "expires_in": 36000,
            "scope": "read write",
            "user": {
                "nombre_usuario": usuario.nombre_usuario,
                "nombre": usuario.nombre,
                "correo": usuario.correo,
                "acceso_web": acceso_web
            }
        }, status=status.HTTP_200_OK)

    except Exception as e:
        return _json_error("Error interno al procesar el login", status.HTTP_500_INTERNAL_SERVER_ERROR)


@extend_schema(
    summary="Logout de usuario",
    description="""
    Cierra la sesión del usuario revocando su token de acceso actual.
    
    **Requiere autenticación:**
    Header: `Authorization: Bearer {access_token}`
    """,
    request=None,
    responses={
        200: OpenApiResponse(
            description="Logout exitoso",
            response={
                'type': 'object',
                'properties': {
                    'message': {'type': 'string', 'example': 'Logout exitoso'}
                }
            }
        ),
        400: OpenApiResponse(
            description="No hay token activo",
            response={
                'type': 'object',
                'properties': {
                    'error': {'type': 'string', 'example': 'No hay token activo'}
                }
            }
        ),
        401: OpenApiResponse(
            description="Token inválido o expirado"
        ),
        500: OpenApiResponse(
            description="Error interno del servidor",
            response={
                'type': 'object',
                'properties': {
                    'error': {'type': 'string'}
                }
            }
        )
    },
    tags=['Autenticación']
)
@api_view(['POST'])
def logout(request):
    """
    Endpoint de logout
    POST /api/auth/logout/
    Header: Authorization: Bearer <token>
    """
    try:
        token = request.auth
        if token:
            token.delete()
            return Response({'message': 'Logout exitoso'})
        return Response(
            {'error': 'No hay token activo'},
            status=status.HTTP_400_BAD_REQUEST
        )
    except Exception as e:
        return Response(
            {'error': str(e)},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@extend_schema(
    summary="Información del usuario autenticado",
    description="""
    Obtiene la información del usuario actualmente autenticado.
    
    **Requiere autenticación:**
    Header: `Authorization: Bearer {access_token}`
    """,
    request=None,
    responses={
        200: OpenApiResponse(
            description="Información del usuario",
            response={
                'type': 'object',
                'properties': {
                    'nombre_usuario': {
                        'type': 'string',
                        'description': 'Nombre de usuario único',
                        'example': 'juan.perez'
                    },
                    'nombre': {
                        'type': 'string',
                        'description': 'Nombre completo del usuario',
                        'example': 'Juan Pérez'
                    },
                    'correo': {
                        'type': 'string',
                        'format': 'email',
                        'description': 'Email del usuario',
                        'example': 'juan.perez@example.com'
                    },
                    'activo': {
                        'type': 'boolean',
                        'description': 'Estado de activación del usuario',
                        'example': True
                    },
                    'acceso_web': {
                        'type': 'boolean',
                        'description': 'Indica si el usuario tiene acceso a la plataforma web',
                        'example': True
                    }
                }
            }
        ),
        401: OpenApiResponse(
            description="Token inválido, expirado o no proporcionado"
        )
    },
    tags=['Autenticación']
)
@api_view(['GET'])
def user_info(request):
    """
    Obtener información del usuario autenticado
    GET /api/auth/me/
    Header: Authorization: Bearer <token>
    """
    user = request.user
    acceso_web = bool(user.acceso_web)
    
    return Response({
        'nombre_usuario': user.nombre_usuario,
        'nombre': user.nombre,
        'correo': user.correo,
        'activo': user.activo,
        'acceso_web': acceso_web
    })