import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from django.conf import settings
import pandas as pd
from io import BytesIO
import uuid
from typing import Tuple, List, Dict


class AzureBlobStorageService:
    def __init__(self):
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = os.getenv('AZURE_CONTAINER', 'fuentes-datos')
        
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING no está configurada")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        self._ensure_container_exists()
    
    def _ensure_container_exists(self):
        """Crea el container si no existe"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
            if not container_client.exists():
                container_client.create_container()
        except Exception as e:
            print(f"Error creando container: {e}")
    
    def upload_file(self, file, original_filename: str) -> Tuple[str, str]:
        """
        Sube un archivo a Azure Blob Storage
        Returns: (blob_name, blob_url)
        """
        # Generar nombre único para el blob
        file_extension = original_filename.split('.')[-1]
        blob_name = f"{uuid.uuid4().hex}.{file_extension}"
        
        # Determinar content type
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        if file_extension.lower() == 'csv':
            content_type = 'text/csv'
        
        # Subir archivo
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name,
            blob=blob_name
        )
        
        file.seek(0)  # Resetear puntero del archivo
        blob_client.upload_blob(
            file,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type)
        )
        
        blob_url = blob_client.url
        return blob_name, blob_url
    
    def delete_file(self, blob_name: str) -> bool:
        """Elimina un archivo de Azure Blob Storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            blob_client.delete_blob()
            return True
        except Exception as e:
            print(f"Error eliminando blob: {e}")
            return False
    
    def download_file(self, blob_name: str) -> bytes:
        """Descarga un archivo de Azure Blob Storage"""
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name,
            blob=blob_name
        )
        return blob_client.download_blob().readall()
    
    @staticmethod
    def parse_file_preview(file, file_extension: str) -> Tuple[List[str], List[Dict]]:
        """
        Parse archivo y retorna columnas y preview de datos (primeras 5 filas)
        Returns: (columnas, preview_data)
        """
        file.seek(0)
        
        try:
            if file_extension.lower() in ['xlsx', 'xls']:
                df = pd.read_excel(file, nrows=5)
            elif file_extension.lower() == 'csv':
                df = pd.read_csv(file, nrows=5)
            else:
                raise ValueError(f"Formato no soportado: {file_extension}")
            
            # Limpiar nombres de columnas
            columnas = [str(col).strip() for col in df.columns]
            
            # Convertir fechas y timestamps a strings para serialización JSON
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif df[col].dtype == 'object':
                    # Intentar convertir timestamps que puedan estar como objetos
                    try:
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
            
            # Reemplazar NaN y valores nulos
            df = df.fillna('')
            
            # Convertir a lista de diccionarios
            preview_data = df.to_dict('records')
            
            # Limpieza adicional: convertir cualquier valor no serializable
            for row in preview_data:
                for key, value in row.items():
                    if pd.isna(value) or value is None:
                        row[key] = ''
                    elif not isinstance(value, (str, int, float, bool, list, dict)):
                        row[key] = str(value)
            
            return columnas, preview_data
            
        except Exception as e:
            raise ValueError(f"Error parseando archivo: {str(e)}")