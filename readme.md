# Descripción  

Para la entrega del código les pedimos utilizar este formato de repositorio.  

1. Este archivo `readme.md` contendrá una descripción general, así como el listado de _notebooks_ que se utilizan en el repo.  

2. El archivo `config.py` tiene los parámetros que se utilizan, desde rutas de archivo, nombres de llaves de acceso, u otros parámetros modificables.   
  Se puede importar en otros _notebooks_ y _scripts_ de Python tal cual:  
  ```  
  from config import DATALAKE_SETUP  
  ```  
  Si utilizan código en otro lenguaje, platiquemos opciones para modificarlo.  

3. Incluimos también la carpeta de `src` para guardar diferentes tipos de funciones.  
  Incluye el archivo `__init__.py` para importarlo como módulo.  
  
4. La carpeta de _notebooks_ contiene los mismos con el propósito de ejecutarse como _jobs_.   
  Utilicemos la siguiente nomenclatura:  
  - Si el archivo es experimental, iniciar el nombre del _notebook_ con las iniciales del autor:   
    Ejemplo: `dvp revisar conexión sftp`  
  - Si el archivo está listo para ejecutarse regularmente, llamar con un índice entero de orden de ejecución:  
    `1 Extract_file carga archivos fuente a tablas delta en zona bronze`  
    `1 carga datos fuente 2`  
    `2 resumir datos por usuario`   
    `3 programar schedule para los Notebooks (1 y 3 meses)`
