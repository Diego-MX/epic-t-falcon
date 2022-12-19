# Runbook 
  
El código de este repo se ejecuta en el ambiente de Databricks.   
Utilizamos la nomenclatura `ops-conciliations` para identificar los recursos.  
Este repo (`transformation-layer`) es parte del análogo `ops-conciliations` de manera 
que utilizamos el nombre del segundo por ser más significativo.  

Entonces utiliza los siguientes recursos: 
* Crear `dbks-ops-conciliations` un _scope_ en Databricks.   

* Crear `ops-conciliations` un _service principal_ para el proyecto.  
  Las credenciales se guardan en el _scope_:  
  `aad-tenant-id`, `sp-ops-conciliations-subscription`, `sp-ops-conciliations-client`, `sp-ops-conciliations-secret`.  

* Acceder al _metastore_ `sqlserver-lakehylia-data-<env>`.  
  Crear un usuario y guardar credenciales en el _scope_:  
  `ops-conciliations-metastore-user`, `ops-conciliatons-metastore-pass`.  

* Acceder al contenedor del _datalake_ `stlakehylia<env>`.  
  Asignar acceso al principado en las carpetas:  
  - `ops/regulatory/card-management/transformation-layer`   
  - `ops/card-management/datasets`   
  - `ops/transactions/spei`   

  cada una en (algunos de) los contenedores `raw` 🥩, `bronze` 🥉, `silver` 🥈, `gold` 🥇.  

* (Reservamos este espacio para el listado de tablas que se crean/utilizan en el _metastore_.)  


`Nota:` El link de [Confluence][runbook] es este.  


# Descripción  

Para la entrega del código les pedimos utilizar este formato de repositorio.  

1. Este archivo `readme.md` contendrá en primer lugar los elementos del _runbook_, así como otras cuestiones tanto funcionales como técnicas 
para cualquiera que utilice este código.  


2. El archivo `config.py` tiene los parámetros que se utilizan, desde rutas de archivo, 
  nombres de llaves de acceso, u otros parámetros modificables.   
  Las variables que se deben configurar externamente se enlistan además en este `readme.md`; 
  y también incluimos algunas variables propias del proyecto.  
  Se puede importar en otros _notebooks_ y _scripts_ de Python tal cual:  
  ```  
  from config import DATALAKE_SETUP  
  ```  
  Si se utiliza código en otro lenguaje, favor de incluir en los requerimientos. 

3. Incluimos también la carpeta de `src` para guardar diferentes tipos de funciones.  
  Incluye el archivo `__init__.py` para importarlo como módulo.  
  
4. La carpeta de _notebooks_ contiene los mismos con el propósito de ejecutarse como _jobs_.   
  Utiliza(cemos) la siguiente nomenclatura:  
  - Si el archivo es experimental, iniciar el nombre del _notebook_ con las iniciales del autor:   
    Ejemplo: `dv revisar conexión sftp`  
  - Si el archivo está listo para ejecutarse regularmente, llamar con un índice entero de orden de ejecución:  
    `1 Extract_file carga archivos fuente a tablas delta en zona bronze`  
    `1 carga datos fuente 2`  
    `2 resumir datos por usuario`   
    `3 programar schedule para los Notebooks (1 y 3 meses)`
    
    
[runbook]: https://bineo.atlassian.net/wiki/spaces/~6282a2fbd9ddcc006e9c3438/pages/1725595654/Conciliaciones+-+runbook
