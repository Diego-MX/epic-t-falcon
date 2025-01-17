# Runbook  
  
El código de este repo se ejecuta en el ambiente de Databricks.   
Utilizamos la nomenclatura `ops-conciliations` para identificar los recursos.  
Este repo (`transformation-layer`) es parte del análogo `ops-conciliations` de manera 
que utilizamos el nombre del segundo por ser más significativo.  

Entonces utiliza los siguientes recursos: 
* Crear `ops-conciliations` un _scope_ en Databricks.  

* Crear `ops-conciliations` un _service principal_ para el proyecto.  
  Las credenciales se guardan en el _scope_:  
  `aad-tenant-id`, `sp-ops-conciliations-subscription`, `sp-ops-conciliations-client`, `sp-ops-conciliations-secret`.  

* Crear `kv-ops-conciliations` un _keyvault_ para los secretos del proyecto.  
  Aunque se pueden leer iguales el _scope_ y el _keyvault_, la intensión de separarlos es separar la funcionalidad de cada uno.  
  El _scope_ representa la persona/aplicación que ejecuta el código desde Databricks.  
  El _keyvault_ resguarda los secretos del proyecto.  

* Acceder al contenedor del _datalake_ `stlakehylia<env>`.  
  Asignar acceso al principado en las carpetas:  
  - `ops/{account-management,card-management,core-banking,fraude,regulatory,transactions}`
  cada una en (algunos de) los contenedores `raw` 🥩, `bronze` 🥉, `silver` 🥈, `gold` 🥇.  

* Acceder al _metastore_ `sqlserver-lakehylia-data-<env>`.  
  Crear un usuario y guardar credenciales en el _keyvault_:  
  `ops-conciliations-metastore-user`, `ops-conciliatons-metastore-pass`.  

* También se requieren permisos para las APIs de SAP.  
  En el servidor -o _cluster_- se lee la variable `CORE_ENV` para identificar el acceso al ambiente de SAP.  
  A su vez se requieren las siguientes llaves en el _keyvault_: 
  - `core-api-key` con el `client-id`, 
  - `core-api-secret` con el `client-secret`, 
  - `core-api-user` que tiene el usuario de SAP, 
  - `core-api-password` que tiene la contraseña correspondiente. 

* (Reservamos este espacio para el listado de tablas que se crean/utilizan en el _metastore_.)  

`Nota:` El link de [Confluence][runbook] es este.  


# Descripción  
  
Esta _capa de transformación_ establece un puente entre el sistema de manejo de tarjetas y otros procesos internos.  
El proveedor de tarjetas es Fiserv, y el principal consumidor de los resultados son los reportes regulatorios.   
Si bien los reportes regulatorios recaen en el _core_ bancario -SAP-, aquí se hacen algunas transformaciones intermedias.  

El esquema general es:  
1. `Fiserv` deposita archivos en `raw`/`bronze`. 
2. `Proyecto` convierte los archivos en formatos `~silver`/`gold`. 
3. `SAP` u otros consumidores utilizan los resultados como insumos. 


# Solicitudes Técnicas  

1. El archivo `config.py` tiene los parámetros que se utilizan, desde rutas de archivo, 
  nombres de llaves de acceso, u otros parámetros modificables.   
  Las variables que se deben configurar externamente se enlistan además en este `readme.md`; 
  y también incluimos algunas variables propias del proyecto.  
  Se puede importar en otros _notebooks_ y _scripts_ de Python tal cual:  
  ```{python} 
  from config import DATALAKE_SETUP  
  ```  
  Si se utiliza código en otro lenguaje, favor de incluir en los requerimientos. 

2. Se incluye también la carpeta de `src` para guardar diferentes tipos de funciones.  
  Incluye el archivo `__init__.py` para importarlo como módulo.  
  
4. La carpeta de _notebooks_ contiene los mismos con el propósito de ejecutarse como _jobs_.   



[runbook]: https://bineo.atlassian.net/wiki/spaces/~6282a2fbd9ddcc006e9c3438/pages/1725595654/Conciliaciones+-+runbook
