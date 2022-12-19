# Traducción Fiserv / SAP

Para la integración de datos de Fiserv a SAP hay datos que necesitamos traducir de un sistema a otro.
- Número de cuenta

## Número de cuenta

De acuerdo a la [hoja de cálculo de cuentas][Accounts SAP-Vision], encontramos la siguiente información:
- Producto débito/crédito.
- [num-cte-sap] Número de cliente SAP
- Cuenta CLABE
- Número de cuenta SAP, cuenta base de ahorro
- Cuenta base + extensión
- [num-cte-v] Número de cliente Vision
- Número de cuenta Vision
- Número de tarjeta Vision (débito)
- Tipo de tarjeta
- Número de tarjeta Vision (crédito)
- ¿Es posible (habilitada) la creación?
- ¿Habilitada para compartir número de cuenta? (durante autorización)
- ¿Habilitada para compartir número de cliente? (durante autorización)
- En ATPT

Hacemos el siguiente análisis:
1. [num-cte-v-15] = 09765 000000 [num-cte-sap-7] [num-1]
2. [num-cta-v+18] = [num-cta-sap]-666-MX  # Algunos tenían un 5 adicional, posiblemente error.
                                 -111-MX  Para cuentas de débito y crédito.
   [num-cta-v-19] =


[Accounts SAP-Vision]: Accounts Architect SAP and Vision V0.2.xlsx

