---

# 02. Estructura de los Datos (Raw)

## Formato del fichero original

Los ficheros `YYYY.csv.gz` de NOAA tienen el siguiente formato (sin cabecera):

| Columna | Nombre en modelo | Tipo            | Descripción                                               |
| ------- | ---------------- | --------------- | --------------------------------------------------------- |
| 1       | station_id       | string          | Código de 11 caracteres de la estación                    |
| 2       | date             | date (YYYYMMDD) | Fecha de la observación                                   |
| 3       | element          | string          | Código del elemento medido (TMAX, TMIN, PRCP, SNOW, etc.) |
| 4       | data_value       | integer         | Valor de la medición (ver unidades por elemento)          |
| 5       | m_flag           | string          | Measurement Flag                                          |
| 6       | q_flag           | string          | Quality Flag                                              |
| 7       | s_flag           | string          | Source Flag                                               |
| 8       | obs_time         | string          | Hora de observación (HHMM)                                |

### Unidades más importantes (según readme oficial NOAA)

| Element | Descripción                | Unidad en datos originales | Unidad convertida en Silver |
| ------- | -------------------------- | -------------------------- | --------------------------- |
| TMAX    | Temperatura máxima diaria  | 0.1 °C                     | °C (dividido por 10)        |
| TMIN    | Temperatura mínima diaria  | 0.1 °C                     | °C (dividido por 10)        |
| PRCP    | Precipitación total diaria | 0.1 mm                     | mm (dividido por 10)        |
| SNOW    | Nieve caída                | mm                         | mm                          |
| SNWD    | Profundidad de nieve       | mm                         | mm                          |
| AWND    | Velocidad media del viento | 0.1 m/s                    | m/s (dividido por 10)       |

**Nota**: Muchos valores son `-9999` (missing). En la capa Silver se convierten a `NULL`.
