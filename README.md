**Project Status 17/10/2023:** Faltan agregar origenes de datos mas complejos y hacer los reportes en PowerBI para las fuentes de datos existentes. Tambien automatizar la extracción de datos con Azure Lambda.
# Portfolio





Mi intención con este proyecto es poder aplicar mis conocimientos de ingenieria y analisis de datos. Para esto diseñe y desarrolle una aplicación que tiene como fin la explotación de datos en Microsoft PowerBI.

                                               Arquitectura de la solucion:

![image](https://github.com/eugeniosawczuk1/Portfolio/assets/147460735/b66ca045-a1d3-4568-a646-e8d26dc1feaf)


1 - Se define una api como fuente de datos.
                                              
                                                
2 - Se consulta a esa api y se guarda la respuesta en un bucket de Amazon S3.
                                              
                                                
3 - Se lee cada archivo del bucket y se lo parsea.
                                              
                                                
4 - Los datos resultantes del parseo se guardan en una base de datos PostgreSQL.
                                              
                                                
5 - A partir de las tablas en la base de datos, se crean datasets para ser explotados en Microsoft PowerBI.
  

A nivel codigo, esto se traduce en una clase Connector para cada fuente de datos. Es decir, un conector es el conjunto de puntos mencionados anteriormente.


**################################################################################################**

**Project Status 10/17/2023:** There's a need to incorporate more complex data sources and create PowerBI reports for the existing data sources. Also automate data-extraction with Azure Lambda-

# Portfolio

My goal with this project is to apply my knowledge of data engineering and analysis. To achieve this, I designed and developed an application aimed at data exploration in Microsoft PowerBI.

                                                Solution Architecture:

![image](https://github.com/eugeniosawczuk1/Portfolio/assets/147460735/b66ca045-a1d3-4568-a646-e8d26dc1feaf)

1. An API is defined as the data source.

2. The API is queried, and the response is saved to an Amazon S3 bucket.

3. Each file in the bucket is read and parsed.

4. The parsed data is stored in a PostgreSQL database.

5. Datasets are created from the tables in the database to be analyzed in Microsoft PowerBI.

From a code perspective, this translates to a "Connector" class for each data source. In other words, a connector encompasses the steps mentioned above.

