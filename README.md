# elt-framework
Extract Load Transform (ELT) framework is a metadata-driven orchestration framework for modern cloud data platforms. This framework simplifies ingestion and transformation pipelines while providing consistent development experience and ease of maintenance. This framework supports batch ingestion. The framework has been extensively tested with Microsoft Fabric and Azure managed services like **[Azure Databricks](https://github.com/rorymcmanus87/databricks-dataplatform)** and **[Azure Synapse](https://github.com/bennyaustin/synapse-dataplatform)**. It uses a ANSI compatible control database as the metadata repository. There are several reusable artifacts that can be readily used as-is. The reusable artifacts can also be easily extendend for custom use cases. Reusable artifacts include data source specific Data Factory pipelines, Spark notebooks and Logic apps. 

The ELT Framework supports the following features:
* Configurable and Extendable.
* Data Source Agnostic. Can ingest from databases, Delta Lake, REST API, flat files, JSON, XML etc, connection strings are not stored as metadata.
* Delta and Full Loads.
* Re-run and Retry capability without manual intervention.
* In-built audit Tracking.
* Audit capability can be extended with Azure PaaS services like Diagnostic Logging.
* Removes the need for manual data patching.
* Data Lineage support for the data life cycle.
* One to many Level1 Transformations.
* Many to many Level2 Transformations.
* Enable/Disable pipelines and transformations on demand.
  
An extensive documentation of the ELT Framewwork is available in **[Wiki](https://github.com/bennyaustin/elt-framework/wiki)**
