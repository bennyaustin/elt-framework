# elt-framework
Extract Load Transform (ELT) framework is a metadata-driven orchestration framework for modern cloud data platforms. This framework simplifies ingestion and transformation pipelines while providing consistent developmenet experience and ease of maintenance. This framework supports batch ingestion. The framework has been extensively tested with Microsoft Fabric and Azure managed services like Azure Databricks and Azure Synapse. It uses a ANSI compatible control database as the metadata repository. There are several reusable artifacts that can be readily used as-is. The reusable artifacts can also be easily extendend for custom use cases. Reusable artifacts include data source specific Data Factory pipelines, Spark notebooks and Logic apps. 

The ELT Framework supports the following features:
* Configurable.
* Data Source Agnostic. Can ingest from databases, Delta Lake, REST API, flat files, JSON, XML etc
* Delta Load and Full Loads.
* Re-run and Retry capability without manual intervention.
* Audit Tracking.
* Removes the need for manual data patching.
* Data Lineage.
* One to many Level1 Transformations.
* Many to many Level2 Transformations.
* Enable/Disable pipelines and transformations on demand.
* Extendable.
* Audit capability with Azure PaaS services like Diagnostic Logging.

An extensive documentation of the ELT Framewwork is available in **[Wiki](https://github.com/bennyaustin/elt-framework/wiki)**
