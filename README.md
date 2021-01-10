# elt-framework
Extract Load Transform (ELT) framework is a metadata orchestration framework for modern cloud data platforms. The ELT framework is primarily for batch ingestion and has been extensively tested with Azure PaaS data services. It has several reusable component and can be  easily extendend to cater to custom use cases.  This framework  simplifies ingestion and transformation pipelines while providing consistency among different workloads. It uses a SQL Server Control database which is used as the metadata repository and integrates well with Azure PaaS services like Data Factory pipelines, Data Lake Storage, Databricks Notebooks, Delta Lake, Synapse and Logic apps. 

The ELT Framework supports the following features:
* Configurable.
* Data Source Agnostic. Can ingest from databases, REST API, flat files, JSON, XML etc
* Delta Load and Full Loads.
* Re-run and Retry capability.
* Audit Tracking.
* Removes the need for manual data patching.
* Data Lineage.
* One to many Level1 Transformations.
* Many to many Level2 Transformations.
* Switch on, switch off pipelines and transformations on demand.
* Extendable.
* Capability to integrate with Azure PaaS services like Diagnostic Logging.

An extensive documentation of the ELT Framewwork is available in **[Wiki](https://github.com/bennyaustin/elt-framework/wiki)**
