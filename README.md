# elt-framework
The Extract Load Transform (ELT) framework is a metadata-driven orchestration framework designed for modern cloud data platforms. It simplifies ingestion and transformation pipelines, ensuring a consistent development experience and ease of maintenance. The framework supports batch ingestion and has been extensively tested with Microsoft Fabric and Azure managed services like **[Azure Databricks](https://github.com/rorymcmanus87/databricks-dataplatform)** and **[Azure Synapse](https://github.com/bennyaustin/synapse-dataplatform)**. It utilizes an ANSI-compatible control database as the metadata repository.

## Key Features:
* **Configurable and Extendable:** Easily adapt the framework to meet specific needs.
* **Data Source Agnostic:** Ingest data from various sources such as databases, Delta Lake, REST API, flat files, JSON, XML, without storing connection strings as metadata.
* **Delta and Full Loads:** Support for both incremental and full data loads.
* **Re-run and Retry Capability:** Automatically handle failures without manual intervention.
* **In-built Audit Tracking:** Track data processing activities with built-in audit capabilities.
* **Extended Audit Capability:** Enhance audit tracking with Azure PaaS services like Diagnostic Logging.
* **Eliminates Manual Data Patching:** Streamline data processing by removing the need for manual interventions.
* **Data Lineage Support:** Maintain data lineage throughout the data lifecycle.
* **Level1 and Level2 Transformations:** Support for one-to-many and many-to-many transformations.
* **On-demand Pipeline and Transformation Management:** Enable or disable pipelines and transformations as needed.

The framework includes several reusable artifacts such as data source-specific Data Factory pipelines, Spark notebooks, and Logic apps, which can be readily used or extended for custom use cases.
  
For extensive documentation, visit our **[Wiki](https://github.com/bennyaustin/elt-framework/wiki)**

### Implementation References
* **[Microsoft Fabric dataplatform using ELT Framework](https://github.com/bennyaustin/fabric-dataplatform)** 
* **[Azure Databricks data platform using ELT Framework](https://github.com/bennyaustin/synapse-dataplatform)**
* **[Azure Synapse data platform using ELT Framework](https://github.com/bennyaustin/synapse-dataplatform)**
