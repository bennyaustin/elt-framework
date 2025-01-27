# elt-framework
The Extract Load Transform (ELT) framework is a metadata-driven orchestration framework designed for modern cloud data platforms. It simplifies ingestion and transformation pipelines, ensuring a consistent development experience and ease of maintenance. The framework supports batch ingestion and has been extensively tested with **[Microsoft Fabric](https://github.com/bennyaustin/fabric-dataplatform)** and Azure managed services like **[Azure Databricks](https://github.com/rorymcmanus87/databricks-dataplatform)** and **[Azure Synapse](https://github.com/bennyaustin/synapse-dataplatform)**. It utilizes an ANSI-compatible control database as the metadata repository.

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

## Documentation  
Key concepts and configuration metadata explained in detail at **[Wiki](https://github.com/bennyaustin/elt-framework/wiki)**

## Getting Started
To get started follow these steps: 
1. **Clone or Fork the Repository**: Start by cloning or forking the repository from [github.com/bennyaustin/elt-framework](https://github.com/bennyaustin/elt-framework).

2. **Deploy ControlDB**: The GitHub Action [workflows/ControlDB-deployment.yml](https://github.com/bennyaustin/elt-framework/blob/main/.github/workflows/ControlDB-deployment.yml) executes the workflow to deploy controlDB objects.

**Pre-Requisites** 
* controlDB is already provisioned by IaC process like [07-IaC-Bicep](https://github.com/bennyaustin/fabric-accelerator/wiki/07-IaC-Bicep) or [iac-synapse-dataplatform](https://github.com/bennyaustin/iac-synapse-dataplatform)
* Create <sup>[1](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal)</sup> or reuse an existing Service Principal. Take note of the Application (client) ID and the secret, they will be required later.
* Grant _db_owner_ permission to the Service Principal
```sql
CREATE USER [<service_principal>] FROM EXTERNAL PROVIDER
GO
EXEC sp_addrolemember 'db_owner', [<service_principal>]
GO
```

This GitHub Action requires the following repository secrets:

1. **CLIENT_ID**: Client/Application ID of the Service Principal.
1. **CLIENT_SECRET**: Service Principal Secret.
1. **SUBSCRIPTION_ID**: Azure Subscription ID of controlDB
1. **TENANT_ID**: Entra Tenant ID of controlDB
1. **CONTROLDB_CONNECTIONSTRING**: controlDB connection string in service principal authentication format.
```
Server=<SQL Server>;Authentication=Active Directory Service Principal; Encrypt=True;Database=controlDB;User Id=<Service Principal Client/Application ID>;Password=<Service Principal Secret>
```
Now, hit the Run Workflow on GitHub action [ControlDB-deployment.yml](https://github.com/bennyaustin/elt-framework/actions/workflows/ControlDB-deployment.yml) to deploy database objects.

## Implementation References
* **[Microsoft Fabric data platform using ELT Framework](https://github.com/bennyaustin/fabric-dataplatform)** 
* **[Azure Databricks data platform using ELT Framework](https://github.com/bennyaustin/synapse-dataplatform)**
* **[Azure Synapse data platform using ELT Framework](https://github.com/bennyaustin/synapse-dataplatform)**

## Collaborate
You can collaborate in various ways, including:
- Pull Requests
- Update/Enrich [Wiki documentation](https://github.com/bennyaustin/elt-framework/wiki)
- Raise [issues](https://github.com/bennyaustin/elt-framework/issues) when you spot one
- Answer questions in the [discussion](https://github.com/bennyaustin/elt-framework/discussions) forum

Please contact me to be added as a contributor.

## Contact
If you have any questions or need support, please contact the maintainer:
- [Benny Austin](https://github.com/bennyaustin)
