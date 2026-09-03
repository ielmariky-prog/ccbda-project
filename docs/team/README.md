# Team collaboration and handoff records

Project 11_02 was built by five people across four AWS accounts. These documents
are the written record of how the work was split and handed off between team
members.

| Document | What it records |
|----------|-----------------|
| `Team Coordination Plan.pdf` | The overall plan: who owns which pipeline stage, the account layout, and the agreed data contract between stages. |
| `Ilyas-Dalibor Handoff.pdf` | Ingestion to data preparation (raw price/news to cleaned parquet). |
| `Dalibor-Ralph Handoff.pdf` | Data preparation to sentiment + forecasting (schema, table ownership). |
| `Ralph-Francesco Handoff.pdf` | Forecasting to visualisation (DynamoDB/Athena to QuickSight). |
| `Francesco-Arthur Handoff.pdf` | Visualisation to anomaly alerting (DynamoDB Stream to SNS). |

Each handoff fixed the interface between two adjacent stages so each owner could
deploy into their own account independently.
