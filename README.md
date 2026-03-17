📑 Executive Summary: Logistics Performance & Routing Integration
Project Overview
This Python-based integration engine synchronizes data between two distinct MySQL/MariaDB databases (qualityentregas and QeJS_db) to reconstruct the full lifecycle of delivery routes (Type 35). The script automates the mapping of logistics events—from warehouse departure to final delivery or occurrence—ensuring data integrity by filtering out rejected claims and canceled orders.

Core Functionalities
Multi-Source Data Fusion: Merges operational delivery data with customer service/occurrence records to provide a 360-degree view of each "Romaneio" (Manifest).

Chronological Route Reconstruction:

Start Event (inicio_35): Captures the exact timestamp and GPS coordinates of the route initialization.

Operational Events (Types 2 & 25): Tracks real-time delivery attempts and occurrences, validated against user permissions and geographical constraints.

End Event (fim_35): Logs the official validation and closing of the manifest.

Return to Hub (retorno_cdd): Calculates the final leg of the journey back to the distribution center.

Data Quality & Integrity:

Filters out "NF Duplicada" (Duplicate Invoices) and canceled statuses.

Cross-references occurrence IDs with the QeJS_db to exclude rejected records (Rejeitado != 1).

Ensures chronological consistency by sorting events via a custom-weighted ordem logic.

Technical Stack
Language: Python 3.x

Libraries: Pandas (Data manipulation), PyMySQL (Database connectivity), NumPy (Vectorized operations).

Architecture: Parametrized SQL queries to prevent injection and optimize fetch performance for specific route IDs.

Strategic Impact
By consolidating fragmented database records into a single, clean Excel output (teste.xlsx), this tool enables the leadership team to:

Audit Driver Behavior: Comparing GPS coordinates between issuance and validation.

Optimize SLAs: Analyzing the time delta between route start and the final delivery event.

Financial Accuracy: Ensuring that only validated, non-rejected occurrences are considered for billing and performance KPIs.
