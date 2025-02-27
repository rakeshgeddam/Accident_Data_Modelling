**Project Overview: Car Accident Insurance Claims Data Modeling**
================================================================================================



     Objective: Design a data model to process and analyze car accident insurance claims, supporting business questions like:

What’s the average claim payout by accident severity?

          a) How long does it take to process high-value claims?
          b) Which regions have the highest claim rejection rates?

    Scope: Simulate a dataset of claims, accidents, vehicles, and claimants; transform it into a star schema (fact and dimension tables); and analyze it for insights.

Tools: Python (Pandas/Faker for data generation), PySpark (ETL), AWS S3 (storage), SQL (modeling/queries), optional visualization (e.g., Matplotlib or Tableau).


Data Model
![snowflake_model](https://github.com/user-attachments/assets/ce69de6d-5d50-4e25-b91e-136238308634)

The car accident insurance claims process involves key entities:
=========================================================================================================

Claim: The insurance claim filed (e.g., claim ID, amount, status).
Accident: Details of the incident (e.g., date, location, severity).
Vehicle: Info about the car involved (e.g., make, model, year).
Claimant: The person filing the claim (e.g., name, age, policy ID).
Policy: Insurance policy details (e.g., coverage type, premium).

=========================================================================================================

Business Questions:
Approval rate by region/severity.
Average processing time by claim type (e.g., collision, theft).
Fraud detection flags (e.g., multiple claims from one claimant).

=========================================================================================================

Step 3: Design the Data Model (Star Schema)
For analytics, use a star schema with a fact table and dimension tables.

Fact Table: fact_claims
claim_id (PK)
accident_id (FK)
claimant_id (FK)
vehicle_id (FK)
policy_id (FK)
claim_date (date)
claim_amount (decimal)
status (varchar)
processing_days (int)

Dimension Tables:
===============================================
dim_accidents:

          accident_id (PK)
          accident_date (date)
          location (varchar)
          severity (varchar)

dim_vehicles:

        vehicle_id (PK)
        make (varchar)
        model (varchar)
        year (int)

dim_claimants:

        claimant_id (PK)
        name (varchar)
        age (int)
        policy_id (varchar)


Why Star Schema?
      Simplifies BI queries (e.g., aggregations by location or severity) and aligns with Amazon’s "data modeling background" requirement.


