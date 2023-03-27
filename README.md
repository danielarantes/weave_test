# uniprot xml data ingestion
This repo defines a simple data ingestion pipeline that will read a (simulated) new uniprot-xml file is parsed and (some of) its data is sent to a neo4j database. 

## How to run?

1. from the root of the repo run `docker compose up`
2. log to neo4j using http://localhost:7474 and neo4j/neo4j as user/password. Change it to neo4j/temp1234 (the new password is hardcoded)
3. log to airflow on http://localhost:8080 using airflow/airflow
4. filter dags by using tag "uniprot" or "xml". It should show 2 dags: `create_uniprot_xml_dataset` and `process_uniprot_xml`
5. enable both dags
6. `create_uniprot_xml_dataset` simulates the ingestion (or creation) of a new uniprot dataset.
7. `process_uniprot_xml` parses the xml, creates the neo4j cypher statements and sends it to the server.
   
## DAG dependencies
1. `process_uniprot_xml` depends on `neo4j` and `biopython` python packages.
