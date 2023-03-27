from __future__ import annotations

import pendulum
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.decorators import task
#from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
# [START dataset_def]
uniprot_data = Dataset("/opt/airflow/data/Q9Y261.xml")

# it could be a single dag with different functions being called...
with DAG(
    dag_id="create_uniprot_xml_dataset",
    catchup=False,
    start_date=pendulum.datetime(2023, 3, 26, 18, 59, tz="UTC"),
    schedule=timedelta(minutes=1),
    tags=["uniprot", "xml", "produces", "dataset-scheduled", "simulates"],
) as create_uniprot_xml_dataset:
    BashOperator(outlets=[uniprot_data], task_id="create_uniprot_xml_dataset", bash_command='echo "`date` simulated the ingestion of a new uniprot xml dataset..."')

# [START dag_dep]
with DAG(
    dag_id="process_uniprot_xml",
    catchup=False,
    start_date=pendulum.datetime(2023, 3, 24, tz="UTC"),
    schedule=[uniprot_data], # starts when the process that produces this file completes.
    tags=["uniprot", "xml", "consumes", "dataset-scheduled"],
) as process_uniprot_xml:
    @task.virtualenv(
        task_id="process_uniprot_xml", requirements=["neo4j==5.6.0", "biopython==1.81"], system_site_packages=False
    )
    def callable_virtualenv():
        import re
        import logging
        import Bio.SeqIO.UniprotIO as uniprotio
        from neo4j import GraphDatabase
        from neo4j.exceptions import ServiceUnavailable
        logger = logging.getLogger(__name__)
        logger.setLevel("DEBUG")
        # if the logger is new add a handler
        if(logger.handlers == []):
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(levelname)s\t%(asctime)s\t\t%(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.propagate = False

        def create_protein_record(seq_rec):
            relation_stmt = f"""
                    create(p:Protein {{id: '{seq_rec.id}'}})
                """
            #print(relation_stmt)
            return relation_stmt

        def create_organism_record(org_name, protein_id):
            relation_stmt = f"""
                    create(o:Organism {{name: '{org_name}'}})
                    with o
                    match (p:Protein)
                    where p.id = '{protein_id}'
                    create (p)-[r:IN_ORGANISM]->(o)
                    return type(r)
            """
            #print(relation_stmt)
            return relation_stmt

        def create_gene_record(gene_names, gene_status, protein_id):
            # if it's a single value, wraps into a list
            if(type(gene_names) is not list):
                gene_names = [gene_names]
            # iterate through the list and create the gene record + the relationship to the protein
            for gene_name in gene_names:
                relation_stmt = f"""
                    create(g:Gene {{name: '{gene_name}'}})
                    with g
                    match (p:Protein)
                    where p.id = '{protein_id}'
                    create (p)-[r:FROM_GENE {{status: '{gene_status.replace("gene_name_", "")}'}}]->(g)
                    return type(r)
                """
            #print(relation_stmt)
            return relation_stmt

        def create_name_record(name_list, name_type, protein_id):
            name_type = name_type.replace("recommendedName_", "").capitalize()
            for name in name_list:
                relation_stmt = f"""
                    create(n:{name_type} {{name: '{name}'}})
                    with n
                    match (p:Protein)
                    where p.id = '{protein_id}'
                    create (p)-[r:HAS_{name_type.upper()}]->(n)
                    return type(r)
                    """
            #print(relation_stmt)
            return relation_stmt

        def create_feature_record(feature, protein_id):
            qualifiers = feature.qualifiers
            # adds the location to the record
            qualifiers['location_start'] = str(feature.location.start.position)
            qualifiers['location_end'] = str(feature.location.end.position)

            # location relation attribute
            location = {'position_start': feature.location.start.position, 'position_end': feature.location.end.position}
            location = re.sub("'([a-zA-z]+)':", r"\1:", str(location))

            # builds the where clause to fetch the record created
            where_qualifiers = ""
            for key, val in qualifiers.items():
                where_qualifiers += f"""and f.{key} = '{val}' """

            # remove quotes from statement
            qualifiers_str = re.sub("'([a-zA-z]+)':", r"\1:", str(qualifiers))
            relation_stmt = f"""
                create(f:Feature {qualifiers_str})
                with f
                match (p:Protein)
                where p.id = '{protein_id}'
                create (p)-[r:HAS_FEATURE {location}]->(f)
                return type(r)
                """
            #print(relation_stmt)
            return relation_stmt

        # helper class to deal with neo4j connection + transactions
        class GraphDB:
            def __init__(self, uri, user, password):
                self.driver = GraphDatabase.driver(uri, auth=(user, password))

            def close(self):
                # Don't forget to close the driver connection when you are finished with it
                self.driver.close()

            def run_stmt(self, stmt):
                with self.driver.session(database="neo4j") as session:
                    # Write transactions allow the driver to handle retries and transient errors
                    result = session.execute_write(self._run_stmt, stmt)
                    # for row in result:
                    #     print(f"Transaction returned: {result}")

            @staticmethod
            def _run_stmt(tx, stmt):
                # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
                # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
                try:
                    logger.debug("Running statement: %s", stmt)
                    result = tx.run(stmt)
                    # return(result)
                except ServiceUnavailable as exception:
                    logger.error("{query} raised an error: \n {exception}".format(query=stmt, exception=exception))
                    raise

        if __name__ == "__main__":
            seq_rec = next(uniprotio.UniprotIterator('./data/Q9Y261.xml'))
            protein_id = seq_rec.id
            graph_db_stmts = []
            logger.info("Getting protein record.")
            graph_db_stmts.append(create_protein_record(seq_rec))
            
            logger.info("Getting annotation records (organism, gene, names).")
            for key, value in seq_rec.annotations.items():
                if(re.match(pattern = "organism", string = key)):
                    graph_db_stmts.append(create_organism_record(org_name = value, protein_id = protein_id))
                elif(re.match(pattern = "gene", string = key)):
                    graph_db_stmts.append(create_gene_record(gene_names = value, gene_status = key, protein_id = protein_id))
                elif(re.match(pattern = "recommendedName_.*", string = key)):
                    graph_db_stmts.append(create_name_record(name_list = value, name_type = key, protein_id = protein_id))

            logger.info("Getting feature records.")
            for f in seq_rec.features:
                graph_db_stmts.append(create_feature_record(feature = f, protein_id = protein_id))

            logger.info("Connecting to neo4j.")
            uri = "neo4j://graphdb:7687"
            user = "neo4j"
            password = "temp1234"
            app = GraphDB(uri, user, password)
            
            logger.info("Cleaning neo4j db.")
            # clean db
            app.run_stmt("match (a) -[r] -> () delete a, r")
            app.run_stmt("match (a) delete a")

            logger.info("Sending data to neo4j.")
            for stmt in graph_db_stmts:
                app.run_stmt(stmt)
            app.close()

    process_uniprot_xml = callable_virtualenv()

    process_uniprot_xml    




