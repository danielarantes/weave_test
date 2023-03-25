
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG for demonstrating behavior of Datasets feature.

Notes on usage:

Turn on all the dags.

DAG dataset_produces_1 should run because it's on a schedule.

After dataset_produces_1 runs, dataset_consumes_1 should be triggered immediately
because its only dataset dependency is managed by dataset_produces_1.

No other dags should be triggered.  Note that even though dataset_consumes_1_and_2 depends on
the dataset in dataset_produces_1, it will not be triggered until dataset_produces_2 runs
(and dataset_produces_2 is left with no schedule so that we can trigger it manually).

Next, trigger dataset_produces_2.  After dataset_produces_2 finishes,
dataset_consumes_1_and_2 should run.

Dags dataset_consumes_1_never_scheduled and dataset_consumes_unknown_never_scheduled should not run because
they depend on datasets that never get updated.
"""
from __future__ import annotations

import pendulum
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
# [START dataset_def]
uniprot_data = Dataset("/opt/airflow/data/test.txt")

with DAG(
    dag_id="update_xml",
    catchup=False,
    start_date=pendulum.datetime(2023, 3, 24, 21,59, tz="UTC"),
    schedule=timedelta(minutes=1),
    tags=["uniprot", "xml", "produces", "dataset-scheduled"],
) as dag1:
    # [START task_outlet]
    BashOperator(outlets=[uniprot_data], task_id="producing_xml_data", bash_command='echo "`date` running..." >> /opt/airflow/data/test.txt')
    # [END task_outlet]


# [START dag_dep]
with DAG(
    dag_id="process_xml",
    catchup=False,
    start_date=pendulum.datetime(2023, 3, 24, tz="UTC"),
    schedule=[uniprot_data],
    tags=["uniprot", "xml", "consumes", "dataset-scheduled"],
) as dag3:
    @task.virtualenv(
        task_id="virtualenv_python", requirements=["neo4j==5.6.0"], system_site_packages=False
    )
    def callable_virtualenv():
        from neo4j import GraphDatabase
        import logging
        from neo4j.exceptions import ServiceUnavailable
        class App:
            
            def __init__(self, uri, user, password):
                self.driver = GraphDatabase.driver(uri, auth=(user, password))

            def close(self):
                # Don't forget to close the driver connection when you are finished with it
                self.driver.close()

            def create_friendship(self, person1_name, person2_name):
                with self.driver.session(database="neo4j") as session:
                    # Write transactions allow the driver to handle retries and transient errors
                    result = session.execute_write(
                        self._create_and_return_friendship, person1_name, person2_name)
                    for row in result:
                        print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

            @staticmethod
            def _create_and_return_friendship(tx, person1_name, person2_name):
                # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
                # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
                query = (
                    "CREATE (p1:Person { name: $person1_name }) "
                    "CREATE (p2:Person { name: $person2_name }) "
                    "CREATE (p1)-[:KNOWS]->(p2) "
                    "RETURN p1, p2"
                )
                result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
                try:
                    return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                            for row in result]
                # Capture any errors along with the query and data for traceability
                except ServiceUnavailable as exception:
                    logging.error("{query} raised an error: \n {exception}".format(
                        query=query, exception=exception))
                    raise

            def find_person(self, person_name):
                with self.driver.session(database="neo4j") as session:
                    result = session.execute_read(self._find_and_return_person, person_name)
                    for row in result:
                        print("Found person: {row}".format(row=row))

            @staticmethod
            def _find_and_return_person(tx, person_name):
                query = (
                    "MATCH (p:Person) "
                    "WHERE p.name = $person_name "
                    "RETURN p.name AS name"
                )
                result = tx.run(query, person_name=person_name)
                return [row["name"] for row in result]


        if __name__ == "__main__":
            uri = "neo4j://graphdb:7687"
            user = "neo4j"
            password = "temp1234"
            app = App(uri, user, password)
            app.create_friendship("Alice", "David")
            app.find_person("Alice")
            app.close()

        print("Finished")

    virtualenv_task = callable_virtualenv()
    # [END howto_operator_python_venv]

    virtualenv_task    




