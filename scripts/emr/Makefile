GEOTRELLIS_VERSION ?= 2.0.0
GEOTRELLIS_VERSION_SUFFIX ?= -SNAPSHOT
ASSEMBLY := ../../spark-etl/target/scala-2.11/geotrellis-spark-etl-assembly-${GEOTRELLIS_VERSION}${GEOTRELLIS_VERSION_SUFFIX}.jar

${ASSEMBLY}: $(call rwildcard, ../../spark-etl/src, *.scala) ../../spark-etl/build.sbt ../../build.sbt
	cd ../../; ./sbt spark-etl/assembly -no-colors
	@touch -m ${ASSEMBLY}

ifndef CLUSTER_ID
CLUSTER_ID=$(shell cat terraform/terraform.tfstate | jq -r .modules[].resources[\"aws_emr_cluster.emr-spark-cluster\"].primary.id)
endif
ifndef KEY_PAIR_FILE
ifndef TF_VAR_pem_path
KEY_PAIR_FILE=$(shell cat terraform/variables.tf.json | jq -r ".variable.pem_path.default")
else
KEY_PAIR_FILE=${TF_VAR_pem_path}
endif
endif

terraform-init:
	cd terraform; terraform init

create-cluster:
	cd terraform; terraform apply

destroy-cluster:
	cd terraform; terraform destroy

proxy: 
	cd terraform; aws emr socks --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE}

ssh:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE}

cleanup-zeppelin:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--command 'rm -r /usr/lib/zeppelin/local-repo/*/geotrellis*'

restart-zeppelin:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--command 'sudo restart zeppelin'

stop-zeppelin:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--command 'sudo stop zeppelin'

start-zeppelin:
	cd terraform; aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--command 'sudo start zeppelin'

upload-assembly: ${ASSEMBLY}
	cd terraform; aws emr put --cluster-id ${CLUSTER_ID} --key-pair-file ${KEY_PAIR_FILE} \
	--src ../${ASSEMBLY} --dest /tmp/geotrellis-spark-etl-assembly-${GEOTRELLIS_VERSION}${GEOTRELLIS_VERSION_SUFFIX}.jar

upload-assembly-s3: ${ASSEMBLY}
	aws s3 cp ${ASSEMBLY} s3://geotrellis-test/daunnc/geotrellis-spark-etl-assembly-${GEOTRELLIS_VERSION}${GEOTRELLIS_VERSION_SUFFIX}.jar

ingest-cog:
	aws emr add-steps --output text --cluster-id ${CLUSTER_ID} \
--steps Type=CUSTOM_JAR,Name="Ingest COG",Jar=command-runner.jar,Args=[\
spark-submit,--master,yarn-cluster,\
--class,geotrellis.spark.etl.test.COGIngest,\
--driver-memory,4200M,\
--driver-cores,2,\
--executor-memory,4200M,\
--executor-cores,2,\
--conf,spark.executor.instances=40,\
--conf,spark.yarn.executor.memoryOverhead=700,\
--conf,spark.yarn.driver.memoryOverhead=700,\
s3://geotrellis-test/daunnc/geotrellis-spark-etl-assembly-${GEOTRELLIS_VERSION}${GEOTRELLIS_VERSION_SUFFIX}.jar\
] | cut -f2 | tee last-step-id.txt

ingest-cog-deflate:
	aws emr add-steps --output text --cluster-id ${CLUSTER_ID} \
--steps Type=CUSTOM_JAR,Name="Ingest Deflate COG",Jar=command-runner.jar,Args=[\
spark-submit,--master,yarn-cluster,\
--class,geotrellis.spark.etl.test.COGDeflateIngest,\
--driver-memory,4200M,\
--driver-cores,2,\
--executor-memory,4200M,\
--executor-cores,2,\
--conf,spark.executor.instances=40,\
--conf,spark.yarn.executor.memoryOverhead=700,\
--conf,spark.yarn.driver.memoryOverhead=700,\
s3://geotrellis-test/daunnc/geotrellis-spark-etl-assembly-${GEOTRELLIS_VERSION}${GEOTRELLIS_VERSION_SUFFIX}.jar\
] | cut -f2 | tee last-step-id.txt

ingest:
	aws emr add-steps --output text --cluster-id ${CLUSTER_ID} \
--steps Type=CUSTOM_JAR,Name="Ingest COG",Jar=command-runner.jar,Args=[\
spark-submit,--master,yarn-cluster,\
--class,geotrellis.spark.etl.test.Ingest,\
--driver-memory,4200M,\
--driver-cores,2,\
--executor-memory,4200M,\
--executor-cores,2,\
--conf,spark.executor.instances=70,\
--conf,spark.yarn.executor.memoryOverhead=700,\
--conf,spark.yarn.driver.memoryOverhead=700,\
s3://geotrellis-test/daunnc/geotrellis-spark-etl-assembly-${GEOTRELLIS_VERSION}${GEOTRELLIS_VERSION_SUFFIX}.jar\
] | cut -f2 | tee last-step-id.txt
