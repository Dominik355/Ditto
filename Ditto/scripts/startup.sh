#!/bin/bash
#Zoberie env premennu KAFKA_CLUSTERS, ktora by mala obsahovat nazvy prefixov kafkovych
# clustrov, ktore su definovane v env premennych. Tie sa primarne includuju z gitops-infrastructure repa,
# kde je kafka env subor. Nasledne sa to rozbije podla ciarok na array a to sa prechadza.
# Zoberie sa template_file a placeholder sa nahradi s kazdym elementom z pola.
# vytvori sa vysledny .properties subor a prezenie sa to cez configgenerator. Ten to ulozi na classpath
# nakoniec spusti Ditto
set -ex

if ! [ -n "$KAFKA_CLUSTERS" ]; then
    echo "Variable KAFKA_CLUSTERS is not defined."
    exit 0
fi

IFS=', ' read -r -a clusterList <<< "$KAFKA_CLUSTERS"

if [ ${#clusterList[@]} -eq 0 ]; then
    echo "Cluster list is empty. Exiting."
    exit 0
fi

placeholder="PLACEHOLDER"
template_file="/app/config/KafkaPropertiesTemplate.tmpl"

mkdir ${DITTO_KAFKA_DIRECTORY}

for currentCluster in "${clusterList[@]}"
do
    echo "Setting Kafka cluster with name $currentCluster"

    current_tmpl="$(dirname "${template_file}")/${currentCluster}.tmpl"

    output_file="${DITTO_KAFKA_DIRECTORY}/${currentCluster}.properties"

    sed "s/$placeholder/$currentCluster/g" "$template_file" > "$current_tmpl"

    touch "$output_file"

    configgenerator --template "$(realpath "${current_tmpl}"):$(realpath "${output_file}")" --cert-in "/etc/input-certs"
done

cp /app/build/libs/Ditto-*.*.*.jar /app/ditto.jar

java -jar /app/ditto.jar