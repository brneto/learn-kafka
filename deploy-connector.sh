#!/usr/bin/bash

example () {
  echo -e "Example:"
  echo -e "\t$0\n"
}

usage () {
  echo -e "Usage: $0"
  echo -e "Package and deploy connector project to local kafka connect\n"
  example
  echo -e "Operation:"
  echo -e "\t-h, --help   Prints this help"
}

error () {
  echo >&2 "deploy-connector:" "$@"
  echo "Try '$0 --help' or '$0 -h' for more information"
  exit 1
}

readPomTag () {
  local depth=0

  while IFS= read -r line; do

    # Disregard top xml tag and comment tags
    if [[ $line =~ \<?xml[[:blank:]].*\> || $line =~ \<!--.*--\> ]]; then
      continue
    fi

    # Extract the value of the tag trimmed
    if [[ $line =~ \<$1\> ]] && ((depth == 1)); then
      sed -n "s/<$1>\(.*\)<\/$1>/\1/p" <<< "$line" | xargs echo
      return
    fi

    # Set the xml tag depth
    if [[ ! $line =~ \<.+\>.*\<.+\> ]]; then
      if [[ $line =~ \<\/.+\> ]]; then
        depth=$((depth-1))
      elif [[ $line =~ \<.+\> ]]; then
        depth=$((depth+1))
      fi
    fi
    #echo "[$depth] $line"

  done < "$2"
}

stop_service () {
  sudo systemctl stop "$system_service"
}

start_service () {
  sudo systemctl start "$system_service"
}

package () {
  mvn clean package
}

prepare () {
  sudo rm -rf "$connectors_dir/$artifactId"
}

deploy () {
  sudo tar -C "$connectors_dir" --strip-components=4 \
  -xzvf target/"$artifactId"-"$version".tar.gz \
  "$artifactId"-"$version"/usr/share/kafka-connect/"$artifactId"
}

check_file () {
  if [[ ! -f "$1" ]]; then
    error "File $1 could not be found in the current directory"
  fi
}

# Show help
if [[ $1 == "-h" || $1 == "--help" ]]; then
  usage; exit
fi

if [[ $# -gt 0 ]]; then
  error "$0 does not support any argument"
fi

## Execute script
pom='pom.xml'
connectors_dir='/usr/share/java'
system_service='confluent-kafka-connect'

# Preconditions
set -e
check_file $pom
artifactId=$(readPomTag 'artifactId' $pom)
version=$(readPomTag 'version' $pom)

## Actions
set -x
stop_service
package
prepare
deploy
start_service

# Result
cat <<EOF

------------------------------------------------------------------------
 DEPLOY CONNECTOR SUCCESS
------------------------------------------------------------------------
EOF
