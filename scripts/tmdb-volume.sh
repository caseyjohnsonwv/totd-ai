#!/bin/bash


CREATE_FLAG=0
DELETE_FLAG=0


while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--create)
      CREATE_FLAG=1
      shift
      ;;
    -d|--delete)
      DELETE_FLAG=1
      shift
      ;;
    --replace)
      CREATE_FLAG=1
      DELETE_FLAG=1
      shift
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done


if (( $DELETE_FLAG == 1 )); then
  docker volume rm trackmania-postgres
fi


if (( $CREATE_FLAG == 1 )); then
  docker volume create --name trackmania-postgres
fi
