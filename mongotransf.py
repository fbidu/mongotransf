"""
Script that receives two Mongo URI defined databases and copies data from the
first to the second.
"""
import argparse
import os
import subprocess
import sys

try:
    from pymongo.uri_parser import parse_uri
    from pymongo.errors import InvalidURI
except ImportError:
    print("Please install PyMongo")
    sys.exit(1)


# Template for the mongodump command
MONGODUMP_TEMPLATE = "mongodump -h {host} -u {user} -p {password} -d {db} -o {output}/"

# Template for the mongorestore command
MONGORESTORE_TEMPLATE = "mongorestore -h {host} -d {destination_db} -u {user} -p {password} {folder}/{origin_db}"

# Template for host:port
HOST_TEMPLATE = "{host}:{port}"


def main(origin, destination, collection=None):
    """
    Function that receives two Mongo connection URIs, dumps the data from
    `origin` and restores it to `destination`
    """

    # Checking if both URIs have the correct schema
    if not (origin.startswith("mongodb://") and
            destination.startswith("mongodb://")):
        raise Exception("Both origin and destination must be Mongo URLs")

    # Parsing the URIs
    try:
        origin = parse_uri(origin)
        destination = parse_uri(destination)
    except InvalidURI:
        raise Exception("Mongo URL parsing error!")

    # Defining the destination path for the dump
    dump_folder = "dump_" + origin['database']
    dump_path = os.path.join(os.getcwd(), dump_folder)

    # If the path already exists, we exit to avoid data overwriting
    if os.path.exists(dump_path):
        raise Exception('Destination path ' + dump_path + ' already exists!')

    # If the parser returns more than one host, we quit
    if len(origin['nodelist']) != 1 or len(destination['nodelist']) != 1:
        raise Exception("Currently, we do not support more than one host")

    # Assembling the origin host string
    origin_host = HOST_TEMPLATE.format(
        host=origin['nodelist'][0][0],
        port=str(origin['nodelist'][0][1])
    )

    # Assembling the mongodump command
    mongodump_command = MONGODUMP_TEMPLATE.format(
        host=origin_host,
        user=origin['username'],
        password=origin['password'],
        db=origin['database'],
        output=dump_path
    )

    # Assembling the destination host string
    destination_host = HOST_TEMPLATE.format(
        host=destination['nodelist'][0][0],
        port=str(destination['nodelist'][0][1])
    )

    # Assembling the mongorestore command
    if collection:
        mongorestore_command = MONGORESTORE_TEMPLATE.format(
            host=destination_host,
            user=destination['username'],
            password=destination['password'],
            destination_db=destination['database'],
            folder=dump_path,
            origin_db=origin['database'] + '/' + collection + '.bson'
        )

    else:
        mongorestore_command = MONGORESTORE_TEMPLATE.format(
            host=destination_host,
            user=destination['username'],
            password=destination['password'],
            destination_db=destination['database'],
            folder=dump_path,
            origin_db=origin['database']
        )

    if collection:
        mongodump_command = mongodump_command + ' -c ' + collection
        mongorestore_command = mongorestore_command + ' -c ' + collection

    # Printing the generated commands
    print(mongodump_command)
    print(mongorestore_command)

    print("** Importing from origin **")

    # Dumping the data
    mongodump = subprocess.Popen(mongodump_command,
                                 shell=True,
                                 stdout=subprocess.PIPE)

    # Waiting the process to finish
    mongodump.wait()

    print("** Exporting to destination **")

    # Restoring the data
    mongorestore = subprocess.Popen(mongorestore_command,
                                    shell=True,
                                    stdout=subprocess.PIPE)
    # Waiting for the process to finish
    mongorestore.wait()

    print("Done!")


if __name__ == "__main__":
    # Registering an argument parser
    parser = argparse.ArgumentParser(description="Migrates data from one Mongo database to another, using Mongo URLs")
    # Adding the argument for the origin URI
    parser.add_argument('origin', help='The Mongo URI for the origin server')
    # Adding the argument for the destination URI
    parser.add_argument('destination', help='The Mongo URI for the destination server')
    # Adding the argument for collection
    parser.add_argument('collection', help='If you want to export a single collection, specify it here')
    # Parsing the arguments
    args = parser.parse_args()
    # Calling main
    main(args.origin, args.destination, args.collection)
