from boto import rds
import sys
import datetime
import time
import argparse

INSTANCE_ID_TMPL = 'scriptedsnap-{0}-{1}'


def create_snapshot(instance_id, purge_days=None, aws_key_id=None, aws_secret_key=None, region='eu-west-1'):
    today = datetime.date.today()
    conn = rds.connect_to_region(region, aws_access_key_id=aws_key_id, aws_secret_access_key=aws_secret_key)
    instances = conn.get_all_dbinstances(instance_id=instance_id)
    if len(instances) != 1:
        print "Unable to locate instance id {0} in region {1}".format(instance_id, region)
        sys.exit(1)
    instance = instances[0]
    snapshot_id = INSTANCE_ID_TMPL.format(instance_id, today.isoformat())

    print "Creating snapshot {0} for instance {1}".format(snapshot_id, instance_id)
    instance.snapshot(snapshot_id)
    snapshot = next((s for s in conn.get_all_dbsnapshots(instance_id=instance_id) if s.id == snapshot_id), None)
    if snapshot:
        status = snapshot.status
        print "Creating snapshot {0} for instance {1}. Status: {2}".format(snapshot_id, instance_id, status)
        while status == 'creating':
            time.sleep(1)
            snapshot = next((s for s in conn.get_all_dbsnapshots(instance_id=instance_id) if s.id == snapshot_id), None)
            status = snapshot.status
        print "Created snapshot {0} for instance {1}. Status: {2}".format(snapshot_id, instance_id, status)

    if purge_days is not None:
        if status != 'available':
            print "Skipping purge as latest snapshot isn't marked as available"
            sys.exit(1)

        purge_day = today - datetime.timedelta(days=purge_days)
        purge_snapshot_id = INSTANCE_ID_TMPL.format(instance_id, purge_day.isoformat())

        snapshot = next((s for s in conn.get_all_dbsnapshots(instance_id=instance_id) if s.id == purge_snapshot_id), None)
        if snapshot:
            print "Deleting snapshot {0} for instance {1}".format(snapshot.id, instance_id)
            conn.delete_dbsnapshot(snapshot.id)
            print "Deleted snapshot {0} for instance {1}".format(snapshot.id, instance_id)


def main():
    parser = argparse.ArgumentParser(description='Create an snapshot for the supplied rds instance')
    parser.add_argument('-k', '--aws_access_key_id', default=None, help='AWS access key id')
    parser.add_argument('-s', '--aws_secret_access_key', default=None, help='AWS secret access key')
    parser.add_argument('-r', '--region', default='eu-west-1', help='AWS region')
    parser.add_argument('-p', '--purge_age', metavar='DAYS', type=int, default=None, help='The age (in days) of the snapshot to purge. Useful for rolling backup window')
    parser.add_argument('instance_id', help='RDS db instance id')
    ns = parser.parse_args()

    create_snapshot(ns.instance_id, purge_days=ns.purge_age, aws_key_id=ns.aws_access_key_id, aws_secret_key=ns.aws_secret_access_key, region=ns.region)


if __name__ == '__main__':
    main()
