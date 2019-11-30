from fabric import Connection
import sys
import argparse

# MySQL user/group
MYSQL_USER = "mysql"
MYSQL_GROUP = "mysql"


def _read_argv():
    ### Check command line parameters
    res = dict()
    parser = argparse.ArgumentParser(description='mysql_copy_dbs.py - command line options')
    parser.add_argument('src', default='', help='Source DB in this format: [user@]host:dbname')
    parser.add_argument('dst', default='', help='Destination DB in this format: [user@]host:dbname')
    parser.add_argument('--tmpdir', default='', help='(Optional) TMP dir on the destination server', required=False)
    parser.add_argument('--copy', default='xtrabackup', help='(Optional) TODO: Method to copy data, default xtrabackup, other options: zfs, lvm (snapshots)', required=False)
    parser.add_argument('--dst_internal_ip', default='',
                        help='(Optional) Internal IP of the destination server (for copying xtrabackup)',
                        required=False)
    parser.add_argument('--force', default='', help='(Optional) Force overwriting DB', required=False,
                        action='store_true')
    args = parser.parse_args()
    src = args.src.split('/')
    dst = args.dst.split('/')
    # print (src, dst)
    res.update({'tmpdir': args.tmpdir, 'force': args.force, 'src_host': src[0], 'dst_host': dst[0],
                'dst_internal_ip': args.dst_internal_ip, 'copy': args.copy})
    if len(src) == 1:
        print("ERROR: Need to provide a list of databases in the format db1[,db2,db3]")
        sys.exit(1)
    else:
        res.update({'src_dbs': src[1].split(',')})
    if len(dst) == 1:
        print("No destination db list, assuming same list as source...")
        res.update({'dst_dbs': src[1].split(',')})
    else:
        res.update({'dst_dbs': dst[1].split(',')})
    print(res)
    return res


def _connect(host, db=''):
    # Check connection first
    c = Connection(host)
    hostname = get_hostname(c)
    print("Connection established to {}. Hostname reported: {}".format(host, hostname))
    # Check that MySQL is running and we can connect to it
    print("MySQL is running, version: {}".format(_run_mysql_query(c, db, "select version()")))
    return c


def run_command(c, cmd, hide=True, ignore=False):
    try:
        res = c.run(cmd, hide=hide)
    except Exception as e:
        print("[ERROR] Can't run command {}, err: {}".format(cmd, str(e)))
        print("Please make sure you can sudo or use root AND you can connect to MySQL from root shell without password")
        if ignore:
            return "Error"
        else:
            sys.exit(1)
    return res.stdout.strip()


def check_connection_between_hosts(c, dst_host):
    cmd = 'ssh {} "hostname"'.format(dst_host)
    try:
        res = c.run(cmd, hide=False)
    except Exception as e:
        print(
            "[ERROR] Can't connect to ssh between hosts, make sure you can connect from source host to {} host, err: {}".format(
                dst_host, str(e)))
        sys.exit(1)


def get_hostname(c, dst_internal_ip=''):
    if dst_internal_ip:
        return dst_internal_ip
    return run_command(c, 'uname -n')


def _run_mysql_query(c, db, sql, sudo=True, hide=True):
    if sudo:
        prefix = "sudo"
    else:
        prefix = ""
    if hide:
        mysql_verbose = ""
    else:
        mysql_verbose = " -vvv "
    return run_command(c, "{} mysql {} -A -NB {} -e '{}'".format(prefix, mysql_verbose, db, sql), hide=hide)


def mysql_query(c, db, sql, sudo=True, hide=True):
    return _run_mysql_query(c, db, sql, sudo, hide).split('\n')


def list_innodb_tables(c, db):
    q = 'SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = "{}" AND ENGINE = "InnoDB"'.format(db)
    # print(q)
    return mysql_query(c, '', q)


def get_datadir(c, db):
    return mysql_query(c, db, "SELECT @@datadir")[0]


def _alter_table_tablespaces(c, db, op):
    TABLES = list_innodb_tables(c, db)
    q = "SET FOREIGN_KEY_CHECKS=0; "
    for t in TABLES:
        q = q + "; ALTER TABLE {}.{} {} TABLESPACE".format(db, t, op)
    return mysql_query(c, db, q, True, False)


def discard_tablespaces(c, db):
    _alter_table_tablespaces(c, db, "DISCARD")


def import_tablespaces(c, db):
    _alter_table_tablespaces(c, db, "IMPORT")


def database_exists(c, db):
    q = 'select schema_name from information_schema.schemata where schema_name="{}"'.format(db)
    if mysql_query(c, '', q)[0] == db:
        return True
    else:
        return False


def create_database(c, db):
    mysql_query(c, '', "create database if not exists {}".format(db))


def copy_schema(c, src_db, dst_host, dst_db):
    cmd = "sudo mysqldump --no-data --triggers --single-transaction --set-gtid-purged=OFF --skip-add-locks {} | ssh {} sudo mysql {}".format(
        src_db, dst_host, dst_db
    )
    run_command(c, cmd)


def create_tmp_dir(c, prefix='/tmp'):
    #if not tmpdir or tmpdir == '':
    tmp_backup_dir = run_command(c, "mktemp -d -t mycpdb.XXXX -p {}".format(prefix))
    print("Created TMPDIR={}".format(tmp_backup_dir))
    #else:
    #    # TODO: check if it exists already
    #    tmp_backup_dir = tmpdir
    return tmp_backup_dir


def make_backup(c, src_db, dst_host, tmp_backup_dir):
    cmd = 'sudo xtrabackup --skip-tables-compatibility-check --backup --stream=xbstream --databases="{}" | ssh {} "xbstream -x -C {}"'.format(
        src_db, dst_host, tmp_backup_dir
    )
    run_command(c, cmd, False)


def prepare_backup(c, tmp_backup_dir, params=" --export "):
    cmd = 'sudo xtrabackup --prepare {} --target-dir={}'.format(params, tmp_backup_dir)
    run_command(c, cmd, True)

def make_zfs_snaphost(c, zfs_opts=''):
    zfp_mount = run_command('zfs list -o name,mountpoint|grep "{}" | cut -d " " -f 1'.format(get_datadir(c, '')))
    cmd= "zfs snapshot {}@copy.$$".format(zfp_mount)
    return run_command(c, cmd, False)

def destroy_zfs_snaphost(c, src_db, zfs_opts=''):
    zfp_mount = run_command('zfs list -o name,mountpoint|grep "{}" | cut -d " " -f 1'.format(get_datadir(c, src_db)))
    cmd= "zfs destroy -t {}@copy.$$".format(zfp_mount)
    run_command(c, cmd, True)

def copy_or_move_files(c, tmpdir, src_db, dst_db, op):
    if op == "copy":
        command = 'rsync -a'
    else:
        command = 'mv'
    datadir = get_datadir(c, dst_db)
    for ext in 'ibd exp cfg'.split(' '):
        files_cmd = "sudo {} {}/{}/*.{} {}/{}/".format(
            command, tmpdir, src_db, ext, datadir, dst_db
        )
        run_command(c, files_cmd, False, True)
    run_command(c, "sudo chown -R {}.{} {}/{}".format(MYSQL_USER, MYSQL_GROUP, datadir, dst_db), False)


def _change_master_cmd(c, tmpdir):
    xtrabackup_binlog_info = run_command(c, 'cat {}/xtrabackup_binlog_info'.format(tmpdir))
    repl = xtrabackup_binlog_info.split('\t')
    if len(repl) < 2:
        print("Can't parse xtrabackup_binlog_info with {} / {}".format(xtrabackup_binlog_info, repl))
        return ''
    print("Found binlog coordinates", repl)
    change_master = 'change master to master_log_file="{}", master_log_pos={}'.format(repl[0], repl[1])
    return change_master


def mysql_set_repl(c, dst_db, change_master):
    repl_filter = 'CHANGE REPLICATION FILTER REPLICATE_WILD_DO_TABLE=("{}.%")'.format(dst_db)
    mysql_cmd = "stop slave; {}; {}; ".format(change_master, repl_filter)
    print(mysql_cmd)
    mysql_query(c, '', mysql_cmd, True, False)


def main():
    params = _read_argv()
    # Ensure connection, we will only check first DB from the list
    print("Connecting to source host...")
    src_host = params['src_host']
    dst_host = params['dst_host']
    c1 = _connect(src_host, params['src_dbs'][0])
    print("Connecting to destination host...")
    c2 = _connect(dst_host)
    dst_internal_hostname = get_hostname(c2, params['dst_internal_ip'])
    check_connection_between_hosts(c1, dst_internal_hostname)

    # perform xtrabackup or snapshot BEFORE looping thru directories
    tmp_backup_dir = create_tmp_dir(c2, params['tmpdir'])

    if params['copy'] == "xtrabackup":
        print("... making xtrabackup")
        make_backup(c1, ' '.join(params['src_dbs']), dst_internal_hostname, tmp_backup_dir)
        print("... preparing xtrabackup")
        prepare_backup(c2, tmp_backup_dir, '')
    else:
        print("Other methods are not supported yet...")
        sys.exit(1)

    for i in range(len(params['src_dbs'])):
        src_db = params['src_dbs'][i]
        if i >= len(params['dst_dbs']):
            print("--- WARNING: destination DB name not found for source DB {}, assuming the same name...".format(
                params['src_dbs'][i]))
            dst_db = params['src_dbs'][i]
        else:
            dst_db = params['dst_dbs'][i]
        print("{} -> {}".format(src_db, dst_db))
        print("... Source DB: {}, Tables: {}".format(src_db, list_innodb_tables(c1, src_db)))
        if database_exists(c2, dst_db) and not params['force']:
            print(
                "--- ERROR: DB {} exits on destination host with the following tables: {}! Skipping this db, use --force to override".format(
                    dst_db, list_innodb_tables(c2, dst_db))
            )
            continue

        # Ensure we have dst_db
        create_database(c2, dst_db)
        # Now copy schema, run it on SOURCE:
        print("... copying schema {}/{} -> {}/{}".format(src_host, src_db, dst_host, dst_db))
        copy_schema(c1, src_db, dst_internal_hostname, dst_db)
        print("Done! Copied schema to destination: {} (hostname: {}), db: {}, tables: {}".format(
            dst_host, dst_internal_hostname, dst_db, list_innodb_tables(c2, dst_db)
        ))
        print("... discarding tablespaces for {}/{}".format(src_host, dst_db))
        discard_tablespaces(c2, dst_db)
        print("... copying/moving files")
        copy_or_move_files(c2, tmp_backup_dir, src_db, dst_db, 'move')
        print("... importing tablespaces")
        import_tablespaces(c2, dst_db)
        print("Done with {}".format(dst_db))
    print("... changing master binlog name and position + repl filter")
    mysql_set_repl(c2, dst_db, _change_master_cmd(c2, tmp_backup_dir))
    print("All done!")


if __name__ == '__main__':
    main()