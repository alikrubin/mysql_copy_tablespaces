#!/usr/bin/env python

from fabric import Connection
import argparse
import glob
import os
import pwd
import socket
import sys

# MySQL user/group
MYSQL_USER = "mysql"
MYSQL_GROUP = "mysql"
SSH_OPTS_NOTTY = '-q -o PasswordAuthentication=no -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'
SSH_OPTS = '-tt %s' % SSH_OPTS_NOTTY
SUDO_STR = ''


def _read_argv():
    ### Check command line parameters
    res = dict()
    parser = argparse.ArgumentParser(description='mysql_copy_dbs.py - command line options')
    parser.add_argument('src', default='', help='Source DB in this format: [user@]host/dbname')
    parser.add_argument('dst', default='', help='Destination DB in this format: [user@]host/dbname')
    parser.add_argument('--tmpdir', default='/tmp', help='(Optional) TMP dir on the destination server', required=False)
    parser.add_argument('--copy', default='xtrabackup', help='(Optional) TODO: Method to copy data, default xtrabackup, other options: zfs, lvm (snapshots)', required=False)
    parser.add_argument('--dst_internal_ip',
                        help='(Optional) Internal IP of the destination server (for copying xtrabackup)')
    parser.add_argument('--force', help='(Optional) Force overwriting DB', required=False,
                        action='store_true')
    parser.add_argument('--skip-sudo', dest='sudo', help='Skip sudo privileges',
                        action='store_false')
    args = parser.parse_args()
    src = args.src.split('/')
    dst = args.dst.split('/')

    # print (src, dst)
    res.update({'tmpdir': args.tmpdir, 'force': args.force, 'src_host': src[0], 'dst_host': dst[0],
                'dst_internal_ip': args.dst_internal_ip, 'copy': args.copy, 'sudo': args.sudo})

    mysql_owner = None

    try:
        user, host = src[0].split('@')
        mysql_owner = user
        res['src_host_ip'] = '%s@%s' % (user, socket.gethostbyname(host))
    except ValueError, err:
        res['src_host_ip'] = socket.gethostbyname(src[0])

    try:
        user, host = dst[0].split('@')
        mysql_owner = user
        host = socket.gethostbyname(host)
        res['dst_host_ip'] = '%s@%s' % (user, host)
        res['dst_internal_ip'] = host
    except ValueError, err:
        res['dst_host_ip'] = socket.gethostbyname(dst[0])

    if args.dst_internal_ip is not None:
        # make sure its really an IP
        try:
            res['dst_internal_ip'] = socket.gethostbyname(args.dst_internal_ip)
        except ValueError, err:
            parser.error('Invalid value for dst_internal_ip')

    # We override default mysql/mysql user group ownership on destination server
    # especially if user on dest is not root and not mysql. Instead we assume
    # its the user we use to SSH, if its empty the user from the src, else the user
    # who executed this script
    if mysql_owner is None and not args.sudo:
        mysql_owner = pwd.getpwuid(os.getuid())[0]

    if mysql_owner == 'root':
        mysql_owner = 'mysql'

    res['mysql_uid'] = mysql_owner
    res['mysql_gid'] = mysql_owner

    if len(src) == 1:
        parser.error("ERROR: Need to provide a list of databases in the format db1[,db2,db3]")
    else:
        res.update({'src_dbs': src[1].split(',')})
    if len(dst) == 1:
        print("No destination db list, assuming same list as source...")
        res.update({'dst_dbs': src[1].split(',')})
    else:
        res.update({'dst_dbs': dst[1].split(',')})

    if len(res['dst_dbs']) > len(res['src_dbs']):
        parser.error('There is more destination databases than the number of sources')

    res['db_maps'] = dict()
    for i in range(len(res['src_dbs'])):
        src_db = res['src_dbs'][i]
        if i >= len(res['dst_dbs']):
            print("--- WARNING: destination DB name not found for source DB {}, assuming the same name...".format(
                src_db))
            res['db_maps'][src_db] = src_db
        else:
            res['db_maps'][src_db] = res['dst_dbs'][i]

    #print(res)
    return res


def _connect(host, db=''):
    # Check connection first
    c = Connection(host, connect_timeout=2)
    hostname = get_hostname(c)
    print("Connection established to {}. Hostname reported: {}".format(host, hostname))
    # Check that MySQL is running and we can connect to it
    print("MySQL is running, version: {}".format(_run_mysql_query(c, db, "select version()")))
    return c


def run_command(c, cmd, hide=True, ignore=False, timeout=None):
    try:
        if SUDO_STR == '':
            pty=False
        else:
            pty=True

        res = c.run(cmd, hide=hide, pty=pty, timeout=timeout)
    except Exception as e:
        print("[ERROR] Can't run command {}, err: {}".format(cmd, str(e)))
        print("Please make sure you can sudo or use root AND you can connect to MySQL from root shell without password")
        if ignore:
            return "Error"
        else:
            sys.exit(1)
    return res.stdout.strip()


def check_connection_between_hosts(c, dst_host):
    cmd = 'ssh {} {} "{} hostname"'.format(SSH_OPTS, dst_host, SUDO_STR)
    try:
        res = c.run(cmd, hide=False)
    except Exception as e:
        print(
            "[ERROR] Can't connect to ssh between hosts, make sure you can connect from source host to {} host, err: {}".format(
                dst_host, str(e)))
        sys.exit(1)


def check_src_dbs_fulltext(c, srcdbs):
    sql = ('select count(1) as ns from {0}.innodb_sys_tables t '
           'where t.name LIKE "{1}/FTS_%"')
    db = 'information_schema'
    for srcdb in srcdbs:
        #print(sql.format(db, srcdb))
        res = mysql_query(c, 'information_schema', sql.format(db, srcdb))
        if type(res) is list:
            res = int(res[0])
            if res > 0:
                raise Exception('ERROR: Source db %s has FULLTEXT indexes, this operation is not supported' % srcdb)
        else:
            raise Exception('ERROR: Unable to validate source db %s' % srcdb)


def check_src_dbs_partitions(c, srcdbs):
    sql = ('select count(1) as ns from {0}.partitions p '
           'where p.table_schema = "{1}" and partition_name is not null')
    db = 'information_schema'
    for srcdb in srcdbs:
        #print(sql.format(db, srcdb))
        res = mysql_query(c, 'information_schema', sql.format(db, srcdb))
        if type(res) is list:
            res = int(res[0])
            if res > 0:
                raise Exception('ERROR: Source db %s has partitioned tables, this operation is not supported' % srcdb)
        else:
            raise Exception('ERROR: Unable to validate source db %s' % srcdb)


def get_hostname(c, dst_internal_ip=''):
    if dst_internal_ip:
        return dst_internal_ip

    # getting hostname should not take more than two seconds
    # this allows us to capture for example when a user on the other
    # end has not configured passwordless sudo which gets stuck in
    # password capture
    return run_command(c, '%suname -n' % SUDO_STR, timeout=2)


def sudo_string(sudo=False):
    if sudo:
        return 'sudo '

    return ''


def _run_mysql_query(c, db, sql, hide=True):
    if hide:
        mysql_verbose = ""
    else:
        mysql_verbose = " -vvv "
    return run_command(c, "{}mysql {} -A -NB {} -e '{}'".format(SUDO_STR, mysql_verbose, db, sql), hide=hide)


def mysql_query(c, db, sql, hide=True):
    return _run_mysql_query(c, db, sql, hide).split('\n')


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
    return mysql_query(c, db, q, False)


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
    cmd = "{}mysqldump --no-data --triggers --single-transaction --set-gtid-purged=OFF --skip-add-locks {} | ssh {} {} {}mysql {}".format(
        SUDO_STR, src_db, SSH_OPTS_NOTTY, dst_host, SUDO_STR, dst_db
    )
    run_command(c, cmd)


def create_tmp_dir(c, prefix='/tmp'):
    #if not tmpdir or tmpdir == '':
    tmp_backup_dir = run_command(c, "{}mktemp -d -t mycpdb.XXXX -p {}".format(SUDO_STR, prefix))
    run_command(c, '{}chmod 0750 {}'.format(SUDO_STR, tmp_backup_dir))
    print("Created TMPDIR={}".format(tmp_backup_dir))
    #else:
    #    # TODO: check if it exists already
    #    tmp_backup_dir = tmpdir
    return tmp_backup_dir


def make_backup(c, src_db, dst_host, tmp_backup_dir):
    # Even with sudo we force --defaults-file ~/.my.cnf
    # this means reading the .my.cnf of the non-root user 
    # which we are using anyway, for non sudo is the same
    # if we are using the root account, has the same effect
    # SO we need to make sure ~/.my.cnf is a prerequisite
    cmd = '{}xtrabackup --defaults-file=~/.my.cnf --skip-tables-compatibility-check --backup --stream=xbstream --databases="{}" --target-dir=. | ssh {} {} "{}xbstream -x -C {}"'.format(
        SUDO_STR, src_db, SSH_OPTS_NOTTY, dst_host, SUDO_STR, tmp_backup_dir
    )
    run_command(c, cmd, False)


def prepare_backup(c, tmp_backup_dir, params=" --export ", sudo=True):
    cmd = '{}xtrabackup --prepare {} --target-dir={}'.format(SUDO_STR, params, tmp_backup_dir)
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
    datadir = get_datadir(c, dst_db)
    if op == "copy":
        command = '/bin/cp'
    else:
        command = '/bin/mv'
    # instead of looping against all extensions we can use find 
    # in cases where there is no exp and cfg mv or rsync fails
    files_cmd = ("{}find {} -type f -name *.ibd -o -name *.cfg -o -name *.exp "
                 "| {}xargs -I {{}} {} {{}} {}/").format(SUDO_STR, tmpdir, SUDO_STR, 
                                                command, os.path.join(datadir, dst_db))
    #print(files_cmd)
    run_command(c, files_cmd, False, True)
    run_command(c, "{}chown -R {}.{} {}/{}".format(SUDO_STR, MYSQL_USER, MYSQL_GROUP, datadir, dst_db), False)


def _change_master_cmd(c, tmpdir):
    xtrabackup_binlog_info = run_command(c, '{}cat {}/xtrabackup_binlog_info'.format(SUDO_STR, tmpdir))
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
    mysql_query(c, '', mysql_cmd, True)


def main():
    global SUDO_STR
    global MYSQL_USER
    global MYSQL_GROUP

    params = _read_argv()
    SUDO_STR = sudo_string(params['sudo'])
    MYSQL_USER = params['mysql_uid']
    MYSQL_GROUP = params['mysql_gid']

    # Ensure connection, we will only check first DB from the list
    src_host = params['src_host_ip']
    dst_host = params['dst_host_ip']
    print("Connecting to source host ... %s" % src_host)
    c1 = _connect(src_host)
    print("Connecting to destination host ... %s" % dst_host)
    c2 = _connect(dst_host)

    # Make sure none of our src dbs has FULLTEXT indexes and partitioned tables
    check_src_dbs_partitions(c1, params['db_maps'])
    check_src_dbs_fulltext(c1, params['db_maps'])
    
    # Simply use dst_internal_ip if given, otherwise use the resolved ip from dst
    check_connection_between_hosts(c1, params['dst_internal_ip'])

    # perform xtrabackup or snapshot BEFORE looping thru directories
    tmp_backup_dir = create_tmp_dir(c2, params['tmpdir'])

    # Attempt to create databases first on dest, especially if force is not
    # specified so we can fail early
    for srcdb in params['db_maps']:
        dstdb = params['db_maps'][srcdb]
        print("Creating {} -> {}".format(srcdb, dstdb))
        print("... Source DB: {}, Tables: {}".format(srcdb, list_innodb_tables(c1, srcdb)))
        if database_exists(c2, dstdb):
            if not params['force']:
                raise Exception(("--- ERROR: DB {} exists on destination host with the following "
                       "tables: {}! Aborting, use --force to override").format(
                            dstdb, list_innodb_tables(c2, dstdb))
                )
            else:
                print('--- WARNING: --force was specified, dropping existing destination tables')
                dsttbls = list_innodb_tables(c1, dstdb)
                for tbl in dsttbls:
                    mysql_query(c2, dstdb, "DROP TABLE IF EXISTS {}".format(tbl))

    if params['copy'] == "xtrabackup":
        print("... making xtrabackup")
        make_backup(c1, ' '.join(params['src_dbs']), params['dst_host_ip'], tmp_backup_dir)
        print("... preparing xtrabackup")
        prepare_backup(c2, tmp_backup_dir, '')
    else:
        print("Other methods are not supported yet...")
        sys.exit(1)

    for srcdb in params['db_maps']:
        dstdb = params['db_maps'][srcdb]
        print("Importing {} -> {}".format(srcdb, dstdb))

        # Ensure we have dst_db
        create_database(c2, dstdb)
        # Now copy schema, run it on SOURCE:
        print("... copying schema {}/{} -> {}/{}".format(src_host, srcdb, dst_host, dstdb))
        copy_schema(c1, srcdb, params['dst_host_ip'], dstdb)
        print("Done! Copied schema to destination: {} (hostname: {}), db: {}, tables: {}".format(
            dst_host, params['dst_host_ip'], dstdb, list_innodb_tables(c2, dstdb)
        ))

        print("... discarding tablespaces for {}/{}".format(src_host, dstdb))
        discard_tablespaces(c2, dstdb)
        print("... copying/moving files")
        copy_or_move_files(c2, tmp_backup_dir, srcdb, dstdb, 'move')
        print("... importing tablespaces")
        import_tablespaces(c2, dstdb)
        print("Done with {}".format(dstdb))
    print("... changing master binlog name and position + repl filter")
    mysql_set_repl(c2, dstdb, _change_master_cmd(c2, tmp_backup_dir))
    print("All done!")


if __name__ == '__main__':
    main()