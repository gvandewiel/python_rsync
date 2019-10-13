"""Backup script.

Performs rsync backup from MacBook to Synology diskstation
"""
import os
import sys
from datetime import datetime
import configparser
import hashlib
import json
import subprocess as sp
import re
from wakeonlan import send_magic_packet
import logging
from .rotate import start_rotation

logging.basicConfig(
    format="%(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("")

class c:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DIM = '\033[2m'

class BackupLocation():
    def __init__(self, settings):
        # Source location and user settings (for SSH access)
        self.source_user = settings['source_user']
        self.source_host = settings['source_host']
        
        # Source hardware adress for WOL
        self.hwaddr = settings['hwaddr']
        
        # Target location and user settings (for SSH access)
        self.target_user = settings['target_user']
        self.target_host = settings['target_host']
        self.backup_root = settings['backup_root']

        # The directory containing the identifiers for previous snapshots
        self.state_dir = os.path.join(self.backup_root, 'rsync-backup')
        # Create rsync-backup folder if not exists
        if not os.path.exists(self.state_dir):
            pass
            # os.makedirs(self.state_dir)

class BackupJob():
    def __init__(self, location, settings, rsync_args):
        # Make BackupLocation object available
        self.loc = location
        rsync_args = list(rsync_args)

        # Retrieve settings
        self.source_dir = settings['source_dir']
        self.target_dir = settings['target_dir']
        # Set new backup date
        self.new_id = self.get_new_id(self.target_dir)

        # retrieve last backup date
        self.prev_id = self.get_previous_id(self.source_dir)

        # Set target for new backup
        self.subfolder = self.get_basename(self.source_dir)
        
        # Set previous backup target
        self.prev_target = self.get_previous_target(self.target_dir, self.prev_id, self.subfolder)

        # Set backup source
        self.backup_source = self.get_backup_source(self.source_dir)

        # Set backup target
        self.backup_target = self.get_backup_target(self.target_dir, self.new_id, self.subfolder)

        self.log_file = os.path.join(self.loc.backup_root, self.new_id, 'rsync-backup.log')

        if '--dry-run' in rsync_args:
            self.dry_run = True
        else:
            self.dry_run = False

        if '--force' in  rsync_args:
            self.force = True
            rsync_args.remove('--force')
        else:
            self.force = False
            
        # Store extra rsync arguments
        self.extra_rsync_args = rsync_args
        # Add rsync exclude list
        self.rsync_exclude_list = os.path.join(self.loc.backup_root, 'rsync-exclude-list.txt')


    def __create_hash__(self, source_dir):
        """Create SHA1 hash from source_dir name.

        Used to store last backup date for that specific source dir.
        """
        return hashlib.sha1(source_dir.encode('UTF-8')).hexdigest()

    def get_new_id(self, target_dir):
        """Generte new id based on current date."""
        return datetime.now().strftime('%Y-%m-%d')

    def get_previous_id(self, source_dir):
        """Calculate SHA1 hash of source_dir"""
        source_hash = self.__create_hash__(source_dir)
        #self.logger.info('    - Source hash = {}'.format(source_hash))

        # Path the state file of backup
        sf = os.path.join(self.loc.state_dir, str(source_hash))
        
        # Try to retrieve previous id from state file
        if os.path.isfile(sf):
            with open(sf, 'r') as f:
                line = f.readline()
                #self.logger.info('    - {}'.format(line.rstrip()))
        else:
            #self.logger.info(c.WARNING + '    - No statefile found' + c.ENDC)
            #self.logger.info(c.FAIL + '    - No link-dest available' + c.ENDC)
            line = ''
        return line

    def get_basename(self, source_dir):
        """Returns the basename of the source directory."""
        return os.path.basename(os.path.normpath(source_dir))

    def get_previous_target(self, target_dir, prev_id, subfolder):
        """Determine the previous backup target (to be used as link dest)."""
        return os.path.join(target_dir, prev_id, subfolder)

    def get_backup_source(self, source_dir):
        """
        Return backup source considering a possible remote host
        
        Args:
            source_dir (str): local path to source dir
        
        Returns:
            [type]: path to source dir
        """
        
        if self.loc.source_user and self.loc.source_host:
            backup_source = '{}@{}:{}'.format(self.loc.source_user, self.loc.source_host, source_dir)
        else:
            backup_source = source_dir
        return backup_source

    def get_backup_target(self, target_dir, new_id, subfolder):
        # if target_user and target_host are not blank,
        # set backup source to remote location
        backup_path = os.path.join(target_dir, new_id, subfolder)

        if self.loc.target_user and self.loc.target_host:
            backup_target = '{}@{}:{}'.format(self.loc.target_user, self.loc.target_host, backup_path)
        else:
            backup_target = backup_path
        return backup_target

    def update_state(self):
        """Retrieve last backup date for source dir."""

        self.hash = self.__create_hash__(self.source_dir)
        state_file = os.path.join(self.loc.state_dir, str(self.hash))
        with open(state_file, 'w') as f:
            f.write(self.new_id)

class Backup():
    """Backup Script.

    Run incrimental backup of a MacBook Pro to a synology diskstation
    over an SSH connection.
    """

    def __init__(self, settings_file='', extra_arguments=[] ):
        """Backup class."""
        logging.basicConfig(
            format="%(message)s",
            level=logging.INFO,
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger("")

        # Current directory of backup script
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

        self.live = False

        if settings_file != '':
            parser = configparser.ConfigParser()
            parser._interpolation = configparser.ExtendedInterpolation()
            parser.read(os.path.join(settings_file))

            # Read general settings from config file
            self.gs = BackupLocation(dict(parser['general_settings']))

            self.jobs = list()
            for job in parser.sections():
                if job != 'general_settings':
                    self.jobs.append(BackupJob(self.gs, dict(parser[job]), extra_arguments))

            # Current time
            #self.start = datetime.now().strftime('%Y-%m-%d (%H:%M:%S)')
            self.__call__()

    def __call__(self):
        # Loop over all backup sets
        for job in self.jobs:
            new_id, update = self.execute_backup(job)
            if new_id:
                self.logger.info(c.OKGREEN + c.BOLD + '  * Backup of "{}" is performed'.format(job.source_dir) + c.ENDC)
                if job.dry_run:
                    self.logger.info(c.WARNING + c.BOLD + '  * "--dry-run" detected, no update of symlink.' + c.ENDC)
                elif update:
                    self.logger.info(c.OKBLUE + c.BOLD + '  * Update symlink of link-dest' + c.ENDC)
                    self.update_symlink(job)
            else:
                self.logger.info(c.WARNING + c.BOLD + '  * No Backup of "{}" is performed'.format(job.source_dir) + c.ENDC)

        if self.live and self.gs.source_host and self.gs.source_user:
            self.send_message(title="Remote backup", subtitle="Finished", message="All backup tasks have finished")

    def execute_backup(self, job):
        """Do the actual backup routine."""
        self.logger.info(c.OKBLUE + c.BOLD + '  * Checking backup of' + c.ENDC)
        self.logger.info('\tSource:\t{}'.format(job.source_dir) + c.ENDC)
        self.logger.info('\tTarget:\t{}'.format(job.target_dir) + c.ENDC)
        
        # Start backup if not performed today
        if job.new_id != job.prev_id or job.force:
            """Check if server is live"""
            self.logger.info(c.OKBLUE + c.BOLD + '  * Checking if remote source is available' + c.ENDC)
            
            if self.__RemoteServerCheck__(job):
                # Start rsync backup
                self.rsync(job)

                # Update current directory
                if job.dry_run:
                    self.logger.info(c.WARNING + c.BOLD + '  * "--dry-run" detected, no update of statefile.' + c.ENDC)
                else:
                    self.logger.info(c.OKGREEN + c.BOLD + '  * Finished rsync job, updating statefile' + c.END)
                    job.update_state()
                    self.logger.info(c.OKBLUE + c.BOLD + '  * Updated statefile with hash "{}" to {}'.format(job.hash, job.new_id) + c.ENDC)
                
                new_id = job.new_id
                update = True
            else:
                self.logger.info(c.FAIL + c.BOLD + '  * RemoteServerCheck failed, skipping...\n' + c.ENDC)
                new_id = False
                update = False
        else:
            # No backup performed
            self.logger.info(c.FAIL + c.BOLD + '  * Backup is already perfomed today, skipping...\n' + c.ENDC)
            new_id = False
            update = False

        return new_id, update

    def send_message(self, title, subtitle, message):
        ssh_server = '{}@{}'.format(self.source_user, self.source_host)
        remote_cmd = "osascript -e 'display notification \"{message}\" with title \"{title} ({now})\" subtitle \"{subtitle}\"'".format(title=title, subtitle=subtitle, message=message, now=datetime.now().strftime('%d-%m-%Y %H:%M'))
        ssh_cmd = ['ssh', ssh_server, remote_cmd]
        sp.check_output(ssh_cmd)

    def prep_rsync(self, job):
        """Create new subfolder."""
        self.logger.info(c.OKBLUE + c.BOLD + '  * Creating backup target folder' + c.ENDC)
        if job.loc.target_host and job.loc.target_user:
            # Remote target
            new_dir = '{}@{}:{}'.format(job.loc.target_user, job.loc.target_host, os.path.join(job.target_dir, job.new_id))
            sp.Popen(['rsync', '--quiet', '/dev/null', new_dir])
        else:
            # Local target
            new_dir = '{}'.format(os.path.join(job.target_dir, job.new_id))
            if not os.path.exists(new_dir):
                os.mkdir(new_dir)

    def update_symlink(self, job):
        self.logger.info(c.OKBLUE + c.BOLD + '  * Creating symlink "current" directory' + c.ENDC)
        src = os.path.join(job.loc.backup_root, job.new_id)
        dst = os.path.join(job.loc.backup_root, 'current')
        try:
            os.unlink(dst)
            os.symlink(src, dst)
            self.logger.info(c.OKGREEN + c.BOLD + "    - Symlink created" + c.ENDC)
        except:
            os.symlink(src, dst)
            self.logger.info(c.OKGREEN + c.BOLD + "    - Symlink created" + c.ENDC)

    #def __check_ssh__(self, host='', username='', remote_dir=''):
    def __RemoteServerCheck__(self, job):
        """Check if server is live"""
        def __AvailabilityTest__(host, hwaddr):
            """Check server status.
            Checks if server is available by sending a ping.
            If the response is false, upto 5 WOL commands will be send.
            """
            status, result = sp.getstatusoutput("ping -W2 -c1 " + str(host))
            if status != 0:
                for cnt in range(0,5):
                    self.logger.info(c.FAIL + '    - Trying to wake remote host' + c.ENDC)
                    send_magic_packet(str(hwaddr))
                    status,result = sp.getstatusoutput("ping -W10 c-1 " + str(host))
                    if status == 0:
                        break
            elif status == 0:
                self.logger.info(c.OKGREEN + c.BOLD + '      * Server is available...' + c.ENDC)
                return True
            else:
                self.logger.info(c.FAIL + '      - Server seems down' + c.ENDC)
                return False

        def __AccessTest__(host, username):
            ssh_server = '{}@{}'.format(username, host)
            ssh_cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', ssh_server, 'echo True']
            test = sp.Popen(ssh_cmd, stdout=sp.PIPE).communicate()[0]
            if test:
                self.logger.info(c.OKGREEN + c.BOLD + '      - SSH connection established' + c.ENDC)
                return True
            else:
                self.logger.info(c.FAIL + '      - SSH connection could not be established' + c.ENDC)
                return False

        def __RemoteDirTest__(host, username, remote_dir):
            # Set default return value
            _ret = False
            
            # Check remote dir
            ssh_server = '{}@{}'.format(username, host)
            ssh_cmd = ['ssh', ssh_server, '[ ! -d \'{}\' ]'.format(remote_dir)]
            ssh = sp.Popen(ssh_cmd, stdout=sp.PIPE)
            ssh.communicate()[0]
            _ret = ssh.returncode == 1
            if _ret:
                self.logger.info(c.OKGREEN + '    - Directory "{}" exists'.format(remote_dir) + c.ENDC)
            else:
                self.logger.info(c.FAIL + '    - Directory "{}" does not exists'.format(remote_dir) + c.ENDC)
            return _ret

        # Retrieve required values from job object
        host = job.loc.source_host
        username = job.loc.source_user
        hwaddr = job.loc.hwaddr
        remote_dir = job.source_dir

        # Set default return value
        _ret = False

        if __AvailabilityTest__(host=host, hwaddr=hwaddr):
            if __AccessTest__(host=host, username=username):
                if __RemoteDirTest__(host=host, username=username, remote_dir=remote_dir):
                    _ret = True
        return _ret

    #def rsync(self, prev_id, new_id, subfolder, prev_target, backup_source, backup_target):
    def rsync(self, job):
        rsync_cmd = [
            "rsync",
            "--recursive",
            "--links",
            "--times",
            "--itemize-changes",
            "--devices",
            "--specials",
            "--delete",
            "--human-readable",
            "--delete-excluded",
            "--ignore-existing",
            "--progress",
            "--stats"
        ]
        _rsync_cmd = rsync_cmd.copy()
        
        # Add exclude list to arguments
        rsync_cmd.append("--exclude-from={}".format(job.rsync_exclude_list))
        _rsync_cmd.append("--exclude-from={}".format(job.rsync_exclude_list))

        # Add extra arguments to arguments list
        rsync_cmd.extend(job.extra_rsync_args)
        _rsync_cmd.extend(job.extra_rsync_args)
        if not job.dry_run:
            _rsync_cmd.append("--dry-run")

        # Add link destination to arguments
        rsync_cmd.append("--link-dest={}".format(job.prev_target))
        _rsync_cmd.append("--link-dest={}".format(job.prev_target))

        # Add backup source
        rsync_cmd.append(job.backup_source)
        _rsync_cmd.append(job.backup_source)

        # Add backup target
        rsync_cmd.append(job.backup_target)
        _rsync_cmd.append(job.backup_target)

        self.prep_rsync(job)

        self.logger.info(c.HEADER + c.BOLD + '  * Backup configuration:' + c.ENDC)
        self.logger.info('    - Source Directory   : {}'.format(job.backup_source))
        self.logger.info('    - Target Directory   : {}'.format(job.backup_target))
        self.logger.info('    - Previous Directory : {}'.format(job.prev_target))
        self.logger.info('    - Previous snapshot  : {}'.format(job.prev_id))
        self.logger.info('    - New snapshot       : {}'.format(job.new_id))
        self.logger.info('    - Snapshot subfolder : {}'.format(job.subfolder))
        self.logger.info('    - Extra rsync options: {}'.format(job.extra_rsync_args))
        self.logger.info(c.HEADER + c.BOLD + '  * Running rsync with:' + c.ENDC)
        for arg in rsync_cmd[1:]:
        	self.logger.info('        {}'.format(arg))

        # Start --dry-run for progress
        self.logger.info('Determine total files...')
        out, err = sp.Popen(_rsync_cmd, stdout=sp.PIPE, universal_newlines=False).communicate()
        mn = re.compile(r'Number of files: (\d+)').findall(out.decode('utf-8'))
        total_files = int(mn[0].replace(',',''))
        self.logger.info('Number of files: ' + str(total_files))

        # Start the actual backup
        # Send message to the osx notifaction centre
        # self.send_message(title="Remote backup", subtitle=job.subfolder, message="Starting backup...")

        # Start backup...
        self.logger.info('Starting actual backup...')
        with sp.Popen(rsync_cmd, stdout=sp.PIPE, bufsize=1, universal_newlines=False) as p:
            self.logger.info('Processing output...')
            for line in p.stdout:
                line = line.decode('utf-8')
                if 'ir-chk' in line:
                    m = re.findall(r'ir-chk=(\d+)/(\d+)', line)
                    progress = (1 * (int(m[0][1]) - int(m[0][0]))) / total_files
                    json.dumps({ "complete": progress })
                elif 'to-check' in line:
                    m = re.findall(r'to-check=(\d+)/(\d+)', line)
                    progress = (1 * (int(m[0][1]) - int(m[0][0]))) / total_files
                    json.dumps({ "complete": progress })
                else:
                    sys.stdout.write(line)
        
        self.logger.info(c.OKGREEN + c.BOLD + '  * Rsync finished' + c.ENDC)
        
        if p.returncode != 0:
            self.logger.info(c.FAIL + c.BOLD + '  * Rsync exited with errorcode {}'.format(p.returncode) + c.ENDC)
            raise sp.CalledProcessError(p.returncode, p.args)
            job.log_file.close()


if __name__ == '__main__':
    try:
        Backup(settings_file=sys.argv[1], extra_arguments=sys.argv[2:])
    except:
        Backup(settings_file=sys.argv[1], extra_arguments=[])
