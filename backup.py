import os
import sys
from datetime import datetime
import configparser
import hashlib
import subprocess
from pprint import pprint

class Backup():
    """Backup Script.
    Run incrimental backup of a MacBook Pro to a synology diskstation
    over an SSH connection.
    """

    def __init__(self, settings_file='', extra_arguments=[]):
        # Check if a settings file is provided
        fp = os.path.dirname(os.path.abspath(__file__))
        print("#### CURRENT DIR = {}".format(fp))

        if settings_file != '':
            self.settings = configparser.ConfigParser()
            self.settings._interpolation = configparser.ExtendedInterpolation()
            self.settings.read(os.path.join(fp, settings_file))

            # Store extra rsync arguments
            self.extra_arguments = extra_arguments

            # Base dir of backup script
            # Current directory of backup script
            self.base_dir = os.getcwd()

            # Current time
            self.start = datetime.now().strftime('%Y-%m-%d (%H:%M:%S)')

            # The directory containing the identifiers for previous snapshots
            self.state_dir = os.path.join(self.base_dir, 'rsync-backup')

            # Exclude certain files defined in a exclude list
            self.rsync_exclude_list = '/volume1/Backup/rsync-exclude-list.txt'

            # Create rsync-backup folder if not exists
            if not os.path.exists(self.state_dir):
                os.makedirs(self.state_dir)

            self.start_backups()

    def start_backups(self):
        settings = self.settings

        # Get general backup settings (SSH settings, log files)
        self.source_user = settings.get('general_settings', 'source_user')
        self.source_host = settings.get('general_settings', 'source_host')

        self.target_user = settings.get('general_settings', 'target_user')
        self.target_host = settings.get('general_settings', 'target_host')

        self.log_file = settings.get('general_settings', 'log_file')
        self.errlog_file = settings.get('general_settings', 'errlog_file')

        self.logfile = open(self.log_file, 'w')
        self.errlogfile = open(self.errlog_file, 'w')

        print('Backing up the following configurations:')
        for section in settings.sections():
            if section != 'general_settings':
                print('  - {} to {}'.format(settings.get(section, 'source_dir'), settings.get(section, 'target_dir')))

        # Loop over all backup sets
        for section in settings.sections():
            if section != 'general_settings':
                print('Starting backup of: {} to {}'.format(settings.get(section, 'source_dir'), settings.get(section, 'target_dir')))
                self.backup(section)

        self.logfile.close()
        self.errlogfile.close()

    def backup(self, section):
        source_dir = self.settings.get(section, 'source_dir')
        target_dir = self.settings.get(section, 'target_dir')

        # Check if a SSH connection is possible and the
        # provided directory is accesible, returns ssh object
        ssh = self.__check_ssh__(host=self.source_host, username=self.source_user, remote_dir=source_dir)

        # retrieve last backup date
        prev_id = self.get_previous_id(source_dir)

        # Set new backup date
        new_id = self.get_new_id()

        # Set target for new backup
        subfolder = self.get_basename(source_dir)

        # Set previous backup target
        prev_target = self.get_previous_target(target_dir, prev_id, subfolder)

        # Set backup source
        backup_source = self.get_backup_source(source_dir)

        # Set backup target
        backup_target = self.get_backup_target(target_dir, new_id, subfolder)

        # Check modification date of log file (if file exists)
        log_date = self.get_log_date()

        # Start backup
        self.start_rsync(prev_id, new_id, subfolder, prev_target, backup_source, backup_target)

    def get_previous_id(self, source_dir):
        """Retrieve last backup date for source dir."""
        print('  * Checking for last backup date')
        source_hash = self.__create_hash__(source_dir)
        print('    - Source hash = {}'.format(source_hash))

        state_file = os.path.join(self.state_dir, str(source_hash))
        if os.path.isfile(state_file):
            with open(state_file, 'r') as f:
                print('    - {}'.format(f.readline()))
                return f.readline()
        else:
            print('    - No statefile found')
            print('    - {}*'.format(self.get_new_id()))
            return self.get_new_id()

    def get_new_id(self):
        """Generte new id based on current date."""
        return datetime.now().strftime('%Y-%m-%d')

    def __create_hash__(self, source_dir):
        """Create SHA1 hash from source_dir name.

        Used to store last backup date for that specific source dir.
        """
        return hashlib.sha1(source_dir.encode('UTF-8')).hexdigest()

    def get_previous_target(self, target_dir, prev_id, subfolder):
        """Determine the previous backup target (to be used as link dest)."""
        return os.path.join(target_dir, prev_id, subfolder)

    def get_basename(self, source_dir):
        """Returns the basename of the source directory."""
        return os.path.basename(os.path.normpath(source_dir))

    def get_backup_source(self, source_dir):
        """if source_user and source_host are not blank.

        set backup source to remote location
        """
        if self.source_user and self.source_host:
            backup_source = '{}@{}:{}'.format(self.source_user, self.source_host, source_dir)
        else:
            backup_source = source_dir

        return backup_source

    def get_backup_target(self, target_dir, new_id, subfolder):
        # if target_user and target_host are not blank,
        # set backup source to remote location
        backup_path = os.path.join(target_dir, new_id, subfolder)
        if self.target_user and self.target_host:
            backup_target = '{}@{}:{}'.format(self.target_user, self.target_host, backup_path)
        else:
            backup_target = backup_path

        return backup_target

    def get_log_date(self):
        # Get timestamp for last modification
        if os.path.isfile(self.log_file):
            mod_time = os.path.getmtime(self.log_file)

            # Return string in Y-m-d format
            return datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d')
        else:
            return ''

    def __check_ssh__(self, host='', username='', remote_dir=''):
        """Check is ssh connection can be made to source."""
        print('  * Checking SSH connection to remote source')
        if host and username and remote_dir:
            ssh_server = '{}@{}'.format(username, host)
            ssh_cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', ssh_server, 'echo ok']
            
            ssh = subprocess.check_output(ssh_cmd).decode('utf-8').strip()
            if ssh == 'ok':
                print('    - SSH connection established')
                ssh_cmd = ['ssh', ssh_server, '[ ! -d \'{}\' ]'.format(remote_dir)]
                ssh = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE)
                ssh.communicate()[0]
                if ssh.returncode == 1:
                    print('    - Directory "{}" exists'.format(remote_dir))
                    return True
                else:
                    print('    - Directory "{}" does not exists'.format(remote_dir))
                    return False
            else:
                print('    - SSH connection could not be established')
                return False
        else:
            print('    - No host, username or remote_dir provided')
            return False

    def start_rsync(self, prev_id, new_id, subfolder, prev_target, backup_source, backup_target):
        arguments = [
            "rsync",
            "--recursive",
            "--links",
            "--times",
            "--itemize-changes",
            "--devices",
            "--specials",
            "--delete",
            "--verbose",
            "--verbose",
            "--human-readable",
            "--delete-excluded",
            "--ignore-existing"
        ]

        # Add exclude list to arguments
        arguments.append("--exclude-from={}".format(self.rsync_exclude_list))

        # Add extra arguments to arguments list
        arguments.extend(self.extra_arguments)

        # Add link destination to arguments
        arguments.append("--link-dest={}".format(prev_target))

        # Add backup source
        arguments.append(backup_source)

        # Add backup target
        arguments.append(backup_target)

        print('  * Backup configuration:')
        print('    - Source Directory   : {}'.format(backup_source))
        print('    - Target Directory   : {}'.format(backup_target))
        print('    - Previous Directory : {}'.format(prev_target))
        print('    - Previous snapshot  : {}'.format(prev_id))
        print('    - New snapshot       : {}'.format(new_id))
        print('    - Snapshot subfolder : {}'.format(subfolder))
        print('    - Extra rsync options: {}'.format(self.extra_arguments))
        print('\n  * Running rsync with:'.format(arguments))
        pprint(arguments[1:])
        
        with subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                print(line, end='')
                self.logfile.write(line)
            for line in p.stderr:
                print(line, end='')
                self.errlogfile.write(line)

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)
            self.logfile.close()
            self.errlogfile.close()
        """"
        with open(self.log_file, 'w') as logfile:
            with open(self.errlog_file, 'w') as errlogfile:
                subprocess.Popen(arguments, stdout=logfile, stderr=errlogfile)
        """


if __name__ == '__main__':
    Backup(settings_file=sys.argv[1:2], extra_arguments=sys.argv[2:])
