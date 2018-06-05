"""Backup script.

Performs rsync backup from MacBook to Synology diskstation
"""
import os
import sys
from datetime import datetime
import configparser
import hashlib
import subprocess
from wakeonlan import send_magic_packet
from . import rotate

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


class Backup():
    """Backup Script.

    Run incrimental backup of a MacBook Pro to a synology diskstation
    over an SSH connection.
    """

    def __init__(self, settings_file='', extra_arguments=[]):
        """Backup class."""
        # Base dir of backup script
        # Current directory of backup script
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.live = False
        
        if settings_file != '':
            self.settings = configparser.ConfigParser()
            self.settings._interpolation = configparser.ExtendedInterpolation()
            self.settings.read(os.path.join(settings_file))

            # Store extra rsync arguments
            self.extra_arguments = extra_arguments

            # Current time
            self.start = datetime.now().strftime('%Y-%m-%d (%H:%M:%S)')

            self.start_backups()

    def start_backups(self):
        settings = self.settings

        # Get general backup settings (SSH settings, log files)
        self.source_user = settings.get('general_settings', 'source_user')
        self.source_host = settings.get('general_settings', 'source_host')
        self.hwaddr = settings.get('general_settings', 'hwaddr')

        self.target_user = settings.get('general_settings', 'target_user')
        self.target_host = settings.get('general_settings', 'target_host')

        self.backup_root = settings.get('general_settings', 'backup_root')

        # The directory containing the identifiers for previous snapshots
        self.state_dir = os.path.join(self.backup_root, 'rsync-backup')
        # Create rsync-backup folder if not exists
        if not os.path.exists(self.state_dir):
            os.makedirs(self.state_dir)
        
        # Exclude certain files defined in a exclude list
        self.rsync_exclude_list = os.path.join(self.backup_root, 'rsync-exclude-list.txt')
        ## self.rsync_exclude_list = '/volume1/Backup/rsync-exclude-list.txt'
        
        self.log_file = os.path.join(self.backup_root, settings.get('general_settings', 'log_file'))
        self.errlog_file = os.path.join(self.backup_root, settings.get('general_settings', 'errlog_file'))
        
        self.logfile = open(self.log_file, 'a')
        self.errlogfile = open(self.errlog_file, 'a')

        # Loop over all backup sets
        for section in settings.sections():
            if section != 'general_settings':
                print(c.OKBLUE + c.BOLD + '  * Checking backup of' + c.ENDC + '\n\tSource: {}\n\tTarget:{}'.format(settings.get(section, 'source_dir'), settings.get(section, 'target_dir')))
                print(c.ENDC)
                new_id, update = self.backup(section)

        # Close log files
        self.logfile.close()
        self.errlogfile.close()

        if update:
            if '--dry-run' in self.extra_arguments:
                print('\n'+ c.WARNING + c.BOLD + '  * "--dry-run" detected, no update of symlink.' + c.ENDC)
            else:
                self.update_symlink(new_id)
        if self.live and self.source_host and self.source_user:
            self.send_message(title="Remote backup", subtitle="Finished", message="All backup tasks have finished")

    def backup(self, section):
        """Do the actual backup routine."""
        source_dir = self.settings.get(section, 'source_dir')
        target_dir = self.settings.get(section, 'target_dir')

        # retrieve last backup date
        prev_id = self.get_previous_id(source_dir)

        # Set new backup date
        new_id = self.get_new_id()

        # Start backup if not performed today
        if new_id != prev_id:

            """Check if server is live"""
            print(c.OKBLUE + c.BOLD + '  * Checking if remote source is available' + c.ENDC)
            # Check if a SSH connection is possible and the
            # provided directory is accesible, returns ssh object
            self.live = self.__check_ssh__(host=self.source_host,
                               username=self.source_user,
                               remote_dir=source_dir)

            if self.live is True:

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

                self.prep_rsync(target_dir, new_id)

                self.start_rsync(prev_id,
                                 new_id,
                                 subfolder,
                                 prev_target,
                                 backup_source,
                                 backup_target)

                # Update current directory
                if '--dry-run' in self.extra_arguments:
                    print('\n'+ c.WARNING + c.BOLD + '  * "--dry-run" detected, no update of statefile.' + c.ENDC)
                    # rotate.start_rotation(path=target_dir, dry_run=True, exclude=prev_target)
                else:
                    self.update_state(source_dir, new_id, target_dir)
                    print('\n'+ c.WARNING + c.BOLD + '  * Starting rotation of backup_target' + c.ENDC)
                    rotate.start_rotation(path=target_dir, dry_run=False, exclude=prev_target)

                new = True
        else:
            # No backup performed
            print(c.FAIL + c.BOLD + '  *** Backup is already perfomed today, skipping... ***\n' + c.ENDC)
            new = False

        return new_id, new

    def send_message(self, title, subtitle, message):
        ssh_server = '{}@{}'.format(self.source_user, self.source_host)
        remote_cmd = "osascript -e 'display notification \"{message}\" with title \"{title} ({now})\" subtitle \"{subtitle}\"'".format(title=title, subtitle=subtitle, message=message, now=datetime.now().strftime('%d-%m-%Y %H:%M'))
        ssh_cmd = ['ssh', ssh_server, remote_cmd]
        subprocess.check_output(ssh_cmd)

    def prep_rsync(self, target_dir, new_id):
        """Create new subfolder."""
        if self.target_host and self.target_user:
            # Remote target
            new_dir = '{}@{}:{}'.format(self.target_user, self.target_host, os.path.join(target_dir,new_id))
            subprocess.Popen(['rsync', '--quiet', '/dev/null', new_dir])
        else:
            # Local target
            new_dir = '{}'.format(os.path.join(target_dir,new_id))
            if not os.path.exists(new_dir):
                os.mkdir(new_dir)

    def update_symlink(self, new_id):
        print(c.OKBLUE + c.BOLD + '  * Creating symlink "current" directory' + c.ENDC)
        src = os.path.join(self.backup_root, new_id)
        dst = os.path.join(self.backup_root, 'current')
        try:
            os.unlink(dst)
            os.symlink(src, dst)
            print(c.OKGREEN + c.BOLD + "    - Symlink created" + c.ENDC)
        except:
            os.symlink(src, dst)
            print(c.OKGREEN + c.BOLD + "    - Symlink created" + c.ENDC)

    def update_state(self, source_dir, new_id, target_dir):
        """Retrieve last backup date for source dir."""

        source_hash = self.__create_hash__(source_dir)
        print('\n'+c.OKBLUE + c.BOLD + '  * Updating statefile with hash "{}" to {}'.format(source_hash, new_id))
        print(c.ENDC)

        state_file = os.path.join(self.state_dir, str(source_hash))
        with open(state_file, 'w') as f:
            f.write(new_id)

    def get_previous_id(self, source_dir):
        """Retrieve last backup date for source dir."""
        print(c.OKBLUE + c.BOLD + '  * Checking for last backup date' + c.ENDC)
        source_hash = self.__create_hash__(source_dir)
        print('    - Source hash = {}'.format(source_hash))

        state_file = os.path.join(self.state_dir, str(source_hash))
        if os.path.isfile(state_file):
            with open(state_file, 'r') as f:
                line = f.readline()
                print('    - {}'.format(line))
                return line
        else:
            print(c.WARNING + '    - No statefile found' + c.ENDC)
            print(c.FAIL + '    - No link-dest available' + c.ENDC)
            return ''

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
        """Check if server is live"""
        live = self.__ipcheck__(host, self.hwaddr)
        """Check is ssh connection can be made to source."""
        print(c.OKBLUE + c.BOLD + '  * Checking SSH connection to remote source' + c.ENDC)
        if host and username and remote_dir and live:
            ssh_server = '{}@{}'.format(username, host)
            ssh_cmd = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', ssh_server, 'echo ok']

            ssh = subprocess.check_output(ssh_cmd).decode('utf-8').strip()
            if ssh == 'ok':
                print(c.OKGREEN + '    - SSH connection established' + c.ENDC)
                ssh_cmd = ['ssh', ssh_server, '[ ! -d \'{}\' ]'.format(remote_dir)]
                ssh = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE)
                ssh.communicate()[0]
                if ssh.returncode == 1:
                    print(c.OKGREEN + '    - Directory "{}" exists'.format(remote_dir))
                    print(c.ENDC)
                    return True
                else:
                    print(c.FAIL + '    - Directory "{}" does not exists'.format(remote_dir))
                    print(c.ENDC)
                    return False
            else:
                print(c.FAIL + '    - SSH connection could not be established' + c.ENDC)
                return False
        else:
            print(c.FAIL + '    - No host, username or remote_dir provided' + c.ENDC)
            return False

    def __ipcheck__(self, host, hwaddr):
        """Check server status.
        Checks if server is available by sending a ping.
        If the response is false, upto 5 WOL commands will be send.
        """
        status,result = subprocess.getstatusoutput("ping -w2 " + str(host))
        if status != 0:
            for cnt in range(0,5):
                print(c.FAIL + '    - Trying to wake remote host' + c.ENDC)
                send_magic_packet(str(hwaddr))
                status,result = subprocess.getstatusoutput("ping -w10 " + str(host))
                if status == 0:
                    break

        if status == 0:
            return True
        else:
            print(c.FAIL + '    - Server seems down' + c.ENDC)
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
            "--human-readable",
            "--delete-excluded",
            "--ignore-existing",
            "--stats"
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

        print(c.HEADER + c.BOLD + '  * Backup configuration:' + c.ENDC)
        print('    - Source Directory   : {}'.format(backup_source))
        print('    - Target Directory   : {}'.format(backup_target))
        print('    - Previous Directory : {}'.format(prev_target))
        print('    - Previous snapshot  : {}'.format(prev_id))
        print('    - New snapshot       : {}'.format(new_id))
        print('    - Snapshot subfolder : {}'.format(subfolder))
        print('    - Extra rsync options: {}'.format(self.extra_arguments))
        print('')
        print(c.HEADER + c.BOLD + '  * Running rsync with:' + c.ENDC)
        for arg in arguments[1:]:
        	print('    {}'.format(arg))

        self.logfile.seek(0, 0)
        self.logfile.truncate

        self.errlogfile.seek(0, 0)
        self.errlogfile.truncate

        # Start the actual backup
        # Send message to the osx notifaction centre
        self.send_message(title="Remote backup", subtitle=subfolder, message="Starting backup...")

        # Start backup...
        with subprocess.Popen(arguments,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              bufsize=1,
                              universal_newlines=True) as p:

            for line in p.stdout:
                if line.startswith('>'):
                    print(c.OKGREEN + line + c.ENDC, end='')
                elif not line.startswith(['h','.','[sender]']):
                    print(line, end='')

                self.logfile.write(line)
            for line in p.stderr:
                print(c.FAIL + line + c.ENDC, end='')
                self.errlogfile.write(line)

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)
            self.logfile.close()
            self.errlogfile.close()


if __name__ == '__main__':
    try:
        Backup(settings_file=sys.argv[1], extra_arguments=sys.argv[2:])
    except:
        Backup(settings_file=sys.argv[1], extra_arguments=[])
