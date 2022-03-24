import ftplib
import logging
import os
import datetime
from time import sleep
import pandas


def establish_connection(ftp_access):
    try:
        ftp = ftplib.FTP(ftp_access.host, ftp_access.username, ftp_access.password)
    except ftplib.all_errors:
        logging.error("Failed to establish an FTP connection to remote host: " + ftp_access.host)
        return None
    logging.info("Established an FTP connection to remote host: " + ftp_access.host)
    return ftp


def close_connection(ftp):
    logging.info("Starting to close connection to remote host")
    ftp.quit()
    logging.info("Closed connection to remote host")


def get_time_sorted_file_list(ftp):
    entries = list(ftp.mlsd())
    entries.sort(key=lambda entry: entry[1]['modify'], reverse=True)
    return entries


def get_most_recent_file(ftp):
    entries = get_time_sorted_file_list(ftp)
    latest_name = entries[0][0]
    print(latest_name)
    return latest_name


def read_file(filename, catalog_cols, catalog_dtypes=None, catalog_encoding=None,
              catalog_sep=None):
    try:
        logging.error('current file : {}'.format(filename))
        catalog_df = pandas.read_csv(filename, compression="gzip", dtype=catalog_dtypes,
                                     encoding=catalog_encoding, error_bad_lines=False,
                                     sep=catalog_sep, usecols=catalog_cols)
    except IOError as e:
        logging.error("I/O error({0}): {1}".format(e.errno, e.message))
        logging.error("Failed to open file.")
        return None
    except pandas.errors.EmptyDataError:
        logging.error("Failed to read data from file.")
        return None
    except ValueError:
        logging.error("Failed to convert value to a new type in file.")
        return None
    except UnicodeEncodeError:
        logging.error("Failed to correctly parse values in file.")
        return None
    except Exception as e:
        logging.error(type(e))
        logging.error(e)
        logging.error("Failed to use file.")
        return None
    logging.debug("Read data from file.")
    return catalog_df


def download_today_file_by_filename(ftp_access, remote_filename, destination, today_ymd, debug=False):
    now = datetime.datetime.strptime(today_ymd, "%Y-%m-%d")
    max_datetime = now + datetime.timedelta(hours=24)

    today = datetime.datetime.today()
    if debug is True:
        today = now
    today_filename = '{}_{}.csv.gz'.format(remote_filename, str(now.strftime("%Y-%m-%d")))
    destination = '{}_{}.csv.gz'.format(destination, str(now.strftime('%Y-%m-%d')))

    if os.path.isfile(today_filename):
        logging.info('Found file in local directory: {}'.format(today_filename))
    else:
        logging.info('Failed to find file in local directory: {}'.format(today_filename))
        while today < max_datetime:
            ftp = establish_connection(ftp_access)
            if ftp is None:
                continue

            # Change directory on remote host if need be
            if not change_directory(ftp, ftp_access.directory):
                close_connection(ftp)
                continue

            # Download today's file
            if download_file(ftp, destination, today_filename):
                close_connection(ftp)
                break
            close_connection(ftp)
            # sleep(900)
        else:
            logging.error("Aborting: Failed to download file from remote host in time: {}".format(today_filename))
            return False

    columns = ['EAN', 'Sku', 'Titre', 'PrixVenteGU', 'prixAchatGU', 'NbVentesFnacCom_OneMonth',
               'DistributeurNewref', 'LibelleDispo', 'PrixClient', 'CodePromotion',
               'DateFinPromotion', 'PrixAdherent', 'Editeur', ]
    catalog_df = read_file(destination, columns, object, "cp1252", "|")
    if catalog_df is None:
        return False
    return catalog_df


def check_entry_date(ftp):
    entries = get_time_sorted_file_list(ftp)
    todays_date = datetime.datetime.now().strftime('%Y%m%d')
    entry_date = entries[0][1]['modify'][:8]
    if todays_date != entry_date:
        return False
    return entries[0][0]


def get_todays_file(ftp):
    filename = check_entry_date(ftp)
    retry = 0
    while filename is False and retry < 10:
        filename = check_entry_date(ftp)
        sleep(120)
        retry += 1
    if retry < 10:
        return filename
    return False


def change_directory(ftp, directory):
    # Do nothing if no change of directory is required
    if directory is None:
        return True
    try:
        ftp.cwd(directory)
    except ftplib.all_errors:
        logging.error("Failed to change directory on remote host: " + directory)
        return False
    logging.info("Changed directory on remote host: " + directory)
    return True


def download_file(ftp, local_file, remote_file):
    try:
        f = open(local_file, "wb")
    except IOError:
        logging.error("Failed to open file: " + local_file)
        return False
    with f:
        try:
            ftp.retrbinary("RETR " + remote_file, f.write)
        except ftplib.all_errors:
            logging.error("Failed to download file from remote host: " + remote_file)
            return False
        logging.info("Downloaded file from remote host: " + remote_file)
        return True


def download_single_file(ftp_access, local_file, catalog_filename):
    ftp = establish_connection(ftp_access)
    if ftp is None:
        logging.error("FTP is None")
    # Change directory on remote host
    if not change_directory(ftp, ftp_access.directory):
        close_connection(ftp)
        logging.error("Couldn't change directory")
    # Download today's file
    if download_file(ftp, local_file, catalog_filename):
        close_connection(ftp)
        return True
    return False


def upload(ftp_access, uploaded_fullpath):
    # Establish connection to remote host
    ftp = establish_connection(ftp_access)
    if ftp is None:
        return False
    # Change directory on remote host if need be
    if not change_directory(ftp, ftp_access.directory):
        close_connection(ftp)
        return False
    # open file
    try:
        f = open(uploaded_fullpath, "rb")
    except IOError:
        logging.error("Failed to open file: " + uploaded_fullpath)
        close_connection(ftp)
        return False
    # Upload file
    with f:
        try:
            ftp.storbinary("STOR {}".format(os.path.basename(uploaded_fullpath)), f)
        except ftplib.all_errors as e:
            logging.error("Failed to upload file to remote host: {}".format(uploaded_fullpath))
            logging.error("Failed error: ".format(str(e)))
            close_connection(ftp)
            return False
        logging.info("Uploaded file to remote host: {}".format(uploaded_fullpath))
    close_connection(ftp)
    return True
