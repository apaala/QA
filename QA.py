"""
QA.py - Perform QA by checking for consistency in filenames and md5sums to those in manifest.

Author - Apaala Chatterjee (apaala.chatterjee@som.umaryland.edu)

Steps:
1. Check Done file
2. Check file names
3. Check file md5sums
4. Check if all required files for user prompted technique are present.
5. ***** rename?
"""

import getopt, sys, os
import argparse
import os
import pandas as pd
import logging
import hashlib
from pathlib import Path
logger = logging.getLogger('app.' + __name__)



def main():
    parser = argparse.ArgumentParser( description='User inputs to QA script')
    parser.add_argument("-d", "--dir_path", dest="dir_path",help="Path to directory with files to be assessed", metavar="PATH")
    parser.add_argument("-m", "--manifest", dest="manifest_path",help="Full path to manifest file", metavar="FILE")
    parser.add_argument("-t", "--technique", dest="technique",help="Technique to check files for")
    options = parser.parse_args()

    #Read manifest file
    manifest = pd.read_csv(options.manifest_path, sep="\t")
    print(len(manifest))

    #List all files in the directory provided
    all_files = os.listdir(options.dir_path)
    print(len(all_files))

    #Get matched and unmatched file names. Error if file in manifest not present in directory.
    matched_files, unmatched_files= check_dir_vs_manifest(all_files, manifest)
    
    #Tests
    print(len(matched_files))
    print("-----")
    print(len(unmatched_files))

    #generate md5sums
    
    #add logic to not get here if file names dont match
    md5sums_df = pd.DataFrame({"full_path": manifest.filename,"manifest_filename": manifest.filename,"manifest_checksum": manifest.checksum, "calculated_md5sum": ""})

    #md5sums_df.File = os.path.join( options.dir_path, md5sums_df.File)
    md5sums_df['full_path'] = options.dir_path + md5sums_df['full_path'].astype(str)
    print(md5sums_df)
    print("----")

    ###commented checksun checking to test technique
    ###check_md5sums = match_md5sums_to_manifest(md5sums_df)
    ###print(check_md5sums)
    #check md5checksums
    master_techniques = open_techniques_with_pathlib("QC_techniques_master.csv")
    print(master_techniques)
    file_list = get_technique_file_list(options.technique, master_techniques)
    required_files = check_tech_assoc_files(manifest, file_list, options.technique)
    # Get the aliquot and use it to find file names. pass it manifest, file_list, techniques
    #get_technique_info

    #Check required files are present

def check_tech_assoc_files(manifest, file_list, techniques):
    technique = pd.read_csv(techniques, sep=",")
    total_file_count = len(manifest.filename)
    print("total files---")
    print(total_file_count)
    for t in technique:
        tname = t['name']
        print(tname)
        aliquot = t['aliquot']
        print(aliquot)
        print("-----")
    return total_file_count

def get_technique_file_list(techniques, master):
    technique = pd.read_csv(techniques, sep=",")
    print(technique)
    file_list = master[master.technique.isin(technique.name)]
    return(technique)

def open_techniques_with_pathlib(file_name):
   script_dir = Path(__file__).resolve().parent
   file_path = script_dir / file_name
   content = pd.read_csv(file_path, sep=",")
   return content

def check_dir_vs_manifest(all_files, manifest):
    #contains_all = manifest['filename'].isin(all_files).all()
    #if contains_all == False:
    #Get all files that are present in directory and in manifest
    contains_all = [x for x in all_files if x not in manifest.filename]
    #missing_files = [x for x in manifest.filename if x not in all_files]
    #contains_all = manifest[manifest.filename.isin(all_files)]
    #Get all files that are present in directory and missing in manifest. NBD. Add to log!
    missing_files_from_manifest = list(set(all_files) - set(manifest.filename))
    if len(missing_files_from_manifest)>0:
        logger.warning("These files are not in manifest but present in the directory: %s ",missing_files_from_manifest)
    #Get all files that are present in manifest and missing in directory. Bad.
    missing_files = list(set(manifest.filename) - set(all_files))
    if len(missing_files)>0:
        logger.error("These files are in manifest but missing from the directory: %s ",missing_files)
    return(contains_all, missing_files)

def match_md5sums_to_manifest(md5sums_df):
    #calc md5sum for each file and match to corresponding column in manifest.
    logger.debug("In confirm_checksums_match().")
    print( "in checking")
    ####
    checksums_ok = False
    error_message = "does not match value provided in the manifest"

    # Compute checksum on each submitted file.
    md5sums_df['calculated_md5sum'] = md5sums_df['full_path'].apply(compute_md5)
    
    # Create mask to find mismatching observed and expected checksums.
    df_mask = (md5sums_df['calculated_md5sum'] != md5sums_df['manifest_checksum'])
    
    # Get a list of row indices where observed checksums do not match the 
    # checksum listed in the manifest.
    rows_mismatched = md5sums_df[df_mask].index.tolist()

    if len(rows_mismatched) > 0:
        # One or more submitted file's checksum does not match checksum listed
        # in manifest.

        # Create human readable error messages.
        logger.debug("In confirm_checksums_match().")

        #send_file_validation_email(errors, submission_id, submitter)
    else:
        checksums_ok = True

    return checksums_ok

def compute_md5(filepath):
    """
    Compute md5 checksum for file.
    """
    logger.info(f"Computing md5 of {filepath}")

    BLOCKSIZE = 65536
    md5 = None

    try:
        hasher = hashlib.md5()
        with open(filepath, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        md5 = hasher.hexdigest()
    except Exception as err:
        logger.exception(f"Unable to compute MD5 for file {filepath} due to error.", exc_info=True)

    return md5

def confirm_checksums_match(worksheet_name, checksum_column_name, file_dataframe):
    """
    Confirms that MD5 checksums of the submitted files match the checksums
    listed in the manifest. Mismatching checksums are reported to the
    submitter.

    Returns True if all checksums match; otherwise returns False.
    """
    logger.debug("In confirm_checksums_match().")

    checksums_ok = False
    error_message = "does not match value provided in the manifest"

    # Compute checksum on each submitted file.
    file_dataframe['observed_checksums'] = file_dataframe['submitted_filepath'].apply(compute_md5)
    
    # Create mask to find mismatching observed and expected checksums.
    df_mask = (file_dataframe[checksum_column_name] != file_dataframe['observed_checksums'])
    
    # Get a list of row indices where observed checksums do not match the 
    # checksum listed in the manifest.
    rows_mismatched = file_dataframe[df_mask].index.tolist()

    if len(rows_mismatched) > 0:
        # One or more submitted file's checksum does not match checksum listed
        # in manifest.

        # Create human readable error messages.
        errors = format_errors_for_excel_manifest(
            rows_mismatched,
            worksheet_name,
            checksum_column_name,
            error_message
        )

        update_submission_db(
            "error",
            ", ".join(errors),
            submission_id
        )

        #send_file_validation_email(errors, submission_id, submitter)
    else:
        checksums_ok = True

    return checksums_ok


if __name__ == '__main__':
    main()
