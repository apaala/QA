"""
QA.py - Perform QA by checking for consistency in filenames and md5sums to those in manifest.

Author - Apaala Chatterjee (apaala.chatterjee@som.umaryland.edu)

Steps:
1. Check Done file
2. Check file names
3. Check file md5sums
4. Check if all required files for user prompted technique are present.
5. Rename files to include flowcell
"""

import getopt, sys, os
import argparse
import os
import pandas as pd
import logging
import hashlib
from pathlib import Path
import numpy as np


logger = logging.getLogger('app.' + __name__)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None 

def main():
    parser = argparse.ArgumentParser( description='User inputs to QA script')
    parser.add_argument("-d", "--dir_path", dest="dir_path",help="Path to directory with files to be assessed", metavar="PATH")
    parser.add_argument("-m", "--manifest", dest="manifest_path",help="Full path to manifest file", metavar="FILE")
    parser.add_argument("-t", "--technique", dest="technique",help="Technique to check files for")
    #Add option to direct logfile to a specific directory.
    parser.add_argument("-l", "--log", dest="log_dir",help="Full path where you would like to direct the detailed log file.", metavar="PATH")
    parser.add_argument("-s", "--skip", dest="skip",help="Flag to skip checksum tests. Only use for testing and checking technique assoc files", action='store_true')
    #Option for writing updated manifest to file
    parser.add_argument("-u", "--umanifest", dest="updated_man",help="Full path where you would like to direct the updated manifest file.", metavar="PATH")
    #Option to do renaming
    parser.add_argument("-r", "--rename", dest="rename",help="Flag to rename the files by adding flowcell name", action='store_true')

    options = parser.parse_args()

    #Logging details
    parent_path = Path(__file__).resolve().parent
    if options.log_dir:
        log_path = options.log_dir
    else:
        log_path = parent_path / "log.txt"
    logging.basicConfig(filename=log_path,
                    filemode='w',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S%z",
                    level=logging.DEBUG)

    #Read manifest file
    manifest = pd.read_csv(options.manifest_path, sep="\t")
    print("----Starting QA----")
    #List all files in the directory provided
    all_files = os.listdir(options.dir_path)

    #Get matched and unmatched file names. Error if file in manifest not present in directory.
    matched_files, unmatched_files, missingfiles_flag= check_dir_vs_manifest(all_files, manifest)

    #Logging matches and mismatches
    logger.info(f"The number of matched files found: {len(matched_files)}")
    logger.info(f"The number of mismatched files found: {len(unmatched_files)}")
    #generate md5sums
    md5sums_df = pd.DataFrame({"full_path": manifest.filename,"manifest_filename": manifest.filename,"manifest_checksum": manifest.checksum, "calculated_md5sum": ""})
    md5sums_df['full_path'] = options.dir_path + md5sums_df['full_path'].astype(str)

    ###commented checksum checking to test technique
    if not options.skip:
        logger.info("Validating checksums now--")
        print("Performing checksum QA!!****")
        #flag for mismatched checksums
        check_md5sums = match_md5sums_to_manifest(md5sums_df)
    else:
        print("Skipping checksum QA!!!****")
        logger.info("Validating checksum step skipped!")
        check_md5sums = None

    #check md5checksums
    master_techniques = open_techniques_with_pathlib("QC_techniques_master.csv")
    ##Should be a loop for multiple techniques and aliquots???
    #####
    file_list = get_technique_file_list(options.technique, master_techniques)
    file_checks = check_tech_assoc_files(manifest, file_list, options.technique, unmatched_files)
    
    #Check required files are present
    QA_flag = None
    #Log for overall printing
    print("*******************")
    print("FINAL QA RESULTS")
    print("*******************")
    if missingfiles_flag == True and check_md5sums == True:
        file_checks["MissingFiles"] = "PASSED"
        file_checks["CheckSumQA"] = "PASSED"
        logger.info(f"QA Passed")
        QA_flag = True
        print(file_checks)
        print("QA Passed. Please check Table for details.")
    elif missingfiles_flag == True and check_md5sums == None:
        file_checks["MissingFiles"] = "PASSED"
        file_checks["CheckSumQA"] = "SKIPPED"
        QA_flag = True
        print(file_checks)
        print("QA Passed. Please check Table for details.")
    elif missingfiles_flag == False and check_md5sums == None:
        file_checks["MissingFiles"] = "FAILED"
        file_checks["CheckSumQA"] = "SKIPPED"
        QA_flag = False
        print(file_checks)
        print("QA Failed. Please check Table for details.")
    else:
        file_checks["MissingFiles"] = "FAILED"
        file_checks["CheckSumQA"] = "FAILED"
        QA_flag = False
        print(file_checks)
        print("QA Failed. Please check Table for details.")

    #Renaming files below
    updated_manifest, renaming_df = renaming_manifest_fastq(manifest, QA_flag, options.dir_path)
    #fnx to rename the files
    if options.rename and QA_flag== True:
        rename_files(renaming_df, 'filename', 'updated_filename')
        #print(updated_manifest)
        #Write outputs
        if options.updated_man:
            updated_manifest.to_csv(options.updated_man, index=False, sep='\t')
        else:
            updated_manifest.to_csv('updated_manifest.txt', index=False, sep='\t')
        #for sanity check writing old and new filenames, maybe include in log file later.
        renaming_df.to_csv('updated_filenames.txt', index=False, sep='\t')
    



def split_column_based_on_aliquotname(df, column_to_split, column_with_delimiter):
    """
    Split a string in one column into two parts based on a delimiter specified in another column,
    and place these parts into two separate new columns.

    Parameters:
    df (pandas.DataFrame): The DataFrame to modify.
    column_to_split (str): The name of the column with strings to split.
    column_with_delimiter (str): The name of the column with the delimiter.

    Returns:
    pandas.DataFrame: The modified DataFrame with two new columns.
    """

    # Check if the columns exist in the DataFrame
    if column_to_split not in df.columns or column_with_delimiter not in df.columns:
        raise ValueError("Specified columns not found in DataFrame")

    # Initialize the new columns
    df[column_to_split + '_Part1'] = None
    df[column_to_split + '_Part2'] = None

    # Perform the split and assign to new columns
    for index, row in df.iterrows():
        delimiter = str(row[column_with_delimiter])
        # Split only on the first occurrence
        parts = row[column_to_split].split(delimiter, 1)
        df.at[index, column_to_split + '_Part1'] = parts[0]
        df.at[index, column_to_split + '_Part2'] = parts[1] if len(parts) > 1 else ''

    return df


def prepend_string_to_column(df, column_name, string_to_prepend):
    """
    Prepend a string to all values in a specified column of a DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame to modify.
    column_name (str): The name of the column to modify.
    string_to_prepend (str): The string to prepend to each value in the column.

    Returns:
    pandas.DataFrame: The modified DataFrame with the string prepended.
    """

    # Check if the column exists in the DataFrame
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame")

    # Prepend the string to each value in the column
    df[column_name] = string_to_prepend + df[column_name].astype(str)

    return df

def check_R1_R2_fastq(lane_files, lane, missing_files):
    """
    Check R1 and R2 files for raw techniques. It checks if the naming is 
    consistent and required files are present.

    Parameters:
    lane_files (pandas.DataFrame): DataFrame with files belonging to specific Lane.
    lane (str): The name of the Lane for which files are being checked.
    missing_files (str): DataFrame with missing files.

    Returns:
    pandas.DataFrame: Dataframe with results.
    """
    #check for R1 and R2 fastq files for raw techniques
    #check if required files are present
    required_files = pd.DataFrame()
    #Lists with corresponding substrings
    required = ["_R1", "_R2"]
    #flag for capturing missing file
    is_missing = True

    for r in required:
        checkr = lane_files[lane_files['filename'].str.contains(r)]
        required_files = required_files.append(pd.DataFrame(data = checkr), ignore_index=True)
        ext_req_checked = required_files[required_files['filename'].str.contains("fastq")]
        if len(required_files) == 2 and len(ext_req_checked) !=2:
            ext_req_checked = required_files[required_files['filename'].str.contains("fq")]
    #Check if names being checked have been reported as missing
    if ext_req_checked.loc[ext_req_checked['filename'].isin(missing_files)].empty:
        is_missing = False
    else:
        is_missing = True
    #Check if only one character is different. should it be ext_req_checked???
    matches = match(ext_req_checked.filename[0],ext_req_checked.filename[1])
    #add logic for checking if matched, else error
    if matches and is_missing == False:
        logger.info(f"In check_R1_R2_fastq(). Following files for lane: {lane} passed: %s ",",".join(ext_req_checked.filename))
    elif matches and is_missing == True:
        missed_names = ext_req_checked.loc[ext_req_checked['filename'].isin(missing_files)]
        logger.error(f"In check_R1_R2_fastq(). Following files for lane: {lane} are missing: %s ",",".join(missed_names.filename))
    else:
        logger.error(f"In check_R1_R2_fastq(). Following files for lane: {lane} failed: %s ",",".join(ext_req_checked.filename))
    return(ext_req_checked)

def check_I1_I2_fastq(lane_files, lane, missing_files):
    """
    Check I1 and I2 files for raw techniques. It checks if the naming is 
    consistent and both sets of optional files are present.

    Parameters:
    lane_files (pandas.DataFrame): DataFrame with files belonging to specific Lane.
    lane (str): The name of the Lane for which files are being checked.
    missing_files (str): DataFrame with missing files.

    Returns:
    pandas.DataFrame: Dataframe with results.
    """
    #check for I1 and I2 fastq files for raw techniques
    #check if optional files are present.If yes both have to present!
    required_files = pd.DataFrame()
    #List with corresponding substrings
    required = ["_I1", "_I2"]
    #flag for capturing missing file
    is_missing = None

    for r in required:
        checkr = lane_files[lane_files['filename'].str.contains(r)]
        required_files = required_files.append(pd.DataFrame(data = checkr), ignore_index=True)
        ext_req_checked = required_files[required_files['filename'].str.contains("fastq")]
        if len(required_files) == 2 and len(ext_req_checked) !=2:
            ext_req_checked = required_files[required_files['filename'].str.contains("fq")]
    #Check if only one character is different. should it be ext_req_checked???
    if len(ext_req_checked) ==2:
        matches = match(ext_req_checked.filename[0],ext_req_checked.filename[1])
    else:
        matches = False
    #Check if names being checked have been reported as missing
    if ext_req_checked.loc[ext_req_checked['filename'].isin(missing_files)].empty:
        is_missing = False
    else:
        is_missing = True
    #add logic for checking if matched, else error
    if matches and is_missing == False:
        logger.info(f"In check_I1_I2_fastq(). Following files for lane: {lane} passed: %s ",",".join(ext_req_checked.filename))
    elif matches and is_missing == True:
        missed_names = ext_req_checked.loc[ext_req_checked['filename'].isin(missing_files)]
        logger.error(f"In check_I1_I2_fastq(). Following files for lane: {lane} are missing: %s ",",".join(missed_names.filename))
    else:
        logger.error(f"In check_I1_I2_fastq(). Following files for lane: {lane} failed file name QC: %s ",",".join(ext_req_checked.filename))
    return(ext_req_checked)

def check_R1_R2_R3_fastq(lane_files, lane):
    #check for R1, R2 and R3 fastq files for raw 5 file techniques
    #check if required files are present
    required_files = pd.DataFrame()
    #Lists with corresponding substrings
    required = ["_R1", "_R2", "_R3", "_I1"]

    for r in required:
        #Will need a more robust solution later as people name things inconsistently
        checkr = lane_files[lane_files['filename'].str.contains(r)]
        required_files = required_files.append(pd.DataFrame(data = checkr), ignore_index=True)
        ext_req_checked = required_files[required_files['filename'].str.contains("fastq")]
        if len(required_files) == 4 and len(ext_req_checked) !=4:
            ext_req_checked = required_files[required_files['filename'].str.contains("fq")]
    #Check if only one character is different. Adding second match for R3 files.
    matches1 = match(ext_req_checked.filename[0],ext_req_checked.filename[1])
    matches2 = match(ext_req_checked.filename[0],ext_req_checked.filename[2])
    #add logic for checking if matched, else error
    if matches1 and matches2:
        logger.info(f"In check_R1_R2_R3_fastq(). Following files for lane: {lane} passed: %s ",",".join(ext_req_checked.filename))
    else:
        logger.error(f"In check_R1_R2_R3_fastq(). Following files for lane: {lane} failed: %s ",",".join(ext_req_checked.filename))
    return(ext_req_checked)

def check_R1_R2_nuchash_fastq(lane_files, lane):
    #check for R1, R2 and R3 fastq files for raw 5 file techniques
    #check if required files are present
    #incomplete. testing required.
    required_files = pd.DataFrame()
    #Lists with corresponding substrings
    required = ["_R1", "_R2", "nuc_hash"]

    for r in required:
        #Will need a more robust solution later as people name things inconsistently
        checkr = lane_files[lane_files['filename'].str.contains(r)]
        required_files = required_files.append(pd.DataFrame(data = checkr), ignore_index=True)
        ext_req_checked = required_files[required_files['filename'].str.contains("fastq")]
        if len(required_files) == 3 and len(ext_req_checked) !=3:
            ext_req_checked = required_files[required_files['filename'].str.contains("fq")]
    #Check if only one character is different. Adding second match for R3 files.
    matches1 = match(ext_req_checked.filename[0],ext_req_checked.filename[1])
    #add logic for checking if matched, else error
    if matches1:
        logger.info(f"In check_R1_R2_nuchash_fastq(). Following files for lane: {lane} passed: %s ",",".join(ext_req_checked.filename))
    else:
        logger.error(f"In check_R1_R2_nuchash_fastq(). Following files for lane: {lane} failed: %s ",",".join(ext_req_checked.filename))
    return(ext_req_checked)


def check_I1_or_I2_fastq(lane_files, lane):
    #check for I1 and I2 fastq files for raw techniques
    #check if optional files are present.If yes both have to present!
    required_files = pd.DataFrame()
    
    #List with corresponding substrings
    required = ["_I1", "_I2"]

    for r in required:
        checkr = lane_files[lane_files['filename'].str.contains(r)]
        required_files = required_files.append(pd.DataFrame(data = checkr), ignore_index=True)
        ext_req_checked = required_files[required_files['filename'].str.contains("fastq")]
        if len(ext_req_checked) ==0:
            ext_req_checked = required_files[required_files['filename'].str.contains("fq")]
    #Check if only one character is different. should it be ext_req_checked???
    #If only one file
    if len(ext_req_checked) == 1:
        logger.info(f"In check_I1_or_I2_fastq(). Following lane: {lane} only has one Optional file: %s ",",".join(ext_req_checked.filename))
    elif len(ext_req_checked) == 2:
        matches = match(ext_req_checked.filename[0],ext_req_checked.filename[1])
        #add logic for checking if matched, else error
        if matches:
            logger.info(f"In check_I1_or_I2_fastq(). Following files for lane: {lane} passed: %s ",",".join(ext_req_checked.filename))
        else:
            logger.error(f"In check_I1_or_I2_fastq(). Following files for lane: {lane} failed: %s ",",".join(ext_req_checked.filename))
    return(ext_req_checked)


def check_raw_4_file_format_techniques(file_list, manifest, aliquot, missing_files):
    """ This function checks for techniques that produce 4 files. 
    These Files are expected to have specific substrings.
    Input: 1) List of expected files, 
           2) Manifest for aliquot 
           3) aliquot string (not using currently may want to later.
           4) list of missing files.
    Output: DF with lane and T/F for required and optional files.
    """
    #ASSUMPTION! Every aliquot has 8 lanes that will be named in rthe format below. 
    #Confirmed assumption with Suvvi on 10/19.

    #Lanes per aliquot
    lanes_substring = ["L001","L002","L003","L004","L005","L006","L007","L008"]
    logger.debug("In check_raw_4_file_format_techniques()")
    #Capture per lane file checks
    lane_checks = []

    #For every aliquot there should be at least R1 and R2 for each lane.
    for lane in lanes_substring:
        #print(lane)
        req = None
        opt = None
        lane_files = manifest[manifest['filename'].str.contains(lane)]
        #Add check to see if the filenames being checked exist in the directory
        row = []
        row.append(lane)
        #Flags to check for missing files
        req_in_dir = None
        opt_in_dir = None
        #If # files == 4, check for R1/2 and I1/2
        if len(lane_files) ==4 or len(lane_files) ==3:
            #check if required files are present
            required_files = check_R1_R2_fastq(lane_files, lane, missing_files)
            if required_files.loc[required_files['filename'].isin(missing_files)].empty:
                req_in_dir = False
            else:
                req_in_dir = True
            if len(required_files) == 2 and req_in_dir == False:
                req = True
                row.append(req)
            else:
                req = False
                row.append(req)
            optional_files = check_I1_I2_fastq(lane_files, lane, missing_files)
            if optional_files.loc[optional_files['filename'].isin(missing_files)].empty:
                opt_in_dir = False
            else:
                opt_in_dir = True
            if len(optional_files) == 2 and opt_in_dir == False:
                opt = True
                row.append(opt)
            else:
                opt = False
                row.append(opt)
            ##check to see if all 4 iles are present and if not, fail flags
            if len(optional_files) == 2 and len(required_files) == 2:
                opt = True
                req = True
            elif len(required_files) == 2 and len(optional_files) == 1:
                opt = False
                req = True
            else:
                opt = False
                req = False
        #If # files == 2, check for R1/2 
        elif len(lane_files) ==2:
            required_files = check_R1_R2_fastq(lane_files, lane, missing_files)
            if len(required_files) == 2:
                req = True
                row.append(req)
            else:
                row.append(opt)
        #account for missing lanes, they dont always submit all lanes.
        elif len(lane_files) == 0:
            logger.warning(f"No files were found for lane {lane} in aliquot {aliquot}")
            row.append(req)
            row.append(opt)
        #If # files anything else error
        else:
            logger.error(f"Mismatch found! Please check file names for aliquot: {aliquot}")
            row.append(req)
            row.append(opt)
        lane_checks.append(row)
    return(pd.DataFrame(lane_checks, columns = ["Lane", "Req", "Opt"]))

def check_raw_3_hash_file_format_techniques(file_list, manifest, aliquot_files, missing_files):
    """ This function checks for techniques that produce 5 files. 
    These Files are expected to have specific substrings.
    Input: 1) List of expected files, 
           2) Manifest for aliquot 
           3) aliquot string (not using currently may want to later.
           4) list of missing files.
    Output: DF with lane and T/F for required and optional files.
    """
    #ASSUMPTION! Every aliquot has 8 lanes that will be named in the format below. Confirmed assumption with Suvvi on 10/19.
    lanes_substring = ["L001","L002","L003","L004","L005","L006","L007","L008"]
    print(" in sub for 3 files")
    #params for nuc_hash
    required = ["R1", "R2"]
    format = ["fastq", "fq"]
    # For every aliquot there should be at least R1 and R2
    #req = '|'.join(r"\b{}\b".format(x) for x in required)
    for lane in lanes_substring:
        print(lane)
        lane_files = manifest[manifest['filename'].str.contains(lane)]
        #If # files == 4, check for R1/2 and I1/2
        if len(lane_files) ==3:
            #check if required files are present
            required_files = check_R1_R2_fastq(lane_files, lane)
            required_hash = "set up params"
        #If # files == 4, check for R1/2 
        elif len(lane_files) ==2:
            required_files = check_R1_R2_fastq(lane_files, lane)
        elif len(lane_files) == 0:
            logger.warning(f"No files were found for lane {lane} in aliquot {aliquot}")
        #If # files anything else error
        else:
            print("Mismatched # of files found")
        #check if both files are present and have the right extention
    return("yes")

def check_raw_5_file_format_techniques(file_list, manifest, aliquot, missing_files):
    """ This function checks for techniques that produce 5 files. 
    These Files are expected to have specific substrings.
    Input: 1) List of expected files, 
           2) Manifest for aliquot 
           3) aliquot string (not using currently may want to later.
           4) list of missing files.
    Output: DF with lane and T/F for required and optional files.
    """
    #ASSUMPTION! Every aliquot has lanes that will be named in the format below. Confirmed assumption with Suvvi on 10/19.
    lanes_substring = ["L001","L002","L003","L004","L005","L006","L007","L008"]
    format = ["fastq", "fq"]
    #Capture per lane file checks
    lane_checks = []

    # For every aliquot there should be at least R1 and R2
    for lane in lanes_substring:
        req = False
        opt = False
        logger.debug("In check_raw_5_file_format_techniques().")
        lane_files = manifest[manifest['filename'].str.contains(lane)]
        row = []
        row.append(lane)
        #If # files == 5, check for R1/2/3 and I1/2
        if len(lane_files) == 4 or len(lane_files) == 5:
            #check if required files are present
            required_files = check_R1_R2_R3_fastq(lane_files, lane)
            if len(required_files) == 4:
                req = True
                row.append(req)
        elif len(lane_files) == 0:
            logger.warning(f"No files were found for lane {lane} in aliquot {aliquot}")
            row.append(req)
            row.append(opt)
        #If # files anything else error
        else:
            #print("Mismatched # of files found")
            logger.error(f"Mismatch found! Please check file names for aliquot: {aliquot}")
            row.append(req)
            row.append(opt)
        lane_checks.append(row)
        #check if both files are present and have the right extention
    print("Performed checks for aliquot: ", aliquot)
    return(pd.DataFrame(lane_checks, columns = ["Lane", "Req"]))


def check_tech_assoc_files(manifest, file_list, techniques, missing_files):
    """ This function checks the technique and calls appropriate functions.
    Input: 1) Manifest 
           2) File list
           3) Techniquees file.
           4) list of missing files.
    Output: DF with lane and T/F for required and optional files.
    """
    technique = pd.read_csv(techniques, sep=",")
    total_file_count = len(manifest.filename)
    data_type = file_list['data_type'].unique()
    master_QA_list = []
    #All techniques that have R1, R2, I1, and I2 are in the list below. Add to list if new technique fits.
    raw_4_file_format_techniques = [ "10X Genomics Multiome;RNAseq", "10X Genomics Immune profiling;VDJ",
     "10X Genomics Immune profilling;GEX", "10xv2", "10xv3", "10xmultiome_cell_hash;RNA"]
    #All techniques that have R1, R2, R3, I1, and I2 are in the list below. Add to list if new technique fits.
    raw_5_file_format_techniques =[ "10X Genomics Multiome;ATAC-seq", "10xmultiome_cell_hash;ATAC"]
    
    for index, row in technique.iterrows():
        tname = row['name']
        aliquot = row['aliquot']
        logger.info(f"Checking Files for {tname} and Aliquot {aliquot}")
        
        #Get all manifest info for technique aliquot
        man_files = manifest[manifest['filename'].str.contains(aliquot)]

        #Currently only supporting raw file types. 
        #Checking to see which case the technique belongs to and preoceeding accordingly.
        if data_type == 'raw' and tname in raw_4_file_format_techniques:
            check_raw_files = check_raw_4_file_format_techniques(file_list, man_files, aliquot, missing_files)
            #for overall QA log return Opt and req along with tech and aliquot
            print("Starting QA for ",tname," aliquot ", aliquot)
            overall_opt, overall_req = check_QA_for_aliquot(check_raw_files)
            print(check_raw_files)
            if check_raw_files['Req'].all() == True and check_raw_files['Opt'].all() == True:
                logger.info(f"All Required AND Optional Files for {tname} and Aliquot {aliquot} are present")
                print("QA passed for ",tname," aliquot ", aliquot)
            elif check_raw_files['Req'].all() == True and check_raw_files['Opt'].any() == False:
                logger.info(f"All Required Files for {tname} and Aliquot {aliquot} are present. Optional files are either absent of failed QA.")
                print("QA passed for Required files for ",tname," aliquot ", aliquot)
            elif check_raw_files['Req'].all() == True and check_raw_files['Opt'].any() == None:
                logger.info(f"All Required Files for {tname} and Aliquot {aliquot} are present. Optional files are either absent.")
                print("QA passed for Required files for ",tname," aliquot ", aliquot)
            else:
                missing_lanes = check_raw_files[check_raw_files.eq(False).any(1)]["Lane"]
                logger.error(f"All Required Files for {tname} and Aliquot {aliquot} are NOT present for following lanes %s ",",".join(map(str,missing_lanes)))
                #logger.error(f"All Required Files for {tname} and Aliquot {aliquot} are NOT present!")
                print("QA FAILED for ",tname," aliquot ", aliquot)
            master_QA_list.append([tname, aliquot,overall_opt, overall_req ])

        elif data_type == 'raw' and tname in raw_5_file_format_techniques:
            #needs testing
            check_raw_files = check_raw_5_file_format_techniques(file_list, man_files, aliquot, missing_files)
            print("Starting QA for ",tname," aliquot ", aliquot)
            #No optional files, find better solution check_QA_for_aliquot
            overall_opt, overall_req = check_QA_for_aliquot(check_raw_files)
            print(check_raw_files)
            if check_raw_files['Req'].all():
                logger.info(f"All Required Files for {tname} and Aliquot {aliquot} are present")
                print("QA passed for ",tname," aliquot ", aliquot)
            else:
                logger.error(f"All Required Files for {tname} and Aliquot {aliquot} are NOT present!")
                print("QA FAILED for ",tname," aliquot ", aliquot)
            #No optional files for these right now.
            #if check_raw_files['Opt'].all():
            #    logger.info(f"All Optional Files for {tname} and Aliquot {aliquot} are present")
            #else:
            #    logger.warning(f"All Optional Files for {tname} and Aliquot {aliquot} are NOT present!")
            master_QA_list.append([tname, aliquot,overall_opt, overall_req ])

        elif data_type == 'raw' and tname == "10xmultiome_cell_hash;hashing":
            #needs testing
            check_raw_files = check_raw_3_hash_file_format_techniques(file_list, man_files, aliquot, missing_files)
            print("Starting QA for ",tname," aliquot ", aliquot)
            print(check_raw_files)
            if check_raw_files['Req'].all():
                logger.info(f"All Required Files for {tname} and Aliquot {aliquot} are present")
                print("QA passed for ",tname," aliquot ", aliquot)
            else:
                logger.error(f"All Required Files for {tname} and Aliquot {aliquot} are NOT present!")
                print("QA FAILED for ",tname," aliquot ", aliquot)
            if check_raw_files['Opt'].all():
                logger.info(f"All Optional Files for {tname} and Aliquot {aliquot} are present")
            else:
                logger.warning(f"All Optional Files for {tname} and Aliquot {aliquot} are NOT present!")
        else:
            print("files are not raw or in 4 format raw")
        master_QA_df = pd.DataFrame(master_QA_list, columns = ['Technique','Aliquot','Optional', 'Required'])
        print("-------------")
    return master_QA_df

def check_QA_for_aliquot(check_raw_files):
    req = None
    opt = None
    #print("***")
    #print(check_raw_files)
    if 'Opt' in check_raw_files.columns:
        if (check_raw_files['Opt']).eq(False).any():
            opt = "FAILED"
        elif (check_raw_files['Opt']).eq(True).all():
            opt = "PASSED"
        elif (check_raw_files['Opt']).eq(None).any() and not (check_raw_files['Opt']).eq(False).any():
            opt = "PASSED"
    else:
        opt = None
    if not check_raw_files['Req'].all():
        req = "FAILED"
    else:
        req = "PASSED"
    return opt,req

def get_technique_file_list(techniques, master):
    technique = pd.read_csv(techniques, sep=",")
    #print(technique)
    file_list = master[master.technique.isin(technique.name)]
    #temporary prints for new users. Will be replaced with logging.
    #print(" Step 3 Complete: Getting technique details")
    logger.info(f"Getting technique details from master file based on user input.")
    return(file_list)

def open_techniques_with_pathlib(file_name):
   script_dir = Path(__file__).resolve().parent
   file_path = script_dir / file_name
   content = pd.read_csv(file_path, sep=",")
   return content

def check_dir_vs_manifest(all_files, manifest):
    """ 
    Check if files in directory and files listed in the manifest match.
    Error: if there are files in manifest missing from the directory.
    Warning: if there are files in directory not in manifest
    Inputs: Directory Files and Manifest Files.
    Outputs: All files in both and missing files.
    """
    ##Flag for logging
    flag = None
    #Get all files that are present in directory and in manifest
    contains_all = [x for x in all_files if x not in manifest.filename]
    #Get all files that are present in directory and missing in manifest. NBD. Add to log!
    missing_files_from_manifest = list(set(all_files) - set(manifest.filename))
    if len(missing_files_from_manifest)>0:
        logger.warning(f"These files are not in manifest but present in the directory: %s ",missing_files_from_manifest)
    #Get all files that are present in manifest and missing in directory. Bad.
    missing_files = list(set(manifest.filename) - set(all_files))
    if len(missing_files)>0:
        logger.error(f"These files are in manifest but missing from the directory: %s ",missing_files)
        flag = False
    else:
        flag = True
    #temporary message for debugging
    #print("Step 1 Complete: Checked Names")
    return(contains_all, missing_files, flag)

def renaming_manifest_fastq(manifest, QA_flag, dpath):
    manifest_copy = manifest
    #Flag for columns N/O/P check. This will skip the correctly formatted NYGC submission.
    NOP = None
    rename_3 = find_rows_with_extensions(manifest,'filename', ['.csv', '.xml'])
    #print(rename_3)
    #Assumption: Only one Flowcell per manifest. Confirmed with Suvvi on 12/06/23
    flowcell = manifest['flow_cell_name'].unique()
    ###Checking for flowcell in NOP
    #Check if 14,15,16 have flowcell in the name demultiplex_stats_filename,run_parameters_filename and top_unknown_barcodes_filename
    #declare df to hold old and new filenames
    if(len(flowcell)==1):
        #Detect unique values
        dsf_t = list(manifest.demultiplex_stats_filename.unique())
        dsf = [x for x in dsf_t if str(x) != 'nan']
        #print(dsf)
        rpf_t = list(manifest.run_parameters_filename.unique())
        rpf = [x for x in rpf_t if str(x) != 'nan']
        tubf_t = list(manifest.top_unknown_barcodes_filename.unique())
        tubf = [x for x in tubf_t if str(x) != 'nan']
        #N
        if any(flowcell[0] in s for s in dsf):
            #check that there arent >2 values (nan and filename)
            print("No changes necessary for N")
        else:
            #prepend the flowcell to column.
            old = dsf
            #print(old)
            oldp = prepend_path_to_variable(old[0], dpath)
            #print(oldp)
            #prepend the flowcell to column.
            hold = prepend_string_to_column(manifest, 'demultiplex_stats_filename', flowcell[0])
            manifest_copy['demultiplex_stats_filename'] = hold['demultiplex_stats_filename']
            new = manifest_copy['demultiplex_stats_filename'].unique()
            #print(new[0])
            newp = prepend_path_to_variable(new[0], dpath)
            #print(newp)
            #add renAMING THESE FILES HERE. EASIEST TO HANDLE.
            rename_info_file(oldp, newp)
            
        #O
        if any(flowcell[0] in s for s in rpf):
            print("No changes necessary for O")
            #check that there arent >2 values (nan and filename)
        else:
            #prepend the flowcell to column.
            old = rpf
            oldp = prepend_path_to_variable(old[0], dpath)
            hold = prepend_string_to_column(manifest, 'run_parameters_filename', flowcell[0])
            manifest_copy['run_parameters_filename'] = hold['run_parameters_filename']
            new = manifest_copy['run_parameters_filename'].unique()
            newp = prepend_path_to_variable(new[0], dpath)
            rename_info_file(oldp, newp)
        #P
        if any(flowcell[0] in s for s in tubf):
            print("No channges necessary for P")
            #check that there arent >2 values (nan and filename)
        else:
            old = tubf
            oldp = prepend_path_to_variable(old[0], dpath)
            #print(oldp)
            #prepend the flowcell to column.
            hold = prepend_string_to_column(manifest, 'top_unknown_barcodes_filename', flowcell[0])
            manifest_copy['top_unknown_barcodes_filename'] = hold['top_unknown_barcodes_filename']
            new = manifest_copy['top_unknown_barcodes_filename'].unique()
            newp = prepend_path_to_variable(new[0], dpath)
            #print(newp)
            rename_info_file(oldp, newp)

    #Split filename into 2 sub strings 
    split_filenames = split_column_based_on_aliquotname(manifest, 'filename', 'library_aliquot_name')

    #prepend flowcell name
    appended = prepend_string_to_column(manifest, 'filename_Part2', flowcell[0])

    #Deal with 3 non fq files
    non_fq = find_files_without_extension(manifest_copy, 'filename', '.gz', 'flow_cell_name')

    #Make updated filename. 
    manifest_copy['updated_filename'] = appended['library_aliquot_name'] +"_"+ appended['filename_Part2']
    
    #Make df for filename change.
    renamed_filt = manifest_copy[['filename', 'updated_filename']].copy()
    renaming_df = renamed_filt.dropna()
    hold = replace_double_underscore(renaming_df, 'updated_filename')
    renaming_df.loc[:,'updated_filename']= hold['updated_filename']
    #print(renaming_df)
    manifest_copy['filename'] = non_fq['non_fq']
    updated_names = replace_values_if_contains(manifest_copy, 'filename', 'updated_filename', 'gz')
        
    
    #prepend paths of files to columns
    hold1 = prepend_directory_path(renaming_df, 'filename', dpath)
    renaming_df.loc[:,'filename']=hold1['filename']
    hold1 = replace_double_underscore(renaming_df, 'filename')
    renaming_df.loc[:,'filename']=hold1['filename']

    hold2 = prepend_directory_path(renaming_df, 'updated_filename', dpath)
    renaming_df.loc[:,'updated_filename']=hold2['updated_filename']
    #hold2 = replace_double_underscore(renaming_df, 'updated_filename')
    #renaming_df['updated_filename']=hold2['updated_filename']
    #renaming_df['updated_filename'] = replace_double_underscore(renaming_df, 'updated_filename')
    #print(renaming_df['filename'])
    #print(renaming_df['updated_filename'])
    

    manifest_copy.loc[:,'filename'] = updated_names['filename']
    #drop extra columns
    manifest_copy.drop(['filename_Part1', 'filename_Part2', 'non_fq', 'updated_filename'], axis=1, inplace=True)

    manifest_temp = replace_double_underscore(manifest_copy, 'filename')
    manifest_f1 =  delete_values_based_on_string(manifest_temp, 'demultiplex_stats_filename','file_format','run metrics')
    manifest_f2 =  delete_values_based_on_string(manifest_f1, 'run_parameters_filename','file_format','run metrics')
    manifest_formatted =  delete_values_based_on_string(manifest_f2, 'top_unknown_barcodes_filename','file_format','run metrics')
    #Save files
    split_filenames.to_csv('split.csv', sep='\t')
    manifest_formatted.to_csv('updated_manifest.txt', index=False, sep='\t')
    renaming_df.to_csv('updated_filenames.txt', index=False, sep='\t')
    
    #Handle non fastq files
    
    return manifest_formatted,renaming_df

def find_rows_with_extensions(df, column_name, extensions):
    """
    Find all rows in a DataFrame where the specified column's entries end with certain extensions.

    Args:
    df (pandas.DataFrame): The DataFrame to search.
    column_name (str): The name of the column to check.
    extensions (list of str): The list of extensions to look for (e.g., ['.csv', '.xml']).

    Returns:
    pandas.DataFrame: A DataFrame containing only the rows where the column entries match the specified extensions.
    """
    # Filter the DataFrame based on the specified extensions
    filtered_df = df[df[column_name].astype(str).str.endswith(tuple(extensions))]

    return filtered_df

def delete_values_based_on_string(df, target_column, condition_column, string_to_check):
    """
    Deletes values in one DataFrame column if a specified string is found in another column.

    :param df: DataFrame containing the data.
    :param target_column: The name of the column whose values are to be deleted.
    :param condition_column: The name of the column to check for the specified string.
    :param string_to_check: The string to check for in the condition column.
    :return: DataFrame with values deleted based on the condition.
    """
    mask = df[condition_column].str.contains(string_to_check, na=False)
    df.loc[mask, target_column] = np.nan
    return df

def rename_info_file(old_file_path, new_file_path):
    """
    Rename a file from one path to another.

    Args:
    old_file_path (str): The current file path.
    new_file_path (str): The new file path.

    Returns:
    bool: True if the file was successfully renamed, False otherwise.
    """
    try:
        # Rename the file
        os.rename(old_file_path, new_file_path)
        return True
    except Exception as e:
        print(f"Error occurred during renaming: {e}")
        return False

def rename_files(dataframe, original_column, new_column):
    """
    Renames files based on names in two columns of a DataFrame.

    Parameters:
    dataframe (pd.DataFrame): The DataFrame containing the file names.
    original_column (str): The name of the column with the original file names.
    new_column (str): The name of the column with the new file names.
    """
    for index, row in dataframe.iterrows():
        original_name = row[original_column]
        new_name = row[new_column]
        
        # Check if the original file exists and rename it
        if os.path.exists(original_name):
            os.rename(original_name, new_name)
        else:
            print(f"File {original_name} does not exist.")

def replace_double_underscore(df, column_name):
    """
    Replaces all double underscores with single underscores in a specified column of a DataFrame.

    :param df: DataFrame containing the data.
    :param column_name: The name of the column in which to replace double underscores.
    :return: DataFrame with the replacements made.
    """
    df.loc[:,column_name] = df[column_name].str.replace('__', '_', regex=False)
    return df


def replace_values_if_contains(df, target_column, replacement_column, string_to_check):
    """
    Replaces values in a DataFrame column with values from another column if they contain a specific string.

    :param df: DataFrame to operate on.
    :param target_column: The name of the column whose values are to be replaced.
    :param replacement_column: The name of the column from which to take replacement values.
    :param string_to_check: The string to check for in the target column values.
    :return: DataFrame with replaced values.
    """
    mask = df[target_column].str.contains(string_to_check, na=False)
    df.loc[mask, target_column] = df.loc[mask, replacement_column]
    return df

def find_files_without_extension(df, column_name, excluded_extension,prepend_column ):
    """
    Finds all files in the specified DataFrame column that do not have the specified extension.

    :param df: DataFrame containing the file names.
    :param column_name: Name of the column in the DataFrame containing the file names.
    :param excluded_extension: The file extension to exclude (e.g., 'txt').
    :return: A list of file names without the specified extension.
    """
    if excluded_extension.startswith('.'):
        excluded_extension = excluded_extension[1:]
    def rename_file(row):
        file_name = row[column_name]
        if not file_name.endswith('.' + excluded_extension):
            if(row[prepend_column] in file_name):
                print("No need to prepend")
            else:
                return row[prepend_column] + '_' + file_name
        return file_name

    df.loc[:,'non_fq'] = df.apply(rename_file, axis=1)
    return df

    return df[df[column_name].apply(lambda x: not x.endswith('.' + excluded_extension))][column_name].tolist()

def split_column_based_on_aliquotname(df, column_to_split, column_with_delimiter):
    """
    Split a string in one column into two parts based on a delimiter specified in another column,
    and place these parts into two separate new columns.

    Parameters:
    df (pandas.DataFrame): The DataFrame to modify.
    column_to_split (str): The name of the column with strings to split.
    column_with_delimiter (str): The name of the column with the delimiter.

    Returns:
    pandas.DataFrame: The modified DataFrame with two new columns.
    """

    # Check if the columns exist in the DataFrame
    if column_to_split not in df.columns or column_with_delimiter not in df.columns:
        raise ValueError("Specified columns not found in DataFrame")

    # Initialize the new columns
    df[column_to_split + '_Part1'] = None
    df[column_to_split + '_Part2'] = None

    # Perform the split and assign to new columns
    for index, row in df.iterrows():
        delimiter = str(row[column_with_delimiter])
        parts = row[column_to_split].split(delimiter, 1)# Split only on the first occurrence

        df.at[index, column_to_split + '_Part1'] = parts[0]
        df.at[index, column_to_split + '_Part2'] = parts[1] if len(parts) > 1 else ''

    return df


def prepend_string_to_column(df, column_name, string_to_prepend):
    """
    Prepend a string to all values in a specified column of a DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame to modify.
    column_name (str): The name of the column to modify.
    string_to_prepend (str): The string to prepend to each value in the column.

    Returns:
    pandas.DataFrame: The modified DataFrame with the string prepended.
    """

    # Check if the column exists in the DataFrame
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame")

    # Prepend the string to each value in the column
    df[column_name] = string_to_prepend +"_"+ df[column_name].astype(str)

    return df

def prepend_path_to_variable(variable, path_to_append):
    """
    Append a path to a variable, ensuring the correct format.

    Args:
    variable (str): The original variable.
    path_to_append (str): The path to append.

    Returns:
    str: The variable with the path appended.
    """
    # Ensure the path_to_append starts with a '/'
    if not path_to_append.startswith('/'):
        path_to_append = '/' + path_to_append
    if not path_to_append.endswith('/'):
        path_to_append += '/'

    # Append the path to the variable
    variable_with_path = path_to_append + variable 

    return variable_with_path


def prepend_directory_path(df, column_name, directory_path):
    """
    Prepend a directory path to every entry in the specified column of a DataFrame.

    Args:
    df (pandas.DataFrame): The DataFrame containing the column.
    column_name (str): The name of the column to modify.
    directory_path (str): The directory path to prepend.

    Returns:
    pandas.DataFrame: The modified DataFrame.
    """
    # Ensure the directory path ends with a '/'
    if not directory_path.endswith('/'):
        directory_path += '/'

    # Prepend the directory path to each entry in the column
    df.loc[:,column_name] = directory_path + df[column_name].astype(str)

    return df

def match_md5sums_to_manifest(md5sums_df):
    """ 
    Generate independent md5sums and check them against those in manifest.
    """
    #calc md5sum for each file and match to corresponding column in manifest.
    logger.debug("In match_md5sums_to_manifest().")
    #print( "in match_md5sums_to_manifest()")
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
        logger.error(f"Found mismatches match_md5sums_to_manifest(). Filename : %s",",".join(md5sums_df.iloc[rows_mismatched,1]))
        print("checksum QC failed for the following",",".join(md5sums_df.iloc[rows_mismatched,1]))
        ###print(df_mask)
        #send_file_validation_email(errors, submission_id, submitter)
    else:
        checksums_ok = True
        print("md5sum QA completed successfully.")
    #temporary prints for new users. Will be replaced with logging.
    #print("Step 2 Complete: Checking md5sums")
    #print("checksums match!")
    return checksums_ok

def compute_md5(filepath):
    """
    Compute md5 checksum for file. Borrowed from AUX.
    """
    #logger.info(f"Computing md5 of {filepath}")

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


def match(s1, s2):
    """
    Check 2 strings and allow for only 1 mismatch.
    ex. NY-MX12001-1_S1_L007_I1_001.fastq.gz and NY-MX12001-1_S1_L007_I2_001.fastq.gz should be a match
    So if match == T and I2 in string it should pass.
    ex. NY-MX12001-1_S1_L007_I1_001.fastq.gz and NY-MX12001-1_S542_L007_I2_001.fastq.gz should not be a match
    
    """
    ok = False
    for c1, c2 in zip(s1, s2):
        if c1 != c2:
            if ok:
                return False
            else:
                ok = True
    return ok

if __name__ == '__main__':
    main()
