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

parent_path = Path(__file__).resolve().parent
log_path = parent_path / "log.txt"

logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('app.' + __name__)



def main():
    parser = argparse.ArgumentParser( description='User inputs to QA script')
    parser.add_argument("-d", "--dir_path", dest="dir_path",help="Path to directory with files to be assessed", metavar="PATH")
    parser.add_argument("-m", "--manifest", dest="manifest_path",help="Full path to manifest file", metavar="FILE")
    parser.add_argument("-t", "--technique", dest="technique",help="Technique to check files for")
    parser.add_argument("-s", "--skip", dest="skip",help="Flag to skip checksum tests. Only use for testing and checking technique assoc files", action='store_true')
    options = parser.parse_args()

    #Read manifest file
    manifest = pd.read_csv(options.manifest_path, sep="\t")
    #print(len(manifest))

    #List all files in the directory provided
    all_files = os.listdir(options.dir_path)
    #print(len(all_files))

    #Get matched and unmatched file names. Error if file in manifest not present in directory.
    matched_files, unmatched_files= check_dir_vs_manifest(all_files, manifest)
    
    #Tests
    #print("The number of matched files found: ",len(matched_files))
    #print("-----")
    #print("The number of mismatched files found: ",len(unmatched_files))

    #Logging matches and mismatches
    logger.info("The number of matched files found: ",len(matched_files))
    logger.info("The number of mismatched files found: ",len(unmatched_files))
    #generate md5sums
    
    #add logic to not get here if file names dont match
    md5sums_df = pd.DataFrame({"full_path": manifest.filename,"manifest_filename": manifest.filename,"manifest_checksum": manifest.checksum, "calculated_md5sum": ""})

    #md5sums_df.File = os.path.join( options.dir_path, md5sums_df.File)
    md5sums_df['full_path'] = options.dir_path + md5sums_df['full_path'].astype(str)
    #print(md5sums_df)
    print("----")

    ###commented checksum checking to test technique
    if not options.skip:
        logger.info("Validating checksums now--")
        #print("Performing checksum tests!!!*****")
        check_md5sums = match_md5sums_to_manifest(md5sums_df)
    else:
        #print("Skipping checksum tests!!!****")
        logger.info("Validating checksum step skipped!")
    #print(check_md5sums)
    #check md5checksums
    master_techniques = open_techniques_with_pathlib("QC_techniques_master.csv")
    #print(master_techniques)
    ##Should be a loop for multiple techniques and aliquots???
    #####
    file_list = get_technique_file_list(options.technique, master_techniques)
    file_checks = check_tech_assoc_files(manifest, file_list, options.technique)
    # Get the aliquot and use it to find file names. pass it manifest, file_list, techniques
    #get_technique_info

    #Check required files are present

def check_R1_R2_fastq(lane_files, lane):
    #check for R1 and R2 fastq files for raw techniques
    #check if required files are present
    required_files = pd.DataFrame()
    
    #Lists with corresponding substrings
    required = ["R1", "R2"]

    for r in required:
        checkr = lane_files[lane_files['filename'].str.contains(r)]
        required_files = required_files.append(pd.DataFrame(data = checkr), ignore_index=True)
        ext_req_checked = required_files[required_files['filename'].str.contains("fastq")]
        if len(required_files) == 2 and len(ext_req_checked) !=2:
            ext_req_checked = required_files[required_files['filename'].str.contains("fq")]
    #Check if only one character is different. should it be ext_req_checked???
    matches = match(ext_req_checked.filename[0],ext_req_checked.filename[1])
    print(matches)
    #add logic for checking if matched, else error
    if matches:
        logger.info(f"In check_R1_R2_fastq(). Following files for lane: {lane} passed: %s ",",".join(ext_req_checked.filename))
    else:
        logger.error(f"In check_R1_R2_fastq(). Following files for lane: {lane} failed: %s ",",".join(ext_req_checked.filename))
    #logger.info("In check_R1_R2_fastq(). Following files for lane: {lane} passed: %s ",ext_req_checked)
    #temporary prints for new users. Will be replaced with logging.
    #print("In check_R1_R2_fastq(). Following files for lane: ", lane," passed: ",','.join(ext_req_checked.filename))
    return(ext_req_checked)

def check_I1_I2_fastq(lane_files, lane):
    #check for I1 and I2 fastq files for raw techniques
    #check if required files are present
    required_files = pd.DataFrame()
    
    #List with corresponding substrings
    required = ["I1", "I2"]

    for r in required:
        checkr = lane_files[lane_files['filename'].str.contains(r)]
        required_files = required_files.append(pd.DataFrame(data = checkr), ignore_index=True)
        ext_req_checked = required_files[required_files['filename'].str.contains("fastq")]
        if len(required_files) == 2 and len(ext_req_checked) !=2:
            ext_req_checked = required_files[required_files['filename'].str.contains("fq")]
    logger.info(f"In check_I1_I2_fastq(). Following files for lane: {lane} passed: %s ",",".join(ext_req_checked.filename))
    #temporary prints for new users. Will be replaced with logging.
    #print("In check_I1_I2_fastq(). Following files for lane: ", lane," passed: ",','.join(ext_req_checked.filename))
    return(ext_req_checked)

def check_raw_4_file_format_techniques(file_list, manifest, aliquot):
    """ This function checks for techniques that produce 4 files. 
    These Files are expected to have specific substrings.
    Input: 1) List of expected files, 
           2) Manifest for aliquot 
           3) aliquot string (not using currently may want to later.)
    Output: DF with lane and T/F for required and optional files.
    """
    #ASSUMPTION! Every aliquot has 8 lanes that will be named in rthe format below. 
    #Confirmed assumption with Suvvi on 10/19.

    #Lanes per aliquot
    lanes_substring = ["L001","L002","L003","L004","L005","L006","L007","L008"]
    logger.debug("In check_raw_4_file_format_techniques()")
    logger.info("")

    #Capture per lane file checks
    lane_checks = []

    #For every aliquot there should be at least R1 and R2 for each lane.
    for lane in lanes_substring:
        #print(lane)
        req = False
        opt = False
        lane_files = manifest[manifest['filename'].str.contains(lane)]
        row = []
        row.append(lane)
        #If # files == 4, check for R1/2 and I1/2
        if len(lane_files) ==4:
            #check if required files are present
            required_files = check_R1_R2_fastq(lane_files, lane)
            if len(required_files) == 2:
                req = True
                row.append(req)
            optional_files = check_I1_I2_fastq(lane_files, lane)
            if len(optional_files) == 2:
                opt = True
                row.append(opt)
            else:
                opt = False
                row.append(opt)
        #If # files == 2, check for R1/2 
        elif len(lane_files) ==2:
            required_files = check_R1_R2_fastq(lane_files, lane)
            if len(required_files) == 2:
                req = True
                row.append(req)
            else:
                row.append(opt)
        #If # files anything else error
        else:
            logger.error(f"Mismatch found! Please check file names for aliquot: {aliquot}")
            row.append(req)
            row.append(opt)
        lane_checks.append(row)
        #print(lane_checks)
        #check if both files are present and have the right extention
    #print("Performed checks for aliquot: ", aliquot)
    return(pd.DataFrame(lane_checks, columns = {"Lane", "Req", "Opt"}))

def check_raw_3_hash_file_format_techniques(file_list, manifest, aliquot_files):
    #ASSUMPTION! Every aliquot has 8 lanes that will be named in rthe format below. Confirmed assumption with Suvvi on 10/19.
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
        #If # files anything else error
        else:
            print("Mismatched # of files found")
        #check if both files are present and have the right extention
    return("yes")

def check_raw_5_file_format_techniques(file_list, manifest, aliquot_files):
    """ This function checks for techniques that produce 5 files. 
    These Files are expected to have specific substrings.
    Input: 1) List of expected files, 
           2) Manifest for aliquot 
           3) aliquot string (not using currently may want to later.)
    Output: DF with lane and T/F for required and optional files.
    """
    #ASSUMPTION! Every aliquot has 8 lanes that will be named in rthe format below. Confirmed assumption with Suvvi on 10/19.
    lanes_substring = ["L001","L002","L003","L004","L005","L006","L007","L008"]
    print(" in sub for 4 files")
    required = ["R1", "R2"]
    #another optional file is R3 not handled by above check. Accounted for below.
    optional = ["I1", "I2"]
    
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
        if len(lane_files) ==5:
            #check if required files are present
            required_files = check_R1_R2_fastq(lane_files, lane)
            if len(required_files) == 2:
                req = True
                row.append(req)
            optional_files = check_I1_I2_fastq(lane_files, lane)
            optional_R3 = lane_files[lane_files['filename'].str.contains("R3")]
            if len(optional_files) == 2 and optional_R3:
                opt = True
                row.append(opt)
        elif len(lane_files) ==2:
            required_files = check_R1_R2_fastq(lane_files, lane)
            if len(required_files) == 2:
                req = True
                row.append(req)
        #If # files anything else error
        else:
            print("Mismatched # of files found")
            row.append(req)
            row.append(opt)
        lane_checks.append(row)
        #check if both files are present and have the right extention
    print("Performed checks for aliquot: ", aliquot)
    return(pd.DataFrame(lane_checks, columns = {"Lane", "Req", "Opt"}))


def check_tech_assoc_files(manifest, file_list, techniques):
    technique = pd.read_csv(techniques, sep=",")
    total_file_count = len(manifest.filename)
    #lanes_substring = ["L001","L002","L003","L004","L005","L006","L007","L008"]
    #print("total files---")
    #print(total_file_count)
    #print(file_list)
    data_type = file_list['data_type'].unique()

    #All techniques that have R1, R2, I1, and I2 are in the list below. Add to list if new technique fits.
    raw_4_file_format_techniques = [ "10X Genomics Multiome;RNAseq", "10X Genomics Immune profiling;VDJ",
     "10X Genomics Immune profilling;GEX", "10xv2", "10xv3", "10xmultiome_cell_hash;RNA"]
    
    #All techniques that have R1, R2, R3, I1, and I2 are in the list below. Add to list if new technique fits.
    raw_5_file_format_techniques =[ "10X Genomics Multiome;ATAC-seq", "10xmultiome_cell_hash;ATAC"]
    
    for index, row in technique.iterrows():
        tname = row['name']
        aliquot = row['aliquot']
        logger.info("Checking Files for {tname} and Aliquot {aliquot}")
        
        #Get all manifest info for technique aliquot
        man_files = manifest[manifest['filename'].str.contains(aliquot)]

        #Currently only supporting raw file types. 
        #Checking to see which case the technique belongs to and preoceeding accordingly.
        if data_type == 'raw' and tname in raw_4_file_format_techniques:
            check_raw_files = check_raw_4_file_format_techniques(file_list, man_files, aliquot)
            if check_raw_files['Req'].all():
                logger.info(f"All Required Files for {tname} and Aliquot {aliquot} are present")
            else:
                logger.error(f"All Required Files for {tname} and Aliquot {aliquot} are NOT present!")
            if check_raw_files['Opt'].all():
                logger.info(f"All Optional Files for {tname} and Aliquot {aliquot} are present")
            else:
                logger.warning(f"All Optional Files for {tname} and Aliquot {aliquot} are NOT present!")
        elif data_type == 'raw' and tname in raw_5_file_format_techniques:
            check_raw_files = check_raw_5_file_format_techniques(file_list, man_files, aliquot)
        elif data_type == 'raw' and tname == "10xmultiome_cell_hash;hashing":
            check_raw_files = check_raw_3_hash_file_format_techniques(file_list, man_files, aliquot)
        else:
            print("files are not raw or in 4 format raw")
            #check_raw_files = raw_file_techniques(man_files.filename, aliquot)
        #print(man_files.filename)
        #temporary prints for new users. Will be replaced with logging.
        print(" Step 4: Completed checks for ", tname, " and aliquot ", aliquot)
    return total_file_count

def get_technique_file_list(techniques, master):
    technique = pd.read_csv(techniques, sep=",")
    #print(technique)
    file_list = master[master.technique.isin(technique.name)]
    #temporary prints for new users. Will be replaced with logging.
    #print(" Step 3 Complete: Getting technique details")
    logger.info("Getting technique details from master file based on user input.")
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
    #contains_all = manifest['filename'].isin(all_files).all()
    #if contains_all == False:
    #Get all files that are present in directory and in manifest
    contains_all = [x for x in all_files if x not in manifest.filename]
    #Get all files that are present in directory and missing in manifest. NBD. Add to log!
    missing_files_from_manifest = list(set(all_files) - set(manifest.filename))
    if len(missing_files_from_manifest)>0:
        logger.warning("These files are not in manifest but present in the directory: %s ",missing_files_from_manifest)
    #Get all files that are present in manifest and missing in directory. Bad.
    missing_files = list(set(manifest.filename) - set(all_files))
    if len(missing_files)>0:
        logger.error("These files are in manifest but missing from the directory: %s ",missing_files)
    #temporary message for debugging
    #print("Step 1 Complete: Checked Names")
    return(contains_all, missing_files)

def match_md5sums_to_manifest(md5sums_df):
    """ 
    Generate independent md5sums and check them against those in manifest.
    """
    #calc md5sum for each file and match to corresponding column in manifest.
    logger.debug("In match_md5sums_to_manifest().")
    print( "in match_md5sums_to_manifest()")
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
        logger.error("Found mismatches match_md5sums_to_manifest().")

        #send_file_validation_email(errors, submission_id, submitter)
    else:
        checksums_ok = True
        print("Step 2 Complete: Checking md5sums")
    #temporary prints for new users. Will be replaced with logging.
    #print("Step 2 Complete: Checking md5sums")
    #print("checksums match!")
    return checksums_ok

def compute_md5(filepath):
    """
    Compute md5 checksum for file. Borrowed from AUX.
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
# def confirm_checksums_match(worksheet_name, checksum_column_name, file_dataframe):
#     """
#     Confirms that MD5 checksums of the submitted files match the checksums
#     listed in the manifest. Mismatching checksums are reported to the
#     submitter.

#     Returns True if all checksums match; otherwise returns False.
#     """
#     logger.debug("In confirm_checksums_match().")

#     checksums_ok = False
#     error_message = "does not match value provided in the manifest"

#     # Compute checksum on each submitted file.
#     file_dataframe['observed_checksums'] = file_dataframe['submitted_filepath'].apply(compute_md5)
    
#     # Create mask to find mismatching observed and expected checksums.
#     df_mask = (file_dataframe[checksum_column_name] != file_dataframe['observed_checksums'])
    
#     # Get a list of row indices where observed checksums do not match the 
#     # checksum listed in the manifest.
#     rows_mismatched = file_dataframe[df_mask].index.tolist()

#     if len(rows_mismatched) > 0:
#         # One or more submitted file's checksum does not match checksum listed
#         # in manifest.

#         # Create human readable error messages.
#         errors = format_errors_for_excel_manifest(
#             rows_mismatched,
#             worksheet_name,
#             checksum_column_name,
#             error_message
#         )

#         update_submission_db(
#             "error",
#             ", ".join(errors),
#             submission_id
#         )

#         #send_file_validation_email(errors, submission_id, submitter)
#     else:
#         checksums_ok = True

#     return checksums_ok


if __name__ == '__main__':
    main()
