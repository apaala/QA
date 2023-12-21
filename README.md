# QA
QA script for checking submissions

Outputs:
-stdout with user logs
-detailed log with more information useful for debugging.
-updated manifest
-renamed files.

Inputs:
Path to directory
Path to manifest
Path to techniques and aliquot
Optional: -s to skip checking md5sums.
Optional: -l to pass full path to file you want logs written to
Optional: -r to rename files
Optional(recommended): -u path to updated manifest output file

#NOTE: to pipe output to a file add -u after python like so:
python -u QA.py *rest of the inputs*

Following techniques are ready to be tested:
-10X Genomics Multiome;RNAseq
-10X Genomics Immune profiling;VDJ
-10X Genomics Immune profilling;GEX
-10xv2
-10xv3
-10xmultiome_cell_hash;RNA

Implemented and not tested:
-10X Genomics Multiome;ATAC-seq
-10xmultiome_cell_hash;ATAC

Error logs should report pass/fail as table below
******************
FINAL QA RESULTS
******************
                      Technique       Aliquot Optional Required MissingFiles CheckSumQA
0  10X Genomics Multiome;RNAseq  NY-MX12001-1   FAILED   PASSED       FAILED    SKIPPED
1  10X Genomics Multiome;RNAseq  NY-MX12001-2   PASSED   PASSED       PASSED    SKIPPED

Optional -> Optional files. False if Failed QA. None if not present.
Required -> Required files.
MissingFiles -> Were there files missing from directory that were present in the manifest
CheckSumQA -> SKIPPED if user adds -s

--------------------------------------------------------------------------------------------------
Usage:

Step 1
-------
Clone repo:
	Go to folder you want to put script in
command:
	git clone https://github.com/apaala/QA.git
	svn co http://subversion.igs.umaryland.edu/svn/ENGR/nemo-aux/bican-qa/

Step 2
-------
Load python 
command:
	module load python/3.8


Step 3
------
Test repo

Print usage:
	python path/2/script/QA.py -h

Step 4
--------
Launch without grid:
Ex.
	qsub -cwd -b y -l mem_free=4G -P owhite-irc-ffs -q all.q -e testx.err -o testx.out -V python -u QA.py -d /path/to/dir//ypw4skw/ -m /manifest/ypw4skw_manifest.tsv -t tech.csv -s -l /path/to/logout.txt

Launch on grid:
Recommend using on grid, as md5sum checking can be time consuming.
Ex. 
	qsub -cwd -b y -l mem_free=4G -P owhite-irc-ffs -q all.q -e testx.err -o testx.out -V python -u QA.py -d /path/to/dir//ypw4skw/ -m /manifest/ypw4skw_manifest.tsv -t tech.csv

*****Rename testx.err and testx.out to track your logs correctly. Recommend using something unique for every submission
*****Pipe the stdout to a file to save user log.
*****QSUB will append to out and err files, not overwrite.
*****Use full paths.
*****Not ready for production!


Note: If you run into issues cloning, I have a version in my scratch under backup
